# Batch Scoring for Model Score Nodes

## Problem
`model_score_fn` in `executor.py` calls `lf.collect()` which materialises the entire upstream pipeline into RAM at once. For large datasets (10M+ rows), this OOMs before `model.predict()` even runs.

## Solution: Sink → Batch Score → Lazy Scan

Mirror the training pattern: sink upstream data to a temp parquet file via streaming, then read/score/write in batches, and return a lazy scan of the scored output.

### Flow
```
1. lf (LazyFrame)
   → sink_parquet(tmp_input.parquet)     # streaming, never fully in RAM

2. Read tmp_input.parquet in row-group batches
   → prepare features (null handling)
   → model.predict(batch)
   → accumulate prediction arrays

3. Read tmp_input.parquet lazily
   → .with_columns(prediction Series)     # BUT this won't work lazily...
```

Actually, the cleaner approach matching the training pattern:

```
1. Sink upstream LazyFrame → tmp_input.parquet (streaming)
2. Read parquet in batches via PyArrow RecordBatchReader
   → For each batch: prepare → predict → write to tmp_scored.parquet
3. Return pl.scan_parquet(tmp_scored.parquet)
```

This is better because:
- Peak RAM = 1 batch (~500K rows) not the full dataset
- Downstream nodes get a lazy scan (projection/predicate pushdown still works)
- Temp files are cleaned up on process exit via atexit

### Changes

**File: `src/haute/executor.py`** — `model_score_fn` only

Replace the current:
```python
df_eager = lf.collect()
x_data = _prepare_predict_frame(model, df_eager, features)
preds = model.predict(x_data).flatten()
df_eager = df_eager.with_columns(pl.Series(_output_col, preds))
```

With:
```python
# 1. Sink upstream to temp parquet (streaming — never fully in RAM)
tmp_input = _sink_to_temp(lf)

# 2. Score in batches → write scored parquet
tmp_scored = _batch_score(model, tmp_input, features, _output_col, _task)

# 3. Return lazy scan of scored output
result_lf = pl.scan_parquet(tmp_scored)
```

New helper `_score_in_batches()` (inside the MODEL_SCORE elif block or as a module-level function):
- Uses `pl.read_parquet_schema` to get schema
- Uses `pyarrow.parquet.ParquetFile` to iterate row groups
- For each row group: read as Polars DataFrame → `_prepare_predict_frame` → `predict` → append prediction column → write to ParquetWriter
- Batch size: one row group at a time (parquet default ~1M rows per row group, but `sink_parquet` uses smaller groups). We can also use `pl.read_parquet(source, row_count=BATCH_SIZE, row_offset=...)` approach.

Actually simplest: use PyArrow's `ParquetFile.iter_batches()` → convert each batch to Polars → predict → write via `pq.ParquetWriter.write_table()`. This way we never hold more than one batch in RAM.

### Cleanup
Register temp files with `atexit` for cleanup, same pattern as training job's temp files. Or use a context-manager approach if the executor tracks lifecycle.

### Codegen template
Update `_MODEL_SCORE` in `codegen.py` to match — the generated `main.py` code should use the same sink+batch pattern.

### Edge cases
- **sink_parquet fails** (Python UDFs upstream): fallback to `collect(engine="streaming").write_parquet()` — same as training
- **Classification with predict_proba**: score both `predict` and `predict_proba` in the same batch loop
- **User code block**: runs AFTER scoring, on the lazy scan result — no change needed
- **Empty upstream**: skip batching, return empty LazyFrame
- **Downstream expects all original columns + prediction**: each batch writes ALL columns plus the prediction column(s)
