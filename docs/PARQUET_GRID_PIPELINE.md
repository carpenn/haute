# Parquet Grid Pipeline

## Problem

The optimiser route executes the user's pipeline graph to produce a scored DataFrame, then builds a `QuoteGrid` for the Rust solver. For large portfolios (tens of millions of rows), materialising the full DataFrame in Python causes high peak memory usage. The Python process holds the scored DataFrame, the QuoteGrid (in Rust), and intermediate copies during type casting — all simultaneously.

## Approach

The pipeline stays lazy (Polars `LazyFrame`) until the optimiser route, which sinks the scored data directly to a temporary Parquet file via `sink_parquet()`. A Rust function (`build_grid_from_parquet`) reads the Parquet file and constructs the `QuoteGrid` without any Python DataFrame intermediate.

### Data flow

```
LazyFrame (scored pipeline output)
    |
    v  sink_parquet(tmp_path)  — streams to disk, Python memory stays flat
    |
    v  build_grid_from_parquet(tmp_path, ...)  — Rust reads Parquet, builds grid
    |
    v  QuoteGrid (compact Rust struct, Arc-shared)
    |
    v  Solver / Frontier / Apply
```

The temp file is cleaned up in a `finally` block regardless of success or failure.

## Alternatives considered

1. **Chunked builder via `QuoteGridBuilder.append()`** — More complex Python-side code; still requires Python to iterate and hold each chunk. Available as a fallback if needed.
2. **Arrow IPC instead of Parquet** — Lower serialisation overhead but less widely supported and no column-level compression. Parquet is the standard interchange format in the data ecosystem.
3. **Pass LazyFrame directly to Rust** — Not feasible with current `pyo3-polars` API; it only supports eager `DataFrame` conversion.

## Trade-offs

- **Disk I/O overhead**: The Parquet round-trip adds latency (~1-2s for large files). For small datasets this is noticeable; for large datasets the memory savings dominate.
- **Peak Python memory**: Near zero — only the `QuoteGrid` handle (a thin wrapper around an `Arc<QuoteGrid>`) lives in Python.
- **Rust memory**: Temporarily holds the full DataFrame during `ingest_dataframe`, then drops it after grid construction. Peak Rust memory is DataFrame + QuoteGrid, settling to QuoteGrid only.
- **Column projection**: The upstream lazy plan selects only solver-relevant columns (`select(solver_cols)`), so the Parquet file is narrow regardless of how many columns the pipeline produces.
