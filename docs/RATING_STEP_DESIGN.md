# Rating Step — Multivariate Table Design

## Problem

Actuarial pricing models apply multiplicative factors (relativities) based on risk characteristics. After banding creates discrete factor levels (e.g. `age_band: "25-35"`, `prop_band: "House"`), the rating step looks up the corresponding relativity from a rating table.

Tables can be:
- **1-way**: single factor → value (e.g. age_band → 1.15)
- **2-way**: two factors → value (grid: age_band × prop_band)
- **3-way**: three factors → value (dropdown selects factor3 level, shows 2-way grid of factor1 × factor2)

A single rating step node supports **multiple tables**, each producing an output column.

## Config Schema

```json
{
  "tables": [
    {
      "name": "Age Factor",
      "factors": ["age_band"],
      "outputColumn": "age_factor",
      "defaultValue": "1.0",
      "entries": [
        { "age_band": "young", "value": "1.3" },
        { "age_band": "older", "value": "0.9" }
      ]
    },
    {
      "name": "Age × Property",
      "factors": ["age_band", "prop_band"],
      "outputColumn": "age_prop_factor",
      "defaultValue": "1.0",
      "entries": [
        { "age_band": "young", "prop_band": "House", "value": "1.2" },
        { "age_band": "young", "prop_band": "Flat",  "value": "1.5" },
        { "age_band": "older", "prop_band": "House", "value": "0.9" },
        { "age_band": "older", "prop_band": "Flat",  "value": "1.1" }
      ]
    }
  ]
}
```

Key points:
- `factors` is an array of 1–3 column names (typically banding output columns).
- `entries` is a flat list of records. Each record has one key per factor + a `value` key.
- `defaultValue` is used when a lookup misses (no matching entry).
- This flat format works uniformly for 1/2/3-way tables — the dimensionality is implicit from the `factors` length.

## Auto-populating Factor Levels

When the user selects a factor column, the UI looks for upstream **banding nodes** and extracts the unique `assignment` values from their rules. This populates the table skeleton instantly without running the pipeline.

Algorithm:
1. Walk upstream edges from the rating step node.
2. For each upstream banding node, inspect `config.factors[].outputColumn` and `config.factors[].rules[].assignment`.
3. Build a map: `column_name → Set<assignment_values>`.
4. When the user picks a factor, populate the table rows/columns with those values.

If no upstream banding node provides the column, fall back to manually typed levels (or cached `_columns` preview data if available).

## Backend

### Executor (`executor.py`)

For each table in `config.tables`:

1. Build a Polars `DataFrame` from `entries`:
   ```python
   lookup_df = pl.DataFrame(table["entries"])
   # Cast value column to Float64 for numeric relativities
   lookup_df = lookup_df.with_columns(pl.col("value").cast(pl.Float64))
   ```
2. Left join the main `LazyFrame` on the factor columns:
   ```python
   lf = lf.join(
       lookup_df.lazy(),
       on=table["factors"],
       how="left",
   )
   ```
3. Rename the `value` column to `outputColumn`, fill nulls with `defaultValue`:
   ```python
   lf = lf.with_columns(
       pl.col("value").fill_null(float(default)).alias(output_col)
   ).drop("value")
   ```

This is simple, efficient, and works for any number of factors.

### Parser (`_parser_helpers.py`)

Detect `tables=` in decorator kwargs → infer `"ratingStep"` node type.
Normalise each table entry's `output_column` → `outputColumn` (Python snake → JS camel).

### Codegen (`codegen.py`)

Emit `@pipeline.node(tables=[...])` with the entries as Python literals.
For readability, single-table nodes could use a flatter decorator, but `tables=[...]` is always valid.

## Frontend

### RatingStepConfig component

Structure mirrors `BandingConfig`:
- **Tab bar** for multiple tables (add/remove).
- Each tab shows:
  - **Name** input
  - **Factor dropdowns** (up to 3, populated from upstream columns/banding)
  - **Output column** input
  - **Default value** input
  - **Table editor** (varies by factor count)

### Table Editor Variants

**1 factor** — simple two-column table:
```
| age_band | value |
|----------|-------|
| young    | 1.3   |
| older    | 0.9   |
```

**2 factors** — grid (factor1 = rows, factor2 = columns):
```
|           | House | Flat | Bungalow |
|-----------|-------|------|----------|
| young     | 1.2   | 1.5  | 1.0      |
| older     | 0.9   | 1.1  | 0.8      |
```

**3 factors** — dropdown for factor3, then 2-way grid of factor1 × factor2:
```
[Factor3: region_band ▼]  [North ▼]

|           | House | Flat | Bungalow |
|-----------|-------|------|----------|
| young     | 1.2   | 1.5  | 1.0      |
| older     | 0.9   | 1.1  | 0.8      |
```

Switching the dropdown value shows the corresponding 2-way slice.

### entries ↔ grid conversion

The config stores a flat `entries` array. The UI converts to/from grid format:

- **To grid**: group entries by factor values, build row/col headers from unique values.
- **From grid**: flatten back to entries array on every cell edit.

This keeps the config format uniform while the UI adapts to dimensionality.

## Files to touch

- `src/haute/executor.py` — `_apply_rating_table`, rating step handler
- `src/haute/_parser_helpers.py` — detect `tables=`, build config
- `src/haute/codegen.py` — rating step template + code generation
- `frontend/src/panels/NodePanel.tsx` — `RatingStepConfig`, table editors
- `frontend/src/panels/NodePalette.tsx` — update default config
- `tests/test_rating_step.py` — new test file

## Table Combination

When a rating step has 2+ tables, their outputs can be combined into a single column using a configurable operation:

- **multiply** (default) — standard multiplicative relativities: `combined = age_factor × region_factor`
- **add** — additive loadings: `combined = loading_a + loading_b`
- **min** / **max** — floor/cap across factors: `combined = min(factor_a, factor_b)`

### Config fields

```json
{
  "tables": [...],
  "operation": "multiply",
  "combinedColumn": "combined_factor"
}
```

- `operation` defaults to `"multiply"` if omitted. Codegen only emits it when non-default.
- `combinedColumn` is the output column name. If empty, no combination is performed.
- Combination is skipped when fewer than 2 tables have an `outputColumn`.

### Codegen

```python
@pipeline.node(tables=[...], operation='add', combined_column='total_loading')
def Rating_Step_1(df: pl.LazyFrame) -> pl.LazyFrame:
    ...
```

`operation='multiply'` is omitted from the decorator since it is the default.

## UI Design

The table editor UI draws from actuarial pricing tool conventions (WTW Radar/Emblem, Earnix, Akur8):

- **Heatmap cell coloring** — cells are tinted by relativity magnitude. Surcharges (>1.0) show warm red/orange, discounts (<1.0) show cool blue. Intensity scales with distance from 1.0, saturating at ±0.5. This gives instant visual feedback on factor impact without needing to read every number.
- **Relativity bar chart** (1-way tables) — a horizontal bar per row shows the magnitude relative to the table maximum, reinforcing the heatmap with a second visual channel.
- **Zebra striping** — alternating row backgrounds for dense table readability.
- **Sticky row headers** (2-way grids) — row labels stay visible when scrolling wide tables horizontally.
- **Row↓ / Col→ indicators** — the corner header cell labels which factor maps to rows vs columns.
- **Stats footer** — every table shows `n=`, `min`, `avg`, `max` in color-coded text. This mirrors the summary statistics panels in Emblem/Radar.
- **Tab badges** — each table tab shows its entry count.
- **Formula summary** — when 2+ tables exist, a live formula shows the combination expression (e.g. `combined = age × region`). Uses function notation for min/max operations.

## Design decisions

- **Values are always Float64** — rating table cell values are numeric relativities, not strings. The executor casts all `value` fields to `pl.Float64`.
- **Flat entries array** — uniform across 1/2/3-way tables. The UI converts to/from grid on the fly.
- **Factor levels from banding assignments** — `extractBandingLevels` scans all banding nodes in the graph (not just direct upstream) and builds a `column → levels[]` map from rule assignments.
- **Heatmap scale** — ±0.5 deviation from 1.0 reaches full intensity. This covers the typical actuarial relativity range (0.5–1.5) while keeping extreme values visually distinct.

## Alternatives considered

1. **External CSV/Excel rating tables** — rejected: inline editing in the GUI is faster, keeps tables version-controlled in `.py`, no file management overhead.
2. **Separate node per table** — rejected: clutters the graph when a single pricing step applies 5+ factors.
3. **Store 2D/3D arrays instead of flat entries** — rejected: flat entries are simpler to parse/generate, work uniformly across dimensions, and are easier to diff in git.
4. **Separate heatmap overlay vs inline coloring** — rejected: inline tinting on the input cell itself is simpler and doesn't require a separate read-only view. The user sees the color while editing.
