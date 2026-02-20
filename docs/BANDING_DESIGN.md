# Banding Node — Design Doc

## Problem

Actuarial pricing pipelines need to discretise continuous variables (e.g. driver age → age band) and group categorical variables (e.g. property type → category) before applying rating factors. This is called **banding**. A single pipeline step often bands multiple columns at once.

## Approach

A new `banding` node type that supports **multiple factors per node**, each independently configured as either continuous (range-based) or categorical (value grouping).

### Config schema

```json
{
  "factors": [
    {
      "banding": "continuous",
      "column": "driver_age",
      "outputColumn": "age_band",
      "rules": [
        { "op1": ">", "val1": 0, "op2": "<=", "val2": 25, "assignment": "0-25" }
      ],
      "default": null
    },
    {
      "banding": "categorical",
      "column": "property_type",
      "outputColumn": "prop_band",
      "rules": [
        { "value": "Semi-detached House", "assignment": "House" }
      ],
      "default": null
    }
  ]
}
```

### Backend

- **Executor** (`executor.py`): `_normalise_banding_factors` reads the `factors` array. The handler loops over factors, calling `_apply_banding` for each. Continuous rules build a Polars `when/then/otherwise` chain. Categorical rules use `replace_strict`.
- **Parser** (`_parser_helpers.py`): Detects `banding=` or `factors=` in the decorator → infers `"banding"` node type. Always normalises to `factors: [...]` in config (permissive parsing).
- **Codegen** (`codegen.py`): Single factor → clean decorator `@pipeline.node(banding=..., column=..., ...)`. Multiple factors → `@pipeline.node(factors=[...])`.

### Frontend

- **NodePanel.tsx**: `BandingConfig` component with tabbed UI — one tab per factor. Each tab has type toggle (continuous/categorical), input column (dropdown when upstream schema available, with auto-detection of type from dtype), output column, rules grid, and default value.
- **NodePalette.tsx**: Default config uses `factors: [...]`.
- **Auto-detection**: When upstream columns are cached from preview, input column becomes a dropdown. Selecting a numeric column auto-sets continuous; string column auto-sets categorical.

### Decorator syntax (public API)

Single factor (clean):
```python
@pipeline.node(banding="continuous", column="age",
               output_column="age_band", rules=[...])
```

Multiple factors:
```python
@pipeline.node(factors=[{"banding": "continuous", "column": "age", ...}, ...])
```

The parser accepts both; the codegen emits whichever is appropriate.

## Alternatives considered

1. **One node per factor** — simpler per-node config but clutters the graph when banding 5+ columns. Rejected.
2. **CSV-based rules** — read banding tables from files. Rejected: inline editing in the GUI is faster and keeps rules version-controlled in the `.py` file.
3. **Separate continuous/categorical node types** — rejected for same reason as (1).

## Files touched

- `src/haute/executor.py` — `_apply_banding`, `_banding_condition`, `_normalise_banding_factors`, banding handler
- `src/haute/_parser_helpers.py` — type inference, config building
- `src/haute/codegen.py` — templates + banding code generation
- `frontend/src/panels/NodePanel.tsx` — `BandingConfig`, `BandingRulesGrid`
- `frontend/src/panels/NodePalette.tsx` — palette entry
- `frontend/src/App.tsx` — node type registration
- `frontend/src/utils/nodeTypes.ts` — icon, colour, label
- `tests/test_banding.py` — 21 tests
