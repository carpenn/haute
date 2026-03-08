# Quote Feature Extractor

You are an AI agent working inside a haute insurance pricing pipeline. Your job
is to create a `feature_processing` node and its supporting `helpers/features.py`
module that transforms the raw flattened quote JSON into a clean, model-ready
dataset.

## Project structure

Haute projects keep helper functions and constants in a `helpers/` module at the
project root, separate from `main.py`. This keeps the pipeline file clean and
readable. After `haute init`, the project has:

```
helpers/
  __init__.py
  features.py          ← starter utilities (to_date, years_between, cols_matching)
main.py                ← pipeline definition (nodes + edges only)
config/quote_input/*.json ← flattenSchema describing the quote JSON
```

Your job is to:

1. **Read the schema** and understand every field
2. **Write `helpers/features.py`** with all helper functions, constants, and
   rename maps needed for feature engineering
3. **Update `main.py`** with the import line and a clean `feature_processing`
   node body

## Agent workflow — follow these steps exactly

### 1. Read the schema

Read these files in order:

1. **The api_input config** — `config/quote_input/*.json`. Look at the
   `flattenSchema` object. This tells you every field in the quote, its type,
   and how arrays are structured (`$max` = max items, `$items` = fields per item).
2. **The pipeline file** — `main.py`. Find the `api_input` node and the
   `feature_processing` node (which you will write/rewrite).
3. **The existing helpers** — `helpers/features.py`. Check what starter
   utilities already exist (typically `to_date`, `years_between`, `cols_matching`
   from `haute init`).
4. **Sample data** (if available) — `data/*.json` or `data/*.parquet`. Scan a
   few rows to see real values, spot nulls, and confirm date formats.

### 2. Categorise every column

Go through every field in the flattenSchema and assign it to exactly one bucket:

| Bucket              | What it means                                         | What you do with it                  |
| ------------------- | ----------------------------------------------------- | ------------------------------------ |
| **Straight rename** | Useful as-is, just needs a clean name                 | Add to `RENAME_MAP`                  |
| **Derived feature** | Needs a calculation (age from DOB, sum of two fields) | Add to `with_columns` + `DERIVED_COLS` |
| **Array field**     | Lives under a numbered array (e.g. `additional_drivers.1.gender`) | Handle via a helper function (e.g. `driver_features()`) |
| **Boolean flag**    | An add-on or option selected/not-selected             | Handle via a helper function (e.g. `addon_features()`) |
| **Drop**            | PII (names, full DOB), raw dates consumed by a derived feature, or internal IDs | Don't include in final `select` |

**Every column must go in one bucket. Don't leave any behind without a reason.**

### 3. Build the rename map

Create `RENAME_MAP` — a Python dict mapping `"dot.notation.name"` to `"clean_snake_case"`.

Follow these rules strictly (they remove ambiguity):

| Rule | When to apply | Example |
|------|---------------|---------|
| **Use the leaf name** | When only one section has that field name | `vehicle.fuel_type` → `fuel_type` |
| **Prefix with section** | When multiple sections share a field name (e.g. proposer and additional drivers both have `gender`) | `proposer.gender` → `proposer_gender` |
| **Shorten deeply nested paths** | When the middle levels add no meaning | `policy_details.usage.commuting` → `usage_commuting` |
| **Keep the schema's own words** | Always — never invent synonyms | `estimated_value` stays `estimated_value`, don't rename to `price` |

### 4. Write `helpers/features.py`

`helpers/features.py` already has three starter utilities from `haute init`:

- `to_date(col_name)` — parse a string column to a date
- `years_between(earlier, later)` — whole years between two date expressions
- `cols_matching(all_cols, pattern_fn)` — filter a list of column names

**Do not remove or recreate these.** Add your new code below them.

**Add the following to `helpers/features.py`:**

1. **One helper function per array section** in the schema (drivers, claims,
   convictions — whatever exists). Each helper takes the list of column names +
   any shared expressions, and returns:
   - A list of Polars expressions to add via `with_columns`
   - A rename map for the array fields
   - A list of output column names to keep

   Follow this pattern for the function signature and return type:

   ```python
   def driver_features(
       cols: list[str],
       cover_start: pl.Expr,
       max_additional: int = 4,
   ) -> tuple[list[pl.Expr], list[str], dict[str, str], list[str]]:
       """Build everything needed for additional driver columns.

       Returns (derived_exprs, age_col_names, rename_map, keep_cols).
       """
   ```

2. **Constants** (module-level, after the functions):
   - `RENAME_MAP` — the full rename dict from step 3
   - `ADDON_NAMES` — list of add-on names (if the schema has add-ons)
   - `DERIVED_COLS` — list of every column name created by `with_columns` steps

### 5. Update the import in `main.py`

Add (or update) the import block between the standard imports and the
`pipeline = ...` line:

```python
from helpers.features import (
    addon_features,
    driver_features,
    to_date,
    years_between,
    ADDON_NAMES,
    DERIVED_COLS,
    RENAME_MAP,
)
```

Import only the names actually used by the node body. Do not import everything.

### 6. Write the node body — exactly 4 steps

The node body should be short and read like a recipe:

```
Step 1 — Add calculated columns     (one .with_columns call)
Step 2 — Cross-column aggregates     (e.g. youngest_driver_age)
Step 3 — Rename                      (one .rename call)
Step 4 — Select                      (one .select call — only keep what's needed)
```

**No `return` statement.** Assign the final result to `df` and haute handles it.

### 7. Verify

After writing, check:
- [ ] Every column from the schema is accounted for (renamed, derived, or explicitly dropped)
- [ ] No column appears in both `RENAME_MAP` values and `DERIVED_COLS` (would duplicate)
- [ ] `DERIVED_COLS` lists every column name created in `with_columns` steps
- [ ] The `select` at the end covers: `RENAME_MAP.values()` + `DERIVED_COLS` + helper keep lists
- [ ] No `return` statement in the node body
- [ ] No list comprehensions in the node body that use local variables (use `for` loops instead)
- [ ] The import in `main.py` matches exactly what's used in the node body
- [ ] `helpers/features.py` has `import polars as pl` at the top

## Exec-sandbox rules (critical — read before writing any node code)

Haute runs node bodies via `exec()`. This means:

1. **No `return` statements** inside the node body. The last value assigned to
   `df` is returned automatically.
2. **No list comprehensions that reference variables defined inside the node body.**
   Python's scoping rules mean `[x for x in local_var]` fails inside `exec()`.
   Use explicit `for` loops instead. This does NOT apply to helper functions in
   `helpers/features.py` — they run in normal Python scope and can use
   comprehensions freely.
3. **Helper functions are available by name** — just call them directly
   (e.g. `to_date(...)`, `driver_features(...)`) as long as they are imported
   in `main.py`.

## Common derived features — use what the schema supports

| Feature | What it needs | How to calculate |
|---------|---------------|------------------|
| Proposer age | `proposer.date_of_birth` + a cover start date | `years_between(to_date("proposer.date_of_birth"), cover_start)` |
| Proposer licence length | `proposer.licence.licence_date` + cover start | `years_between(to_date("proposer.licence.licence_date"), cover_start)` |
| Vehicle age | `vehicle.year_of_manufacture` + cover start | `cover_start.dt.year() - pl.col("vehicle.year_of_manufacture")` |
| Total excess | `voluntary_excess` + `compulsory_excess` | `pl.col("...voluntary_excess") + pl.col("...compulsory_excess")` |
| Postcode area (outcode) | `address.postcode` | `pl.col("address.postcode").str.split(" ").list.first()` |
| Address tenure in months | `years_at_address` + `months_at_address` | `years * 12 + months` |
| Youngest driver age | proposer age + all additional driver ages | `pl.min_horizontal("proposer_age", *additional_driver_age_cols)` |
| Number of drivers | count of non-null additional driver DOBs + 1 | sum of `is_not_null()` casts + `pl.lit(1)` |
| Number of add-ons | count of selected add-on flags | sum of `.fill_null(False).cast(Int64)` |

Only create features where the source columns exist in the schema. Don't guess
at column names that aren't there.

---

## Complete worked example

This is a real example from a UK motor insurance quote schema. Use it as your
template — adapt the column names and features to match the actual schema you're
working with.

### `helpers/features.py`

```python
"""Feature engineering helpers for insurance quote processing.

Provides utilities for:
- Date parsing and age calculation from nested JSON fields
- Additional driver feature extraction (ages, licence years, categorical fields)
- Add-on feature extraction (selected flags, counts)
- Column rename mappings (dot-notation → clean snake_case)
"""

import polars as pl


# ── Generic Polars helpers (from haute init) ──────────────────────────


def to_date(col_name: str) -> pl.Expr:
    """Parse a string column to a date."""
    return pl.col(col_name).str.to_date("%Y-%m-%d")


def years_between(earlier: pl.Expr, later: pl.Expr) -> pl.Expr:
    """Whole years between two date expressions (floor)."""
    return ((later - earlier).dt.total_days() / 365.25).floor().cast(pl.Int64)


def cols_matching(all_cols: list[str], pattern_fn) -> list[str]:
    """Return columns from *all_cols* where pattern_fn(col) is True."""
    return [c for c in all_cols if pattern_fn(c)]


# ── Additional driver features ────────────────────────────────────────

# Fields to extract from each additional_drivers.N sub-object.
# Maps dot-notation suffix → clean column suffix.
AD_FIELDS = {
    "gender": "gender",
    "marital_status": "marital_status",
    "relationship_to_proposer": "relationship",
    "employment_status": "employment_status",
    "licence.licence_type": "licence_type",
    "has_criminal_convictions": "has_criminal_convictions",
    "uk_resident_since_birth": "uk_resident_since_birth",
}


def driver_features(
    cols: list[str],
    cover_start: pl.Expr,
    max_additional: int = 4,
) -> tuple[list[pl.Expr], list[str], dict[str, str], list[str]]:
    """Build everything needed for additional driver columns.

    Returns (derived_exprs, age_col_names, rename_map, keep_cols).
    """
    derived: list[pl.Expr] = []
    age_col_names: list[str] = []
    rename_map: dict[str, str] = {}
    keep_cols: list[str] = []

    for i in range(1, max_additional + 1):
        pfx = f"additional_drivers.{i}"
        dob_col = f"{pfx}.date_of_birth"
        lic_col = f"{pfx}.licence.licence_date"

        if dob_col in cols:
            age_name = f"additional_driver_{i}_age"
            derived.append(years_between(to_date(dob_col), cover_start).alias(age_name))
            age_col_names.append(age_name)
            keep_cols.append(age_name)

        if lic_col in cols:
            lic_name = f"additional_driver_{i}_licence_years"
            derived.append(years_between(to_date(lic_col), cover_start).alias(lic_name))
            keep_cols.append(lic_name)

        for field, leaf in AD_FIELDS.items():
            src = f"{pfx}.{field}"
            if src in cols:
                rename_map[src] = f"additional_driver_{i}_{leaf}"

    # number_of_drivers and has_additional_driver
    dob_cols = cols_matching(
        cols,
        lambda c: c.endswith(".date_of_birth") and c.startswith("additional_drivers."),
    )
    if dob_cols:
        num_ad = pl.lit(0)
        for c in dob_cols:
            num_ad = num_ad + pl.col(c).is_not_null().cast(pl.Int64)
        derived.append((pl.lit(1) + num_ad).alias("number_of_drivers"))
        derived.append((num_ad > 0).alias("has_additional_driver"))
    else:
        derived.append(pl.lit(1).alias("number_of_drivers"))
        derived.append(pl.lit(False).alias("has_additional_driver"))

    keep_cols += list(rename_map.values()) + ["number_of_drivers", "has_additional_driver"]
    return derived, age_col_names, rename_map, keep_cols


# ── Add-on features ──────────────────────────────────────────────────


def addon_features(
    cols: list[str],
    addon_names: list[str],
) -> tuple[list[pl.Expr], dict[str, str], list[str]]:
    """Build everything needed for add-on columns.

    Returns (derived_exprs, rename_map, keep_cols).
    """
    derived: list[pl.Expr] = []
    rename_map: dict[str, str] = {}
    keep_cols: list[str] = []

    selected_cols: list[str] = []
    for name in addon_names:
        src = f"add_ons.{name}.selected"
        if src in cols:
            selected_cols.append(src)
            rename_map[src] = f"addon_{name}"

    if selected_cols:
        count_expr = pl.lit(0)
        for c in selected_cols:
            count_expr = count_expr + pl.col(c).fill_null(False).cast(pl.Int64)
        derived.append(count_expr.alias("number_of_add_ons"))
        keep_cols.append("number_of_add_ons")

    keep_cols += list(rename_map.values())
    return derived, rename_map, keep_cols


# ── Column mappings ──────────────────────────────────────────────────

# Flattened dot-notation → clean snake_case
RENAME_MAP = {
    # Quote
    "quote_metadata.quote_id": "quote_id",
    "quote_metadata.channel": "channel",
    # Policy
    "policy_details.cover_type": "cover_type",
    "policy_details.payment_frequency": "payment_frequency",
    "policy_details.voluntary_excess": "voluntary_excess",
    "policy_details.compulsory_excess": "compulsory_excess",
    "policy_details.ncd_years": "ncd_years",
    "policy_details.ncd_protected": "ncd_protected",
    "policy_details.annual_mileage": "annual_mileage",
    "policy_details.is_renewal": "is_renewal",
    "policy_details.usage.social_domestic_pleasure": "usage_sdp",
    "policy_details.usage.commuting": "usage_commuting",
    # Proposer
    "proposer.gender": "proposer_gender",
    "proposer.marital_status": "proposer_marital_status",
    "proposer.employment_status": "proposer_employment_status",
    "proposer.licence.licence_type": "proposer_licence_type",
    "proposer.is_homeowner": "proposer_is_homeowner",
    "proposer.is_main_driver": "proposer_is_main_driver",
    "proposer.has_criminal_convictions": "proposer_has_criminal_convictions",
    "proposer.uk_resident_since_birth": "proposer_uk_resident_since_birth",
    "proposer.access_to_other_vehicles": "proposer_access_to_other_vehicles",
    # Vehicle
    "vehicle.make": "vehicle_make",
    "vehicle.model": "vehicle_model",
    "vehicle.body_type": "body_type",
    "vehicle.fuel_type": "fuel_type",
    "vehicle.transmission": "transmission",
    "vehicle.engine_size_cc": "engine_size_cc",
    "vehicle.engine_power_bhp": "engine_power_bhp",
    "vehicle.number_of_seats": "number_of_seats",
    "vehicle.insurance_group": "insurance_group",
    "vehicle.estimated_value": "vehicle_value",
    "vehicle.is_imported": "vehicle_is_imported",
    "vehicle.has_been_modified": "vehicle_has_been_modified",
    "vehicle.overnight_location": "overnight_location",
    "vehicle.daytime_location": "daytime_location",
    "vehicle.owner": "vehicle_owner",
    "vehicle.current_mileage": "current_mileage",
    "vehicle.security.alarm": "security_alarm",
    "vehicle.security.immobiliser": "security_immobiliser",
    "vehicle.security.tracker": "security_tracker",
    # Address
    "address.postcode": "postcode",
    "address.city": "city",
    "address.years_at_address": "years_at_address",
}

ADDON_NAMES = [
    "breakdown_cover",
    "legal_expenses",
    "motor_legal_protection",
    "key_cover",
    "courtesy_car",
    "windscreen_cover",
    "excess_protection",
    "no_claims_step_back",
    "personal_accident",
    "personal_belongings",
    "tools_in_transit",
]

# Columns created by with_columns (not renamed from source)
DERIVED_COLS = [
    "proposer_age",
    "proposer_licence_length_years",
    "vehicle_age",
    "total_excess",
    "address_total_months",
    "postcode_area",
    "youngest_driver_age",
]
```

### `main.py` (relevant sections only)

```python
"""Pipeline: my_pipeline"""

import polars as pl
import haute

from helpers.features import (
    addon_features,
    driver_features,
    to_date,
    years_between,
    ADDON_NAMES,
    DERIVED_COLS,
    RENAME_MAP,
)

pipeline = haute.Pipeline("my_pipeline", description="")


@pipeline.node(config="config/quote_input/quotes.json")
def quotes() -> pl.LazyFrame:
    """api_input node"""
    from haute._json_flatten import read_json_flat
    return read_json_flat("data/quotes_10m.jsonl", config_path="config/quote_input/quotes.json")


@pipeline.node
def feature_processing(quotes: pl.LazyFrame) -> pl.LazyFrame:
    """Feature engineering for insurance pricing"""

    cols = quotes.collect_schema().names()
    cover_start = to_date("policy_details.cover_start_date")

    # Helpers build the dynamic driver + addon expressions
    ad_derived, ad_age_cols, ad_renames, ad_keep = driver_features(cols, cover_start)
    addon_derived, addon_renames, addon_keep = addon_features(cols, ADDON_NAMES)

    # Step 1 — Add calculated columns
    df = quotes.with_columns(
        # Proposer
        years_between(to_date("proposer.date_of_birth"), cover_start).alias("proposer_age"),
        years_between(to_date("proposer.licence.licence_date"), cover_start).alias("proposer_licence_length_years"),
        # Vehicle
        (cover_start.dt.year() - pl.col("vehicle.year_of_manufacture")).alias("vehicle_age"),
        # Policy
        (pl.col("policy_details.voluntary_excess") + pl.col("policy_details.compulsory_excess")).alias("total_excess"),
        # Address
        (pl.col("address.years_at_address") * 12 + pl.col("address.months_at_address")).alias("address_total_months"),
        pl.col("address.postcode").str.split(" ").list.first().alias("postcode_area"),
        # Additional drivers + counts + addons
        *ad_derived,
        *addon_derived,
    )

    # Step 2 — Youngest driver across proposer + any additional drivers
    df = df.with_columns(
        pl.min_horizontal("proposer_age", *ad_age_cols).alias("youngest_driver_age"),
    )

    # Step 3 — Rename dot-notation columns to clean names
    df = df.rename({**RENAME_MAP, **ad_renames, **addon_renames})

    # Step 4 — Keep only the columns we need
    df = df.select(list(RENAME_MAP.values()) + DERIVED_COLS + ad_keep + addon_keep)
```

---

## Additional features requested by the user

<!-- INSTRUCTIONS FOR THE USER:
     Describe any extra features you want in plain English below.
     You don't need to write any code — just say what you want and the
     AI will figure out the Polars logic.

     Format:  feature_name: plain English description

     Examples:
       policy_duration_days: number of days between cover start and cover end
       is_young_driver: true if the proposer is under 25
       high_value_vehicle: true if estimated value is over 30000
       total_claims: count of how many claims the proposer has
       years_since_last_claim: years between the most recent claim date and cover start
       risk_postcode: first two letters of the postcode only
-->

(Add your features here, one per line)
