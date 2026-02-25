# Config Sidecar Files

## Problem

Pipeline decorators accumulate large config payloads (banding rules, rating
tables, optimiser settings) that make `main.py` unreadable. A single banding
node can produce 50+ lines of inline kwargs. This also makes diffs noisy and
manual config editing painful.

## Approach

Node config is externalised into JSON sidecar files under a `config/`
directory, organised by node type:

```
config/
  factors/          # banding nodes
  tables/           # rating step nodes
  datasource/       # data source nodes
  model_score/      # model score nodes
  optimiser/        # optimiser nodes
  optimiser_apply/  # optimiser apply nodes
  scenario_expander/
  output/
  sink/             # data sink nodes
  external_model/   # external file nodes
  modelling/
  constant/
  api_input/
  live_switch/
```

Each node gets one file: `config/<type_folder>/<node_name>.json`.

The decorator references the file instead of carrying inline config:

```python
@pipeline.node(config="config/factors/optimiser_banding.json")
def optimiser_banding(data_source):
    ...
```

### What stays in Python

- **User code** (`code` key) remains in the `.py` function body. The JSON
  file never contains executable code.
- **Transform nodes** have no config file — they are code-only.
- **Submodel and submodel port** nodes have no config file.

### Folder-as-type convention

The subfolder name determines the node type at parse time. This is a
deliberate design choice: node types are immutable once created (changing
type means deleting and recreating), so the folder→type mapping is stable.

The canonical mapping lives in `_config_io.NODE_TYPE_TO_FOLDER`.

### Backward compatibility

The parser still accepts inline decorator kwargs (the pre-sidecar format).
If no `config=` kwarg is present, it falls back to `_build_node_config()`
from `_parser_helpers.py`. This means hand-edited pipelines without JSON
files continue to work.

## Key modules

| Module | Role |
|---|---|
| `_config_io.py` | Path conventions, read/write, `collect_node_configs()` |
| `_parser_helpers._resolve_node_config()` | Shared config resolution for parser + submodel parser |
| `codegen._node_to_code()` | Post-processes decorator to `config=` reference |
| `routes/pipeline.py` | Writes config JSON files on save |
| `server.py` | Watches `config/` directory for live sync |

## Alternatives considered

1. **Flat config folder** (all JSON files in one `config/` directory) —
   rejected because it doesn't scale and provides no type information from
   the path alone.
2. **Extend `.haute.json`** — rejected because `.haute.json` is layout
   metadata, not business logic. Mixing concerns would complicate both
   parsing and the GUI.
3. **Python config files** — rejected because JSON is more accessible to
   non-engineers and is the format the GUI already uses internally.
