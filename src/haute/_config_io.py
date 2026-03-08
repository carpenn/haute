"""Config file I/O: read/write node config JSON sidecar files.

Each pipeline node's declarative config (everything except user code in
the function body) is stored in a JSON file under
``config/<type_folder>/<node_name>.json``.  The decorator references it::

    @pipeline.node(config="config/banding/optimiser_banding.json")

This module provides:

- Path conventions (folder ↔ NodeType mappings)
- Read / write helpers
- ``collect_node_configs`` for generating all config files from a graph
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from haute._logging import get_logger
from haute._types import NodeType, PipelineGraph, _sanitize_func_name

logger = get_logger(component="config_io")

# ---------------------------------------------------------------------------
# Folder ↔ NodeType mappings
# ---------------------------------------------------------------------------

NODE_TYPE_TO_FOLDER: dict[NodeType, str] = {
    NodeType.API_INPUT: "quote_input",
    NodeType.DATA_SOURCE: "data_source",
    NodeType.LIVE_SWITCH: "source_switch",
    NodeType.MODEL_SCORE: "model_scoring",
    NodeType.BANDING: "banding",
    NodeType.RATING_STEP: "rating_step",
    NodeType.OUTPUT: "quote_response",
    NodeType.DATA_SINK: "data_sink",
    NodeType.EXTERNAL_FILE: "load_file",
    NodeType.MODELLING: "model_training",
    NodeType.OPTIMISER: "optimisation",
    NodeType.OPTIMISER_APPLY: "apply_optimisation",
    NodeType.SCENARIO_EXPANDER: "expander",
    NodeType.CONSTANT: "constant",
}

FOLDER_TO_NODE_TYPE: dict[str, NodeType] = {v: k for k, v in NODE_TYPE_TO_FOLDER.items()}

# Keys that live in the .py function body, NOT in the JSON config file.
_CODE_KEYS: frozenset[str] = frozenset({"code"})


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def has_config_folder(node_type: NodeType) -> bool:
    """Whether this node type stores config in an external JSON file."""
    return node_type in NODE_TYPE_TO_FOLDER


def config_path_for_node(
    node_type: NodeType,
    node_name: str,
    base_dir: Path | None = None,
) -> Path:
    """Build the config file path for a node.

    Returns a relative path like ``config/banding/optimiser_banding.json``.
    If *base_dir* is provided, returns an absolute path.
    """
    folder = NODE_TYPE_TO_FOLDER.get(node_type)
    if folder is None:
        raise ValueError(f"No config folder for node type {node_type!r}")
    rel = Path("config") / folder / f"{node_name}.json"
    if base_dir:
        return base_dir / rel
    return rel


def infer_node_type_from_config_path(config_path: str | Path) -> NodeType | None:
    """Extract the node type from the config path's folder name.

    E.g. ``"config/banding/optimiser_banding.json"`` → ``NodeType.BANDING``.
    """
    parts = Path(config_path).parts
    # Expected: config/<type_folder>/<name>.json  (or just <type_folder>/<name>.json)
    for part in reversed(parts[:-1]):
        if part in FOLDER_TO_NODE_TYPE:
            return FOLDER_TO_NODE_TYPE[part]
    return None


# ---------------------------------------------------------------------------
# Read / Write
# ---------------------------------------------------------------------------


def load_node_config(
    config_path: str | Path,
    base_dir: Path | None = None,
) -> dict[str, Any]:
    """Load a node's config from its JSON file.

    *config_path* can be relative (resolved against *base_dir*) or absolute.
    """
    p = Path(config_path)
    if not p.is_absolute() and base_dir:
        p = base_dir / p
    return json.loads(p.read_text())


def save_node_config(
    node_type: NodeType,
    node_name: str,
    config: dict[str, Any],
    base_dir: Path,
) -> Path:
    """Write a node's config to its JSON file.

    Returns the **relative** path (for use in the decorator).
    Code keys are excluded — they stay in the ``.py`` function body.
    """
    rel_path = config_path_for_node(node_type, node_name)
    abs_path = base_dir / rel_path
    abs_path.parent.mkdir(parents=True, exist_ok=True)
    filtered = {k: v for k, v in config.items() if k not in _CODE_KEYS}
    abs_path.write_text(json.dumps(filtered, indent=2, ensure_ascii=False) + "\n")
    logger.info("config_saved", path=str(rel_path), node_type=node_type.value)
    return rel_path


def remove_config_file(
    node_type: NodeType,
    node_name: str,
    base_dir: Path,
) -> bool:
    """Remove a node's config file.  Returns *True* if removed."""
    try:
        rel_path = config_path_for_node(node_type, node_name)
    except ValueError:
        return False
    abs_path = base_dir / rel_path
    if abs_path.is_file():
        abs_path.unlink()
        logger.info("config_removed", path=str(rel_path))
        return True
    return False


# ---------------------------------------------------------------------------
# Graph-level helpers
# ---------------------------------------------------------------------------


def collect_node_configs(graph: PipelineGraph) -> dict[str, str]:
    """Extract config files for all nodes in a graph.

    Returns a dict mapping relative path (e.g.
    ``"config/banding/optimiser_banding.json"``) to JSON content string.

    Nodes without a config folder (transforms, submodels) are skipped.
    Instance nodes are skipped (they reference an original).
    """
    configs: dict[str, str] = {}
    for node in graph.nodes:
        nt = node.data.nodeType
        if not has_config_folder(nt):
            continue
        if node.data.config.get("instanceOf"):
            continue
        func_name = _sanitize_func_name(node.data.label)
        rel_path = str(config_path_for_node(nt, func_name))
        filtered = {k: v for k, v in node.data.config.items() if k not in _CODE_KEYS}
        configs[rel_path] = json.dumps(filtered, indent=2, ensure_ascii=False) + "\n"
    return configs
