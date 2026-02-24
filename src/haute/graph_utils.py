"""Backward-compatible re-export facade.

All symbols that were previously defined in ``graph_utils.py`` are now
split across focused modules but re-exported here so existing imports
like ``from haute.graph_utils import GraphNode`` continue to work.

Modules:
    _types.py        — TypedDicts, _Frame, _sanitize_func_name, instance mapping
    _topo.py         — topo_sort_ids, ancestors
    _cache.py        — graph_fingerprint
    _io.py           — read_source, load_external_object
    _flatten.py      — flatten_graph
    _execute_lazy.py — _prepare_graph, _execute_lazy
"""

from haute._cache import graph_fingerprint as graph_fingerprint
from haute._execute_lazy import EagerResult as EagerResult
from haute._execute_lazy import _execute_eager_core as _execute_eager_core
from haute._execute_lazy import _execute_lazy as _execute_lazy
from haute._execute_lazy import _prepare_graph as _prepare_graph
from haute._flatten import flatten_graph as flatten_graph
from haute._io import _object_cache as _object_cache
from haute._io import load_external_object as load_external_object
from haute._io import read_source as read_source
from haute._mlflow_io import load_mlflow_model as load_mlflow_model
from haute._topo import ancestors as ancestors
from haute._topo import topo_sort_ids as topo_sort_ids
from haute._types import OPTIMISER_CONFIG_KEYS as OPTIMISER_CONFIG_KEYS
from haute._types import SCENARIO_EXPANDER_CONFIG_KEYS as SCENARIO_EXPANDER_CONFIG_KEYS
from haute._types import GraphEdge as GraphEdge
from haute._types import GraphNode as GraphNode
from haute._types import HauteError as HauteError
from haute._types import NodeData as NodeData
from haute._types import NodeType as NodeType
from haute._types import PipelineGraph as PipelineGraph
from haute._types import _Frame as _Frame
from haute._types import _sanitize_func_name as _sanitize_func_name
from haute._types import build_instance_mapping as build_instance_mapping
from haute._types import resolve_orig_source_names as resolve_orig_source_names
