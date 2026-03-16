"""RAM estimation for training — sample-probe approach.

Before materialising a full pipeline for model training, run a small
probe (default 1 000 rows) through the pipeline, measure the per-row
memory footprint, and extrapolate to the full dataset.  If the
estimated total (with a CatBoost overhead multiplier) would exceed
available system RAM, calculate a safe ``row_limit`` and return a
human-readable warning.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import NamedTuple

from haute._logging import get_logger
from haute.graph_utils import GraphNode, NodeType, PipelineGraph

logger = get_logger(component="ram_estimate")

__all__ = [
    "available_ram_bytes",
    "available_vram_bytes",
    "estimate_gpu_vram_bytes",
    "estimate_source_rows",
    "estimate_safe_training_rows",
    "RamEstimate",
]


# ---------------------------------------------------------------------------
# System RAM
# ---------------------------------------------------------------------------


def available_ram_bytes() -> int:
    """Return available system RAM in bytes.

    - **Linux**: reads ``/proc/meminfo`` (most accurate).
    - **macOS / POSIX**: ``os.sysconf`` page-based query.
    - **Windows**: ``GlobalMemoryStatusEx`` via ctypes.
    - **Fallback**: conservative 4 GiB default.
    """
    # Linux: /proc/meminfo gives the most accurate available figure
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    # Format: "MemAvailable:   29146944 kB"
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass

    # macOS / POSIX fallback
    try:
        import os

        pages = os.sysconf("SC_AVPHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        if pages > 0 and page_size > 0:
            return pages * page_size
    except (AttributeError, ValueError):
        pass

    # Windows: GlobalMemoryStatusEx
    import sys

    if sys.platform == "win32":
        try:
            import ctypes

            class MemoryStatusEx(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong),
                    ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong),
                    ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong),
                    ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong),
                    ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
                ]

            mem = MemoryStatusEx()
            mem.dwLength = ctypes.sizeof(MemoryStatusEx)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(  # type: ignore[attr-defined]
                ctypes.byref(mem)
            ):
                return int(mem.ullAvailPhys)
        except (OSError, AttributeError, ImportError):
            pass

    # Last resort: assume 4 GiB
    return 4 * 1024**3


# ---------------------------------------------------------------------------
# GPU VRAM
# ---------------------------------------------------------------------------


def available_vram_bytes() -> int | None:
    """Return total GPU VRAM in bytes, or ``None`` if no GPU is detected.

    Queries ``nvidia-smi`` (works without ``pynvml``).  Returns the
    **total** VRAM of the first GPU — free VRAM fluctuates constantly,
    so the total is the meaningful upper bound for planning.
    """
    try:
        import subprocess

        result = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=memory.total",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            # May return multiple lines for multi-GPU — take the first
            line = result.stdout.strip().split("\n")[0].strip()
            return int(line) * 1024 * 1024  # MiB → bytes
    except (FileNotFoundError, subprocess.TimeoutExpired, ValueError, OSError):
        pass
    return None


# CatBoost GPU stores per row:
#   float32 feature data  (4 bytes/feature)
#   + binarised features  (1 byte/feature)
#   + label/gradient/hessian (12 bytes)
# Plus histogram buffers that depend on border_count and tree depth.
# The 2× safety multiplier accounts for CUDA runtime overhead (~500 MB),
# CatBoost-internal temporary buffers (split evaluation, CTR statistics,
# leaf value computation), and memory that grows during tree construction.
# Empirically validated: 10M rows × 100 features OOM'd an 8 GB GPU at
# iteration 231/1000, confirming that the raw data footprint (~5 GB)
# roughly doubles during training.
_VRAM_SAFETY_MULTIPLIER = 2.0


def estimate_gpu_vram_bytes(
    n_rows: int,
    n_features: int,
    *,
    border_count: int = 128,
    depth: int = 6,
) -> int:
    """Estimate CatBoost GPU VRAM needed for *n_rows* × *n_features*.

    The estimate covers both train and eval pools (pass total rows
    including the eval partition).  It accounts for:

    - Float32 feature data and binarised representation.
    - Per-row gradient, hessian, and label arrays.
    - Level-wise histogram buffers (``border_count × 2^depth``).
    - A 1.3× safety multiplier for CUDA fragmentation.
    """
    feature_bytes = n_rows * n_features * 5  # float32 + binarised
    per_row_bytes = n_rows * 12              # label + gradient + hessian
    n_leaves = 2 ** min(depth, 10)           # cap at 1024 leaves
    histogram_bytes = n_features * border_count * n_leaves * 8

    raw = feature_bytes + per_row_bytes + histogram_bytes
    return int(raw * _VRAM_SAFETY_MULTIPLIER)


# ---------------------------------------------------------------------------
# Source row count
# ---------------------------------------------------------------------------


def _count_source_rows_for_node(node: GraphNode) -> int | None:
    """Estimate the row count for a single source node without reading all data.

    - **Parquet**: reads row-group metadata (instant, zero I/O beyond footer).
    - **JSON/JSONL with cache**: reads cached parquet metadata.
    - **Databricks**: returns ``None`` (unknown).
    """
    config = node.data.config
    node_type = node.data.nodeType

    if node_type == NodeType.API_INPUT:
        path = config.get("path", "")
        # JSON/JSONL sources have a parquet cache — check it
        if path.endswith((".json", ".jsonl")):
            from haute._json_flatten import json_cache_info
            info = json_cache_info(path)
            if info is not None:
                return info["row_count"]
            return None
        # Flat parquet api_input
        if path and Path(path).exists():
            return _parquet_row_count(path)
        return None

    if node_type == NodeType.DATA_SOURCE:
        source_type = config.get("sourceType", "flat_file")
        if source_type == "databricks":
            return None
        path = config.get("path", "")
        if path and Path(path).exists():
            if path.endswith(".parquet"):
                return _parquet_row_count(path)
            if path.endswith(".csv"):
                # CSV: count lines minus header — cheap streaming read
                return _csv_row_count(path)
        return None

    return None


def _parquet_row_count(path: str) -> int:
    """Row count from parquet metadata (reads only the footer, not data)."""
    import pyarrow.parquet as pq
    meta = pq.read_metadata(path)
    return meta.num_rows


def _csv_row_count(path: str) -> int:
    """Approximate row count for a CSV file (line count minus header)."""
    count = 0
    with open(path, "rb") as f:
        for _ in f:
            count += 1
    return max(count - 1, 0)  # subtract header


def estimate_source_rows(graph: PipelineGraph) -> int | None:
    """Estimate total rows entering the pipeline from source nodes.

    Returns the **maximum** row count across all source nodes (since joins
    and transforms may change cardinality, the largest source is the best
    proxy for peak memory).  Returns ``None`` if no source row count could
    be determined.
    """
    max_rows: int | None = None
    for node in graph.nodes:
        if node.data.nodeType in (NodeType.API_INPUT, NodeType.DATA_SOURCE):
            count = _count_source_rows_for_node(node)
            if count is not None:
                max_rows = max(max_rows or 0, count)
    return max_rows


# ---------------------------------------------------------------------------
# Sample probe + safe row limit
# ---------------------------------------------------------------------------

# CatBoost overhead multiplier.  Peak during training is:
#   train Pool (float64 internal) + eval Pool + CatBoost histograms/buffers.
# Pool float64 ≈ 2× the Polars float32 estimate.  Adding eval pool and
# CatBoost overhead brings the total to ~3×.  Strings are now converted
# to Polars Categorical before .to_pandas(), so the Pandas→Pool path no
# longer balloons memory for string-heavy datasets.
_CATBOOST_OVERHEAD_MULTIPLIER = 3.0

# Don't use more than 70% of available RAM for training data.
_RAM_SAFETY_FACTOR = 0.7

# Minimum probe size (rows) — smaller probes are less accurate.
_MIN_PROBE_ROWS = 500

# Default probe size.
_DEFAULT_PROBE_ROWS = 1_000


class RamEstimate(NamedTuple):
    """Result of the RAM estimation probe."""

    safe_row_limit: int | None
    """Row limit that fits in RAM, or ``None`` if no limit is needed."""
    total_rows: int | None
    """Estimated total source rows, or ``None`` if unknown."""
    estimated_bytes: int
    """Estimated bytes for the full dataset at the target node."""
    available_bytes: int
    """Available system RAM in bytes."""
    bytes_per_row: float
    """Measured bytes per row from the probe."""
    was_downsampled: bool
    """Whether a row limit was applied."""
    warning: str | None
    """Human-readable warning message if downsampled, else ``None``."""
    probe_columns: int = 0
    """Number of columns in the probe output (proxy for feature count)."""


def estimate_safe_training_rows(
    graph: PipelineGraph,
    target_node_id: str,
    build_node_fn: Callable,
    *,
    probe_rows: int = _DEFAULT_PROBE_ROWS,
    overhead_multiplier: float = _CATBOOST_OVERHEAD_MULTIPLIER,
    safety_factor: float = _RAM_SAFETY_FACTOR,
    preamble_ns: dict | None = None,
    scenario: str = "live",
) -> RamEstimate:
    """Estimate whether the full pipeline fits in RAM for training.

    1. Run the pipeline with ``row_limit=probe_rows`` (fast, <100ms).
    2. Measure ``df.estimated_size()`` at the target node.
    3. Extrapolate to the full source row count.
    4. Compare against available RAM × safety_factor.
    5. If it won't fit, calculate a safe ``row_limit``.

    Returns a ``RamEstimate`` with the decision and warning message.
    """
    from haute._execute_lazy import _execute_eager_core

    # 1. Probe: run the pipeline with a small row limit
    probe_result = _execute_eager_core(
        graph, build_node_fn,
        target_node_id=target_node_id,
        row_limit=probe_rows,
        swallow_errors=True,
        preamble_ns=preamble_ns,
        scenario=scenario,
    )

    probe_df = probe_result.outputs.get(target_node_id)
    if probe_df is None or len(probe_df) == 0:
        # Can't estimate — let the real run fail with a proper error
        logger.warning("probe_empty", target_node_id=target_node_id)
        return RamEstimate(
            safe_row_limit=None,
            total_rows=None,
            estimated_bytes=0,
            available_bytes=available_ram_bytes(),
            bytes_per_row=0,
            was_downsampled=False,
            warning=None,
            probe_columns=0,
        )

    # 2. Measure bytes per row from the probe
    probe_size = probe_df.estimated_size()
    actual_probe_rows = len(probe_df)
    bytes_per_row = probe_size / actual_probe_rows
    n_columns = probe_df.width

    # 3. Get total source rows
    total_rows = estimate_source_rows(graph)

    if total_rows is None:
        # Can't estimate — proceed without a limit
        logger.info(
            "source_rows_unknown",
            msg="Cannot estimate source row count, proceeding without limit",
        )
        return RamEstimate(
            safe_row_limit=None,
            total_rows=None,
            estimated_bytes=0,
            available_bytes=available_ram_bytes(),
            bytes_per_row=bytes_per_row,
            was_downsampled=False,
            warning=None,
            probe_columns=n_columns,
        )

    # 4. Extrapolate to full dataset with overhead
    estimated_bytes = int(bytes_per_row * total_rows)
    ram_needed = int(estimated_bytes * overhead_multiplier)
    available = available_ram_bytes()
    usable_ram = int(available * safety_factor)

    logger.info(
        "ram_estimate",
        total_rows=total_rows,
        bytes_per_row=round(bytes_per_row, 1),
        estimated_mb=round(estimated_bytes / 1024**2, 1),
        ram_needed_mb=round(ram_needed / 1024**2, 1),
        available_mb=round(available / 1024**2, 1),
        usable_mb=round(usable_ram / 1024**2, 1),
    )

    # 5. Decision
    if ram_needed <= usable_ram:
        return RamEstimate(
            safe_row_limit=None,
            total_rows=total_rows,
            estimated_bytes=estimated_bytes,
            available_bytes=available,
            bytes_per_row=bytes_per_row,
            was_downsampled=False,
            warning=None,
            probe_columns=n_columns,
        )

    # Calculate safe row count
    safe_rows = int(usable_ram / (bytes_per_row * overhead_multiplier))
    safe_rows = max(safe_rows, _MIN_PROBE_ROWS)  # never go below minimum

    warning = (
        f"Dataset downsampled to {safe_rows:,} of {total_rows:,} rows to fit in "
        f"available RAM ({available / 1024**3:.1f} GB). "
        f"Estimated full dataset would need {ram_needed / 1024**3:.1f} GB "
        f"(including CatBoost overhead)."
    )
    logger.warning("downsampling", safe_rows=safe_rows, total_rows=total_rows, warning=warning)

    return RamEstimate(
        safe_row_limit=safe_rows,
        total_rows=total_rows,
        estimated_bytes=estimated_bytes,
        available_bytes=available,
        bytes_per_row=bytes_per_row,
        was_downsampled=True,
        warning=warning,
        probe_columns=n_columns,
    )
