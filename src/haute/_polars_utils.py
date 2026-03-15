"""Polars streaming helpers shared across execution paths."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import polars as pl

from haute._logging import get_logger

logger = get_logger(component="polars_utils")


# ---------------------------------------------------------------------------
# Atomic write helper
# ---------------------------------------------------------------------------


@contextmanager
def atomic_write(dest: Path) -> Generator[Path, None, None]:
    """Context manager for atomic file writes via temp-then-rename.

    Yields a temporary path (``dest`` with ``.parquet.tmp`` suffix).
    On successful exit, atomically renames the temp file to *dest*.
    On exception, cleans up the temp file and re-raises.

    Usage::

        with atomic_write(cache_path) as tmp:
            df.write_parquet(tmp, compression="zstd")
        # cache_path now exists
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".parquet.tmp")
    try:
        yield tmp
        tmp.rename(dest)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


# ---------------------------------------------------------------------------
# Parquet metadata reader
# ---------------------------------------------------------------------------


def read_parquet_metadata(path: Path) -> dict[str, Any]:
    """Read lightweight schema info from a parquet file.

    Returns a dict with keys: ``row_count``, ``column_count``, ``columns``
    (name → Arrow type string), ``size_bytes``, and ``mtime``.
    """
    import pyarrow.parquet as pq

    stat = path.stat()
    meta = pq.read_metadata(str(path))
    arrow_schema = pq.read_schema(str(path))
    columns = {
        name: str(arrow_schema.field(name).type)
        for name in arrow_schema.names
    }
    return {
        "row_count": meta.num_rows,
        "column_count": meta.num_columns,
        "columns": columns,
        "size_bytes": stat.st_size,
        "mtime": stat.st_mtime,
    }


def _malloc_trim() -> None:
    """Ask the OS to return freed heap pages.

    After ``del df; gc.collect()``, Python's allocator keeps the pages
    mapped — RSS stays high even though the memory is logically free.

    Platform strategies:

    - **Linux**: ``malloc_trim(0)`` via glibc forces arena release.
    - **Windows**: ``_heapmin()`` via msvcrt compacts the CRT heap.
    - **macOS**: no direct API — callers should ``gc.collect()`` beforehand.
    """
    import sys

    platform = sys.platform
    if platform == "linux":
        try:
            import ctypes

            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except (OSError, AttributeError):
            logger.debug("malloc_trim_unavailable", platform=platform)
    elif platform == "win32":
        try:
            import ctypes

            ctypes.cdll.msvcrt._heapmin()
        except (OSError, AttributeError):
            logger.debug("heapmin_unavailable", platform=platform)
    # macOS / other: no native heap compaction API available


def safe_sink(
    lf: pl.LazyFrame,
    path: str | Path,
    *,
    fmt: str = "parquet",
) -> None:
    """Sink a LazyFrame to file via streaming, with fallback.

    Tries ``sink_parquet`` / ``sink_csv`` first (streaming, low memory).
    If Polars raises a streaming-incompatible error, falls back to
    ``collect(engine="streaming")`` + eager write.

    Only retries on Polars-specific errors (``ComputeError``,
    ``InvalidOperationError``, ``SchemaError``).  Real I/O errors
    (permissions, disk full) propagate immediately.
    """
    path = Path(path)
    try:
        if fmt == "csv":
            lf.sink_csv(path)
        else:
            lf.sink_parquet(path)
    except (
        pl.exceptions.ComputeError,
        pl.exceptions.InvalidOperationError,
        pl.exceptions.SchemaError,
    ):
        logger.info("sink_streaming_fallback", path=str(path), fmt=fmt)
        df = lf.collect(engine="streaming")
        if fmt == "csv":
            df.write_csv(path)
        else:
            df.write_parquet(path)
        del df
