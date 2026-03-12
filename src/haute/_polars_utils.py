"""Polars streaming helpers shared across execution paths."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from haute._logging import get_logger

logger = get_logger(component="polars_utils")


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
