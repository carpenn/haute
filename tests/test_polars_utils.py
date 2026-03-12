"""Tests for haute._polars_utils (safe_sink & _malloc_trim)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from haute._polars_utils import _malloc_trim, safe_sink

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
# Newer Polars (>= 1.x) routes DataFrame.write_parquet through
# LazyFrame.sink_parquet internally.  That means a class-level mock on
# sink_parquet also breaks the eager fallback path.  We work around this by
# patching write_parquet / write_csv so the fallback writes the file
# directly via PyArrow, completely bypassing the mocked sink methods.


def _pyarrow_write_parquet(self: pl.DataFrame, path, **_kw) -> None:
    """Write a DataFrame to Parquet via PyArrow, bypassing Polars sinks."""
    import pyarrow.parquet as pq

    pq.write_table(self.to_arrow(), str(path))


def _manual_write_csv(self: pl.DataFrame, path, **_kw) -> None:
    """Write a DataFrame to CSV via stdlib, bypassing Polars sinks."""
    import csv

    cols = self.columns
    with open(str(path), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        for row in self.iter_rows():
            writer.writerow(row)


# ---------------------------------------------------------------------------
# Happy-path tests
# ---------------------------------------------------------------------------


def test_safe_sink_writes_parquet(tmp_path: Path):
    """Happy path: LazyFrame -> sink_parquet -> read back matches."""
    lf = pl.LazyFrame({"x": [10, 20, 30], "y": ["a", "b", "c"]})
    out = tmp_path / "out.parquet"

    safe_sink(lf, out)

    result = pl.read_parquet(out)
    assert result.shape == (3, 2)
    assert result["x"].to_list() == [10, 20, 30]
    assert result["y"].to_list() == ["a", "b", "c"]


def test_safe_sink_writes_csv(tmp_path: Path):
    """Happy path with fmt='csv'."""
    lf = pl.LazyFrame({"a": [1, 2], "b": [3.5, 4.5]})
    out = tmp_path / "out.csv"

    safe_sink(lf, out, fmt="csv")

    result = pl.read_csv(out)
    assert result.shape == (2, 2)
    assert result["a"].to_list() == [1, 2]
    assert result["b"].to_list() == [3.5, 4.5]


# ---------------------------------------------------------------------------
# Fallback tests (parquet)
# ---------------------------------------------------------------------------


def _run_parquet_fallback(tmp_path: Path, error: Exception) -> None:
    """Shared helper: verify parquet fallback for a given Polars error type."""
    lf = pl.LazyFrame({"a": [1, 2, 3]})
    out = tmp_path / "test.parquet"

    with (
        patch.object(pl.LazyFrame, "sink_parquet", side_effect=error),
        patch.object(
            pl.DataFrame,
            "write_parquet",
            autospec=True,
            side_effect=_pyarrow_write_parquet,
        ),
    ):
        safe_sink(lf, out)

    result = pl.read_parquet(out)
    assert result["a"].to_list() == [1, 2, 3]


def test_safe_sink_fallback_on_compute_error(tmp_path: Path):
    """ComputeError in sink_parquet triggers collect+write_parquet fallback."""
    _run_parquet_fallback(
        tmp_path, pl.exceptions.ComputeError("streaming not supported")
    )


def test_safe_sink_fallback_on_invalid_operation_error(tmp_path: Path):
    """InvalidOperationError in sink_parquet triggers fallback."""
    _run_parquet_fallback(
        tmp_path, pl.exceptions.InvalidOperationError("bad op")
    )


def test_safe_sink_fallback_on_schema_error(tmp_path: Path):
    """SchemaError in sink_parquet triggers fallback."""
    _run_parquet_fallback(
        tmp_path, pl.exceptions.SchemaError("schema mismatch")
    )


# ---------------------------------------------------------------------------
# Fallback tests (csv)
# ---------------------------------------------------------------------------


def test_safe_sink_csv_fallback(tmp_path: Path):
    """ComputeError in sink_csv triggers collect+write_csv fallback."""
    lf = pl.LazyFrame({"name": ["alice", "bob"], "score": [90, 85]})
    out = tmp_path / "test.csv"

    with (
        patch.object(
            pl.LazyFrame,
            "sink_csv",
            side_effect=pl.exceptions.ComputeError("csv streaming failed"),
        ),
        patch.object(
            pl.DataFrame,
            "write_csv",
            autospec=True,
            side_effect=_manual_write_csv,
        ),
    ):
        safe_sink(lf, out, fmt="csv")

    result = pl.read_csv(out)
    assert result["name"].to_list() == ["alice", "bob"]
    assert result["score"].to_list() == [90, 85]


# ---------------------------------------------------------------------------
# Non-Polars errors must propagate
# ---------------------------------------------------------------------------


def test_safe_sink_real_error_propagates(tmp_path: Path):
    """PermissionError (non-Polars) must NOT be caught by the fallback."""
    lf = pl.LazyFrame({"a": [1]})
    out = tmp_path / "test.parquet"

    with patch.object(
        pl.LazyFrame,
        "sink_parquet",
        side_effect=PermissionError("permission denied"),
    ):
        with pytest.raises(PermissionError, match="permission denied"):
            safe_sink(lf, out)


# ---------------------------------------------------------------------------
# _malloc_trim
# ---------------------------------------------------------------------------


def test_malloc_trim_does_not_raise():
    """_malloc_trim must never raise regardless of platform."""
    _malloc_trim()  # should not raise


class TestMallocTrimDispatch:
    """Verify _malloc_trim calls the correct platform API."""

    def test_linux_calls_glibc_malloc_trim(self, monkeypatch):
        from unittest.mock import MagicMock

        mock_cdll = MagicMock()
        monkeypatch.setattr("sys.platform", "linux")
        monkeypatch.setattr("ctypes.CDLL", mock_cdll)
        _malloc_trim()
        mock_cdll.assert_called_once_with("libc.so.6")
        mock_cdll.return_value.malloc_trim.assert_called_once_with(0)

    def test_windows_calls_heapmin(self, monkeypatch):
        from unittest.mock import MagicMock

        mock_msvcrt = MagicMock()
        monkeypatch.setattr("sys.platform", "win32")
        monkeypatch.setattr("ctypes.cdll", MagicMock(msvcrt=mock_msvcrt))
        _malloc_trim()
        mock_msvcrt._heapmin.assert_called_once()

    def test_macos_is_noop(self, monkeypatch):
        """macOS has no native heap compaction — verify no ctypes calls."""
        from unittest.mock import MagicMock

        mock_cdll_cls = MagicMock()
        mock_cdll_inst = MagicMock()
        monkeypatch.setattr("sys.platform", "darwin")
        monkeypatch.setattr("ctypes.CDLL", mock_cdll_cls)
        monkeypatch.setattr("ctypes.cdll", mock_cdll_inst)
        _malloc_trim()
        mock_cdll_cls.assert_not_called()
        mock_cdll_inst.assert_not_called()

    def test_linux_graceful_on_oserror(self, monkeypatch):
        """If libc.so.6 can't be loaded, _malloc_trim must not raise."""
        from unittest.mock import MagicMock

        monkeypatch.setattr("sys.platform", "linux")
        monkeypatch.setattr("ctypes.CDLL", MagicMock(side_effect=OSError))
        _malloc_trim()  # should not raise

    def test_windows_graceful_on_attribute_error(self, monkeypatch):
        """If msvcrt._heapmin is missing, _malloc_trim must not raise."""
        from unittest.mock import MagicMock, PropertyMock

        mock_cdll = MagicMock()
        type(mock_cdll).msvcrt = PropertyMock(side_effect=AttributeError)
        monkeypatch.setattr("sys.platform", "win32")
        monkeypatch.setattr("ctypes.cdll", mock_cdll)
        _malloc_trim()  # should not raise
