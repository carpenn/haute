"""Tests for cross-platform memory helpers in _algorithms and _ram_estimate."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, mock_open, patch

import pytest

from haute.modelling._algorithms import _get_available_mb, _get_rss_mb, _mem_checkpoint

# ---------------------------------------------------------------------------
# _get_rss_mb
# ---------------------------------------------------------------------------


class TestGetRssMb:
    """Verify _get_rss_mb dispatches correctly per platform."""

    def test_returns_float(self):
        result = _get_rss_mb()
        assert isinstance(result, float)
        assert result >= 0.0, "RSS should never be negative"
        # A running Python process uses at least a few MB of RSS
        # (on platforms where /proc or equivalent is available).
        # On unsupported platforms the function returns 0.0, which is still >= 0.
        if result > 0.0:
            assert result < 100_000, "RSS above 100 GB is implausible for a test process"

    @pytest.mark.skipif(sys.platform != "linux", reason="Linux only")
    def test_positive_on_linux(self):
        """On Linux, /proc/self/status should give a positive RSS."""
        assert _get_rss_mb() > 0.0

    def test_linux_reads_proc_status(self, monkeypatch):
        monkeypatch.setattr("sys.platform", "linux")
        fake_status = "Name:\tpython\nVmRSS:\t204800 kB\nVmSize:\t500000 kB\n"
        with patch("builtins.open", mock_open(read_data=fake_status)):
            result = _get_rss_mb()
        assert result == pytest.approx(200.0)  # 204800 kB / 1024

    def test_linux_fallback_on_oserror(self, monkeypatch):
        monkeypatch.setattr("sys.platform", "linux")
        with patch("builtins.open", side_effect=OSError):
            result = _get_rss_mb()
        assert result == 0.0

    def test_darwin_calls_getrusage(self, monkeypatch):
        monkeypatch.setattr("sys.platform", "darwin")
        mock_rusage = MagicMock()
        mock_rusage.ru_maxrss = 500 * 1024 * 1024  # 500 MB in bytes
        mock_resource = MagicMock()
        mock_resource.getrusage.return_value = mock_rusage
        mock_resource.RUSAGE_SELF = 0

        with patch.dict("sys.modules", {"resource": mock_resource}):
            with patch("builtins.open", side_effect=OSError):
                result = _get_rss_mb()

        assert result == pytest.approx(500.0)
        mock_resource.getrusage.assert_called_once_with(0)

    def test_win32_calls_get_process_memory_info(self, monkeypatch):
        monkeypatch.setattr("sys.platform", "win32")

        mock_windll = MagicMock()
        mock_windll.kernel32.GetCurrentProcess.return_value = 42
        mock_windll.psapi.GetProcessMemoryInfo.return_value = True

        mock_ctypes = MagicMock()
        mock_ctypes.windll = mock_windll
        mock_ctypes.c_size_t = int
        mock_ctypes.wintypes.DWORD = int

        # The function defines ProcessMemoryCounters inline, so we can't
        # easily intercept the struct. Instead, verify the API was called.
        modules = {"ctypes": mock_ctypes, "ctypes.wintypes": mock_ctypes.wintypes}
        with patch("builtins.open", side_effect=OSError):
            with patch.dict("sys.modules", modules):
                # This won't return a real value due to mocking, but it should not raise
                _get_rss_mb()

        mock_windll.kernel32.GetCurrentProcess.assert_called_once()

    def test_returns_zero_on_unknown_platform(self, monkeypatch):
        monkeypatch.setattr("sys.platform", "freebsd13")
        with patch("builtins.open", side_effect=OSError):
            assert _get_rss_mb() == 0.0


# ---------------------------------------------------------------------------
# _get_available_mb
# ---------------------------------------------------------------------------


class TestGetAvailableMb:
    """Verify _get_available_mb delegates to available_ram_bytes."""

    def test_returns_positive_float(self):
        result = _get_available_mb()
        assert isinstance(result, float)
        assert result > 0.0

    def test_delegates_to_available_ram_bytes(self):
        """Must return available_ram_bytes() / (1024 * 1024)."""
        fake_bytes = 8 * 1024 * 1024 * 1024  # 8 GiB
        with patch("haute.modelling._algorithms.available_ram_bytes", return_value=fake_bytes):
            result = _get_available_mb()
        assert result == pytest.approx(8192.0)


# ---------------------------------------------------------------------------
# _mem_checkpoint
# ---------------------------------------------------------------------------


class TestMemCheckpoint:
    def test_writes_to_log_file(self, tmp_path, monkeypatch):
        log_path = tmp_path / "mem.log"
        monkeypatch.setattr("haute.modelling._algorithms._MEM_LOG", log_path)

        _mem_checkpoint("test_label")

        content = log_path.read_text()
        assert "test_label" in content
        assert "RSS=" in content
        assert "Avail=" in content

    def test_appends_on_multiple_calls(self, tmp_path, monkeypatch):
        log_path = tmp_path / "mem.log"
        monkeypatch.setattr("haute.modelling._algorithms._MEM_LOG", log_path)

        _mem_checkpoint("first")
        _mem_checkpoint("second")

        lines = log_path.read_text().strip().split("\n")
        assert len(lines) == 2
        assert "first" in lines[0]
        assert "second" in lines[1]

    def test_fsync_failure_does_not_raise(self, tmp_path, monkeypatch):
        """Defensive fsync: OSError from fsync must not crash the process."""
        log_path = tmp_path / "mem.log"
        monkeypatch.setattr("haute.modelling._algorithms._MEM_LOG", log_path)

        with patch("os.fsync", side_effect=OSError("not supported")):
            _mem_checkpoint("test")  # should not raise

        assert "test" in log_path.read_text()
