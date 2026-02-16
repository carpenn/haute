"""Tests for haute.cli — CLI command tests using click.testing.CliRunner."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from click.testing import CliRunner

from haute.cli import cli


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def project_dir(tmp_path: Path) -> Path:
    """Temp project with a root-level pipeline and data/."""
    data = tmp_path / "data"
    data.mkdir()
    pl.DataFrame({"x": [1, 2, 3]}).write_parquet(data / "input.parquet")

    code = f'''\
import polars as pl
import haute

pipeline = haute.Pipeline("test_cli", description="CLI test pipeline")


@pipeline.node(path="{data / 'input.parquet'}")
def source() -> pl.DataFrame:
    """Read data."""
    return pl.scan_parquet("{data / 'input.parquet'}")


@pipeline.node
def transform(source: pl.DataFrame) -> pl.DataFrame:
    """Add a column."""
    return source.with_columns(y=pl.col("x") * 2)


pipeline.connect("source", "transform")
'''
    (tmp_path / "main.py").write_text(code)
    return tmp_path


# ---------------------------------------------------------------------------
# haute --version
# ---------------------------------------------------------------------------

class TestVersion:
    def test_version_flag(self, runner: CliRunner):
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "version" in result.output.lower()


# ---------------------------------------------------------------------------
# haute init
# ---------------------------------------------------------------------------

class TestInit:
    def test_creates_project_structure(self, runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(cli, ["init"], catch_exceptions=False)
        assert result.exit_code == 0, result.output
        assert (tmp_path / "data").is_dir()
        assert (tmp_path / "test_quotes").is_dir()
        assert (tmp_path / "haute.toml").exists()
        assert (tmp_path / ".env.example").exists()
        assert (tmp_path / ".gitignore").exists()
        assert (tmp_path / "main.py").exists()
        assert (tmp_path / "pyproject.toml").exists()

        # haute.toml should reference main.py
        toml_content = (tmp_path / "haute.toml").read_text()
        assert 'pipeline = "main.py"' in toml_content

        # Starter pipeline should be valid Python
        py_content = (tmp_path / "main.py").read_text()
        compile(py_content, "<test>", "exec")

        # pyproject.toml should have haute as a dependency
        pyproject_content = (tmp_path / "pyproject.toml").read_text()
        assert '"haute"' in pyproject_content

    def test_works_alongside_uv_init(self, runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """haute init should layer on top of an existing pyproject.toml from uv init."""
        monkeypatch.chdir(tmp_path)
        # Simulate uv init output
        (tmp_path / "pyproject.toml").write_text(
            '[project]\nname = "motor-pricing"\nversion = "0.1.0"\n'
            'requires-python = ">=3.11"\ndependencies = []\n',
        )
        (tmp_path / "main.py").write_text("print('hello')\n")

        result = runner.invoke(cli, ["init"], catch_exceptions=False)
        assert result.exit_code == 0, result.output

        # Should pick up the project name from pyproject.toml
        toml_content = (tmp_path / "haute.toml").read_text()
        assert "motor-pricing" in toml_content

        # Should add haute to existing dependencies
        pyproject_content = (tmp_path / "pyproject.toml").read_text()
        assert '"haute"' in pyproject_content

        # main.py should be overwritten with the starter pipeline
        py_content = (tmp_path / "main.py").read_text()
        assert "haute.Pipeline" in py_content

    def test_skips_haute_dep_if_already_present(self, runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """If pyproject.toml already lists haute, don't duplicate it."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "pyproject.toml").write_text(
            '[project]\nname = "my-project"\nversion = "0.1.0"\n'
            'dependencies = [\n    "haute",\n]\n',
        )
        result = runner.invoke(cli, ["init"], catch_exceptions=False)
        assert result.exit_code == 0, result.output
        content = (tmp_path / "pyproject.toml").read_text()
        assert content.count('"haute"') == 1

    def test_appends_to_existing_gitignore(self, runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """If .gitignore exists (from uv init), append haute entries."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".gitignore").write_text("__pycache__/\n.venv/\n")
        result = runner.invoke(cli, ["init"], catch_exceptions=False)
        assert result.exit_code == 0, result.output
        content = (tmp_path / ".gitignore").read_text()
        assert "__pycache__/" in content  # original preserved
        assert ".env" in content
        assert "*.haute.json" in content

    def test_already_initialised_fails(self, runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "haute.toml").write_text("[project]\nname = \"x\"\n")
        result = runner.invoke(cli, ["init"])
        assert result.exit_code == 1
        assert "already" in result.output.lower()


# ---------------------------------------------------------------------------
# haute run
# ---------------------------------------------------------------------------

class TestRun:
    def test_run_explicit_file(self, runner: CliRunner, project_dir: Path):
        pipeline_file = str(project_dir / "main.py")
        result = runner.invoke(
            cli, ["run", pipeline_file], catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        assert "test_cli" in result.output
        assert "2 nodes" in result.output
        # Should show per-node results
        assert "source" in result.output
        assert "transform" in result.output
        assert "rows" in result.output

    def test_run_auto_discover(self, runner: CliRunner, project_dir: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.chdir(project_dir)
        result = runner.invoke(cli, ["run"], catch_exceptions=False)
        assert result.exit_code == 0, result.output
        assert "test_cli" in result.output

    def test_run_no_pipeline_found(self, runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(cli, ["run"])
        assert result.exit_code == 1
        assert "no pipeline file" in result.output.lower() or "Error" in result.output

    def test_run_file_not_found(self, runner: CliRunner):
        result = runner.invoke(cli, ["run", "/nonexistent/pipeline.py"])
        assert result.exit_code == 1
        assert "not found" in result.output.lower() or "Error" in result.output

    def test_run_empty_pipeline(self, runner: CliRunner, tmp_path: Path):
        """A .py file with no @pipeline.node functions should fail."""
        empty = tmp_path / "empty.py"
        empty.write_text("import polars as pl\nimport haute\npipeline = haute.Pipeline('e')\n")
        result = runner.invoke(cli, ["run", str(empty)])
        assert result.exit_code == 1
        assert "no" in result.output.lower() and "node" in result.output.lower()

    def test_run_reports_node_errors(self, runner: CliRunner, tmp_path: Path):
        """Pipeline with a node that errors should report the failure."""
        data = tmp_path / "data"
        data.mkdir()
        pl.DataFrame({"x": [1]}).write_parquet(data / "d.parquet")

        code = f'''\
import polars as pl
import haute

pipeline = haute.Pipeline("broken")


@pipeline.node(path="{data / 'd.parquet'}")
def source() -> pl.DataFrame:
    return pl.scan_parquet("{data / 'd.parquet'}")


@pipeline.node
def bad(source: pl.DataFrame) -> pl.DataFrame:
    return source.select("nonexistent_column")


pipeline.connect("source", "bad")
'''
        p = tmp_path / "broken.py"
        p.write_text(code)
        result = runner.invoke(cli, ["run", str(p)])
        assert result.exit_code == 1
        assert "failed" in result.output.lower() or "✗" in result.output


# ---------------------------------------------------------------------------
# haute serve (smoke test only — can't test the long-running server)
# ---------------------------------------------------------------------------

class TestServe:
    def test_serve_no_frontend_no_static_fails(self, runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """Without frontend/ or built static, serve should fail with a clear message."""
        monkeypatch.chdir(tmp_path)
        # Mock STATIC_DIR to a non-existent path so we hit the error branch
        # (the real STATIC_DIR may exist from a previous npm run build)
        monkeypatch.setattr("haute.server.STATIC_DIR", tmp_path / "nonexistent_static")
        result = runner.invoke(cli, ["serve", "--no-browser"])
        assert result.exit_code == 1
        assert "frontend" in result.output.lower() or "npm" in result.output.lower()
