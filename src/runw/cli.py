"""CLI entrypoint for runw."""

import os
import signal
import subprocess
import sys
import webbrowser
from pathlib import Path

import click


def _open_browser(url: str) -> None:
    """Open *url* in the default browser, suppressing noisy stderr from gio."""
    try:
        devnull = open(os.devnull, "w")
        if sys.platform == "linux":
            subprocess.Popen(["xdg-open", url], stdout=devnull, stderr=devnull)
        elif sys.platform == "darwin":
            subprocess.Popen(["open", url], stdout=devnull, stderr=devnull)
        else:
            webbrowser.open(url)
    except Exception:
        webbrowser.open(url)


@click.group()
@click.version_option(package_name="runw")
def cli() -> None:
    """runw — Open-source pricing engine for insurance teams on Databricks."""


def _find_frontend_dir() -> Path | None:
    """Walk up from cwd looking for a frontend/ directory with package.json."""
    cwd = Path.cwd()
    for parent in [cwd, *cwd.parents]:
        candidate = parent / "frontend"
        if (candidate / "package.json").exists():
            return candidate
    return None


@cli.command()
@click.argument("name")
def init(name: str) -> None:
    """Scaffold a new runw pricing project."""
    project_dir = Path.cwd() / name

    if project_dir.exists():
        click.echo(f"Error: Directory '{name}' already exists.", err=True)
        raise SystemExit(1)

    project_dir.mkdir()
    (project_dir / "pipelines").mkdir()
    (project_dir / "data").mkdir()

    # Starter pipeline
    starter = '''\
"""My first pricing pipeline."""

import polars as pl
import runw

pipeline = runw.Pipeline("{name}", description="A new pricing pipeline")


@pipeline.node(path="data/input.parquet")
def read_data() -> pl.DataFrame:
    """Read input data."""
    return pl.read_parquet("data/input.parquet")


@pipeline.node
def transform(df: pl.DataFrame) -> pl.DataFrame:
    """Transform the data."""
    return df


@pipeline.node
def output(df: pl.DataFrame) -> pl.DataFrame:
    """Final output."""
    return df
'''
    (project_dir / "pipelines" / "main.py").write_text(
        starter.format(name=name)
    )

    # .gitignore
    (project_dir / ".gitignore").write_text(
        "__pycache__/\n*.pyc\n.venv/\n"
    )

    click.echo(f"Created project '{name}/'")
    click.echo(f"  pipelines/main.py  — starter pipeline")
    click.echo(f"  data/              — put your data files here")
    click.echo(f"\nNext steps:")
    click.echo(f"  cd {name}")
    click.echo(f"  runw serve")


@cli.command()
@click.argument("pipeline_file", required=False)
def run(pipeline_file: str | None) -> None:
    """Execute a pipeline and print the result."""
    import importlib.util

    if pipeline_file is None:
        # Auto-discover
        for loc in [Path.cwd() / "pipelines", Path.cwd() / "examples"]:
            if loc.is_dir():
                candidates = sorted(loc.glob("*.py"))
                candidates = [c for c in candidates if c.name != "__init__.py"]
                if candidates:
                    pipeline_file = str(candidates[0])
                    break

    if not pipeline_file:
        click.echo("Error: No pipeline file found. Pass a path or create pipelines/main.py", err=True)
        raise SystemExit(1)

    filepath = Path(pipeline_file)
    click.echo(f"Running pipeline: {filepath}")

    spec = importlib.util.spec_from_file_location(filepath.stem, filepath)
    if spec is None or spec.loader is None:
        click.echo(f"Error: Cannot load {filepath}", err=True)
        raise SystemExit(1)

    import sys as _sys
    module = importlib.util.module_from_spec(spec)
    _sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    from runw.pipeline import Pipeline

    pipeline_obj = None
    for attr in dir(module):
        obj = getattr(module, attr)
        if isinstance(obj, Pipeline):
            pipeline_obj = obj
            break

    if pipeline_obj is None:
        click.echo("Error: No Pipeline instance found in file.", err=True)
        raise SystemExit(1)

    result = pipeline_obj.run()
    click.echo(f"\nResult ({len(result)} rows):")
    click.echo(result)


@cli.command()
@click.option("--host", default="127.0.0.1", help="Host to bind to.")
@click.option("--port", default=8000, type=int, help="Backend API port.")
@click.option("--no-browser", is_flag=True, help="Don't open browser automatically.")
def serve(host: str, port: int, no_browser: bool) -> None:
    """Start the runw UI server."""
    import uvicorn

    from runw.server import STATIC_DIR

    frontend_dir = _find_frontend_dir()
    dev_mode = frontend_dir is not None and (frontend_dir / "node_modules").exists()

    if dev_mode:
        click.echo("🔧 Dev mode: starting Vite dev server + FastAPI backend")
        click.echo(f"   Frontend → http://localhost:5173  (open this)")
        click.echo(f"   Backend  → http://{host}:{port}   (API only)")
        click.echo("")
        vite_proc = subprocess.Popen(
            ["npm", "run", "dev"],
            cwd=str(frontend_dir),
            stdout=sys.stdout,
            stderr=sys.stderr,
        )

        def _cleanup(signum, frame):
            vite_proc.terminate()
            sys.exit(0)

        signal.signal(signal.SIGINT, _cleanup)
        signal.signal(signal.SIGTERM, _cleanup)

        if not no_browser:
            import threading
            threading.Timer(2.0, _open_browser, args=("http://localhost:5173",)).start()

        try:
            uvicorn.run(
                "runw.server:app",
                host=host,
                port=port,
                reload=True,
                reload_excludes=["pipelines/*", "examples/*", "*.runw.json"],
            )
        finally:
            vite_proc.terminate()
    else:
        if not STATIC_DIR.exists():
            click.echo(
                "Error: No built frontend found. "
                "Run 'npm run build' in frontend/ first, or "
                "install node_modules for dev mode.",
                err=True,
            )
            raise SystemExit(1)

        if not no_browser:
            import threading
            threading.Timer(1.5, _open_browser, args=(f"http://{host}:{port}",)).start()

        uvicorn.run(
            "runw.server:app",
            host=host,
            port=port,
        )
