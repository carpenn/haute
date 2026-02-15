"""CLI entrypoint for Haute."""

import signal
import subprocess
import sys
import webbrowser
from pathlib import Path

import click


def _open_browser(url: str) -> None:
    """Open *url* in the default browser, suppressing noisy stderr from gio."""
    try:
        if sys.platform == "linux":
            subprocess.Popen(
                ["xdg-open", url], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
        elif sys.platform == "darwin":
            subprocess.Popen(
                ["open", url], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
        else:
            webbrowser.open(url)
    except Exception:
        webbrowser.open(url)


@click.group()
@click.version_option(package_name="haute")
def cli() -> None:
    """Haute — Open-source pricing engine for insurance teams on Databricks."""


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
    """Scaffold a new Haute pricing project."""
    project_dir = Path.cwd() / name

    if project_dir.exists():
        click.echo(f"Error: Directory '{name}' already exists.", err=True)
        raise SystemExit(1)

    project_dir.mkdir()
    (project_dir / "pipelines").mkdir()
    (project_dir / "data").mkdir()
    (project_dir / "test_quotes").mkdir()

    # Starter pipeline
    starter = '''\
"""My first pricing pipeline."""

import polars as pl
import haute

pipeline = haute.Pipeline("{name}", description="A new pricing pipeline")


@pipeline.node(path="data/input.parquet", deploy_input=True)
def read_data() -> pl.DataFrame:
    """Read input data."""
    return pl.scan_parquet("data/input.parquet")


@pipeline.node
def transform(read_data: pl.DataFrame) -> pl.DataFrame:
    """Transform the data."""
    return read_data


@pipeline.node(output=True)
def output(transform: pl.DataFrame) -> pl.DataFrame:
    """Final output."""
    return transform


pipeline.connect("read_data", "transform")
pipeline.connect("transform", "output")
'''
    (project_dir / "pipelines" / "main.py").write_text(
        starter.format(name=name)
    )

    # haute.toml — project config
    haute_toml = '''\
# Haute project configuration
# Docs: https://github.com/PricingFrontier/haute

[project]
name = "{name}"
pipeline = "pipelines/main.py"

# ─────────────────────────────────────────────────────────────────
# Deployment — Databricks MLflow Model Serving
# ─────────────────────────────────────────────────────────────────
[deploy]
target = "databricks"
model_name = "{name}"
endpoint_name = "{name}"

[deploy.databricks]
# Workspace credentials are read from .env (see .env.example)
#   DATABRICKS_HOST  = https://adb-xxxxx.xx.azuredatabricks.net
#   DATABRICKS_TOKEN = dapi...
experiment_name = "/Shared/haute/{name}"
catalog = "main"
schema = "pricing"
serving_workload_size = "Small"
serving_scale_to_zero = true

# ─────────────────────────────────────────────────────────────────
# Test quotes — example JSON payloads for pre-deploy validation
# ─────────────────────────────────────────────────────────────────
[test_quotes]
dir = "test_quotes"
'''
    (project_dir / "haute.toml").write_text(
        haute_toml.format(name=name)
    )

    # .env.example — template for secrets
    env_example = '''\
# Haute — Databricks credentials
# Copy this file to .env and fill in your values.
# .env is gitignored and will never be committed.
#
#   cp .env.example .env

# Databricks workspace URL (no trailing slash)
DATABRICKS_HOST=https://adb-1234567890123456.12.azuredatabricks.net

# Personal access token (Databricks > User Settings > Developer > Access Tokens)
DATABRICKS_TOKEN=your_databricks_token_here
'''
    (project_dir / ".env.example").write_text(env_example)

    # Starter test quote
    test_quote = '''\
[
  {
    "_description": "Example quote — replace with your own fields",
    "id": 1,
    "field_a": "value",
    "field_b": 42
  }
]
'''
    (project_dir / "test_quotes" / "example.json").write_text(test_quote)

    # .gitignore
    (project_dir / ".gitignore").write_text(
        "__pycache__/\n*.pyc\n.venv/\n.env\n"
    )

    click.echo(f"Created project '{name}/'")
    click.echo("  haute.toml            — project & deploy config")
    click.echo("  .env.example         — Databricks credentials template")
    click.echo("  pipelines/main.py    — starter pipeline")
    click.echo("  data/                — put your data files here")
    click.echo("  test_quotes/         — example JSON payloads for testing")
    click.echo("\nNext steps:")
    click.echo(f"  cd {name}")
    click.echo("  cp .env.example .env   # fill in Databricks credentials")
    click.echo("  haute serve")


@cli.command()
@click.argument("pipeline_file", required=False)
def run(pipeline_file: str | None) -> None:
    """Execute a pipeline and print the result.

    Uses the same parse → execute_graph path as the GUI so both
    produce identical results from the same .py file.
    """
    from haute.executor import execute_graph
    from haute.parser import parse_pipeline_file

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
        click.echo(
            "Error: No pipeline file found. Pass a path or create pipelines/main.py",
            err=True,
        )
        raise SystemExit(1)

    filepath = Path(pipeline_file)
    click.echo(f"Running pipeline: {filepath}")

    if not filepath.exists():
        click.echo(f"Error: File not found: {filepath}", err=True)
        raise SystemExit(1)

    try:
        graph = parse_pipeline_file(filepath)
    except Exception as e:
        click.echo(f"Error parsing pipeline: {e}", err=True)
        raise SystemExit(1)

    nodes = graph.get("nodes", [])
    if not nodes:
        click.echo("Error: No pipeline nodes found in file.", err=True)
        raise SystemExit(1)

    name = graph.get("pipeline_name", filepath.stem)
    click.echo(f"Pipeline: {name} ({len(nodes)} nodes)")

    try:
        results = execute_graph(graph)
    except Exception as e:
        click.echo(f"Error executing pipeline: {e}", err=True)
        raise SystemExit(1)

    # Report per-node results
    errors = 0
    for nid, res in results.items():
        status = res.get("status", "unknown")
        if status == "ok":
            rows = res.get("row_count", 0)
            cols = res.get("column_count", 0)
            click.echo(f"  ✓ {nid}: {rows:,} rows × {cols} cols")
        else:
            errors += 1
            click.echo(f"  ✗ {nid}: {res.get('error', 'unknown error')}")

    if errors:
        click.echo(f"\n{errors} node(s) failed.", err=True)
        raise SystemExit(1)

    # Print the last node's preview
    last_nid = list(results.keys())[-1]
    last = results[last_nid]
    if last.get("preview"):
        import polars as pl

        df = pl.DataFrame(last["preview"])
        click.echo(f"\nOutput — {last_nid} ({last['row_count']:,} rows):")
        click.echo(df)


@cli.command()
@click.option("--host", default="127.0.0.1", help="Host to bind to.")
@click.option("--port", default=8000, type=int, help="Backend API port.")
@click.option("--no-browser", is_flag=True, help="Don't open browser automatically.")
def serve(host: str, port: int, no_browser: bool) -> None:
    """Start the Haute UI server."""
    import uvicorn

    from haute.server import STATIC_DIR

    frontend_dir = _find_frontend_dir()
    dev_mode = frontend_dir is not None and (frontend_dir / "node_modules").exists()

    if dev_mode:
        click.echo("🔧 Dev mode: starting Vite dev server + FastAPI backend")
        click.echo("   Frontend → http://localhost:5173  (open this)")
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
                "haute.server:app",
                host=host,
                port=port,
                reload=True,
                reload_excludes=["pipelines/*", "examples/*", "*.haute.json"],
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
            "haute.server:app",
            host=host,
            port=port,
        )


@cli.command()
@click.argument("pipeline_file", required=False)
@click.option("--model-name", default=None, help="Override model name from haute.toml.")
@click.option("--dry-run", is_flag=True, help="Validate and score test quotes without deploying.")
def deploy(pipeline_file: str | None, model_name: str | None, dry_run: bool) -> None:
    """Deploy a pipeline as a live scoring API.

    Reads config from haute.toml + credentials from .env.
    Pipeline file, model name, and target are all optional —
    defaults come from [project] and [deploy] in haute.toml.
    """
    from haute.deploy._config import DeployConfig, resolve_config
    from haute.deploy._validators import score_test_quotes, validate_deploy

    # 1. Load config
    toml_path = Path.cwd() / "haute.toml"
    if toml_path.exists():
        config = DeployConfig.from_toml(toml_path)
        click.echo("  ✓ Loaded config from haute.toml")
    elif pipeline_file:
        config = DeployConfig(
            pipeline_file=Path(pipeline_file),
            model_name=model_name or Path(pipeline_file).stem,
        )
    else:
        click.echo(
            "Error: No haute.toml found and no pipeline file specified.",
            err=True,
        )
        raise SystemExit(1)

    # Apply CLI overrides
    overrides: dict[str, str | Path | None] = {}
    if pipeline_file:
        overrides["pipeline_file"] = Path(pipeline_file)
    if model_name:
        overrides["model_name"] = model_name
    config = config.override(**overrides)

    click.echo(f"\nDeploying pipeline: {config.model_name}")
    click.echo(f"  Pipeline: {config.pipeline_file}")

    # 2. Resolve (parse, prune, detect I/O, collect artifacts, infer schemas)
    try:
        resolved = resolve_config(config)
    except Exception as e:
        click.echo(f"  ✗ Resolution failed: {e}", err=True)
        raise SystemExit(1)

    n_kept = len(resolved.pruned_graph.get("nodes", []))
    n_removed = len(resolved.removed_node_ids)
    click.echo(f"  ✓ Parsed pipeline ({n_kept + n_removed} nodes, "
               f"{len(resolved.pruned_graph.get('edges', []))} edges)")
    click.echo(f"  ✓ Pruned to output ancestors ({n_kept} nodes)")
    if n_removed:
        click.echo(f"  ✓ Skipped {n_removed} nodes not in scoring path "
                   f"({', '.join(resolved.removed_node_ids)})")
    click.echo(f"  ✓ Collected {len(resolved.artifacts)} artifacts")
    click.echo(f"  ✓ Input node(s): {', '.join(resolved.input_node_ids)}")
    click.echo(f"  ✓ Output node: {resolved.output_node_id}")
    click.echo(f"  ✓ Inferred input schema ({len(resolved.input_schema)} columns)")
    click.echo(f"  ✓ Inferred output schema ({len(resolved.output_schema)} columns)")

    # 3. Validate
    errors = validate_deploy(resolved)
    if errors:
        click.echo("\n  ✗ Validation failed:", err=True)
        for err in errors:
            click.echo(f"    - {err}", err=True)
        raise SystemExit(1)
    click.echo("  ✓ Validation passed")

    # 4. Score test quotes
    tq_results = score_test_quotes(resolved)
    if tq_results:
        all_ok = True
        for r in tq_results:
            status_icon = "✓" if r["status"] == "ok" else "✗"
            click.echo(
                f"  {status_icon} Test quotes: {r['file']:<30s} "
                f"{r['rows']:>3} rows  {r['status']}  ({r['time_ms']}ms)"
            )
            if r["status"] != "ok":
                click.echo(f"      Error: {r['error']}", err=True)
                all_ok = False
        if not all_ok:
            click.echo("\n  ✗ Test quote scoring failed. Fix errors before deploying.", err=True)
            raise SystemExit(1)

    if dry_run:
        click.echo("\n  Dry run complete — no model was deployed.")
        return

    # 5. Deploy to MLflow
    try:
        from haute.deploy._mlflow import deploy_to_mlflow

        result = deploy_to_mlflow(resolved)
        click.echo(f"  ✓ Logged MLflow model: {result.model_name} v{result.model_version}")
        click.echo(f"  ✓ Model URI: {result.model_uri}")
        if result.endpoint_url:
            click.echo(f"\nEndpoint ready:\n  POST {result.endpoint_url}")
        else:
            click.echo("\nDeploy complete. Serve locally with:")
            click.echo(f'  mlflow models serve -m "{result.model_uri}" -p 5001')
    except ImportError:
        click.echo(
            "\n  ✗ mlflow is not installed. Install with: pip install haute[databricks]",
            err=True,
        )
        raise SystemExit(1)
    except Exception as e:
        click.echo(f"\n  ✗ Deployment failed: {e}", err=True)
        raise SystemExit(1)


@cli.command()
@click.argument("model_name", required=False)
def status(model_name: str | None) -> None:
    """Check the status of a deployed model."""
    # Load model name from haute.toml if not specified
    if model_name is None:
        toml_path = Path.cwd() / "haute.toml"
        if toml_path.exists():
            from haute.deploy._config import DeployConfig
            config = DeployConfig.from_toml(toml_path)
            model_name = config.model_name
        else:
            click.echo("Error: No model name specified and no haute.toml found.", err=True)
            raise SystemExit(1)

    try:
        from haute.deploy._mlflow import get_deploy_status
        info = get_deploy_status(model_name)
    except ImportError:
        click.echo(
            "Error: mlflow is not installed. Install with: pip install haute[databricks]",
            err=True,
        )
        raise SystemExit(1)

    if info.get("status") == "not_found":
        click.echo(f"Model '{model_name}' not found in MLflow Model Registry.")
        return

    click.echo(f"Model: {info['model_name']}")
    click.echo(f"  Latest version: {info['latest_version']}")
    click.echo(f"  Stage: {info.get('latest_stage', 'N/A')}")
    click.echo(f"  Status: {info['status']}")
    click.echo(f"  Run ID: {info.get('run_id', 'N/A')}")
