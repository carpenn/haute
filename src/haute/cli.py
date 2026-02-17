"""CLI entrypoint for Haute."""

import signal
import subprocess
import sys
import time
import webbrowser
from pathlib import Path

import click


def _open_browser(url: str) -> None:
    """Open *url* in the default browser, suppressing noisy stderr from gio."""
    try:
        if sys.platform == "linux":
            rc = subprocess.call(
                ["xdg-open", url],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if rc != 0:
                webbrowser.open(url)
        elif sys.platform == "darwin":
            subprocess.Popen(
                ["open", url],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        else:
            webbrowser.open(url)
    except Exception:
        webbrowser.open(url)


@click.group()
@click.version_option(package_name="haute")
def cli() -> None:
    """Haute - Open-source pricing engine for insurance teams on Databricks."""


def _find_frontend_dir() -> Path | None:
    """Walk up from cwd looking for a frontend/ directory with package.json."""
    cwd = Path.cwd()
    for parent in [cwd, *cwd.parents]:
        candidate = parent / "frontend"
        if (candidate / "package.json").exists():
            return candidate
    return None


@cli.command()
@click.option(
    "--target",
    type=click.Choice(["databricks", "docker", "sagemaker", "azure-ml"]),
    default="databricks",
    help="Deploy target (default: databricks).",
)
@click.option(
    "--ci",
    type=click.Choice(["github", "none"]),
    default="github",
    help="CI/CD provider (default: github).",
)
def init(target: str, ci: str) -> None:
    """Scaffold a Haute pricing project in the current directory.

    Generates haute.toml, CI/CD workflows, credentials template, and a
    starter pipeline - all configured for the chosen deploy target and
    CI provider.

    \b
    Examples:
      haute init                                  # databricks + github
      haute init --target docker --ci none        # docker, no CI
      haute init --target sagemaker --ci github   # AWS + github
    """
    import tomllib

    from haute._scaffold import (
        env_example,
        github_ci_yml,
        github_deploy_yml,
        haute_toml,
        pre_commit_hook,
        starter_pipeline,
        starter_test,
        starter_test_quote,
    )

    # Solo mode is configured in haute.toml, not via a CLI flag.
    # Default is team mode; user sets min_approvers = 0 for solo.

    project_dir = Path.cwd()

    if (project_dir / "haute.toml").exists():
        click.echo("Error: haute.toml already exists - project already initialised.", err=True)
        raise SystemExit(1)

    # ── Resolve project name ──────────────────────────────────────
    pyproject_path = project_dir / "pyproject.toml"
    name = project_dir.name.replace("-", "_").replace(" ", "_").lower()

    if pyproject_path.exists():
        with open(pyproject_path, "rb") as f:
            pyproject = tomllib.load(f)
        if "project" in pyproject and "name" in pyproject["project"]:
            name = pyproject["project"]["name"]

    # ── pyproject.toml - ensure haute is a dependency ─────────────
    _ensure_haute_dependency(pyproject_path, name)

    # ── Directories ───────────────────────────────────────────────
    (project_dir / "data").mkdir(exist_ok=True)

    # ── main.py - starter pipeline ────────────────────────────────
    (project_dir / "main.py").write_text(starter_pipeline(name), encoding="utf-8")

    # ── haute.toml - project + deploy + safety + CI config ────────
    (project_dir / "haute.toml").write_text(
        haute_toml(name, target, ci),
        encoding="utf-8",
    )

    # ── .env.example - target-specific credentials ────────────────
    (project_dir / ".env.example").write_text(env_example(target), encoding="utf-8")

    # ── Starter test file + test quotes ─────────────────────────────
    tests_dir = project_dir / "tests"
    tests_dir.mkdir(exist_ok=True)
    quotes_dir = tests_dir / "quotes"
    quotes_dir.mkdir(exist_ok=True)
    (quotes_dir / "example.json").write_text(
        starter_test_quote(),
        encoding="utf-8",
    )
    (tests_dir / "test_pipeline.py").write_text(
        starter_test(name),
        encoding="utf-8",
    )

    # ── CI/CD workflow files ──────────────────────────────────────
    ci_files: list[str] = []
    if ci == "github":
        workflows_dir = project_dir / ".github" / "workflows"
        workflows_dir.mkdir(parents=True, exist_ok=True)
        (workflows_dir / "ci.yml").write_text(github_ci_yml(), encoding="utf-8")
        (workflows_dir / "deploy.yml").write_text(
            github_deploy_yml(target),
            encoding="utf-8",
        )
        ci_files = [".github/workflows/ci.yml", ".github/workflows/deploy.yml"]

    # ── Pre-commit hook ───────────────────────────────────────────
    hooks_dir = project_dir / ".githooks"
    hooks_dir.mkdir(exist_ok=True)
    hook_path = hooks_dir / "pre-commit"
    hook_path.write_text(pre_commit_hook(), encoding="utf-8")
    hook_path.chmod(0o755)

    # Install into .git/hooks if inside a git repo
    git_hooks_dir = project_dir / ".git" / "hooks"
    if git_hooks_dir.is_dir():
        installed = git_hooks_dir / "pre-commit"
        installed.write_text(pre_commit_hook(), encoding="utf-8")
        installed.chmod(0o755)

    # ── .gitignore - append if exists, create if not ──────────────
    gitignore_path = project_dir / ".gitignore"
    haute_entries = ".env\n*.haute.json\n"
    if gitignore_path.exists():
        existing = gitignore_path.read_text()
        missing = [line for line in haute_entries.splitlines() if line and line not in existing]
        if missing:
            with open(gitignore_path, "a", encoding="utf-8") as f:
                f.write("\n# Haute\n" + "\n".join(missing) + "\n")
    else:
        gitignore_path.write_text(
            "__pycache__/\n*.pyc\n.venv/\n.env\n*.haute.json\n",
            encoding="utf-8",
        )

    # ── Summary ───────────────────────────────────────────────────
    click.echo(f"Initialised Haute project '{name}' ({target} + {ci})\n")
    click.echo("  pyproject.toml        - haute added as dependency")
    click.echo("  haute.toml            - project, deploy, safety & CI config")
    click.echo(f"  .env.example         - {target} credentials template")
    click.echo("  main.py              - starter pipeline")
    click.echo("  data/                - put your data files here")
    click.echo("  tests/               - starter test + example quote payloads")
    click.echo("  .githooks/pre-commit - auto-format on commit (ruff)")
    for f in ci_files:
        click.echo(f"  {f}")
    if git_hooks_dir.is_dir():
        click.echo("  .git/hooks/pre-commit  (installed)")
    click.echo("\nNext steps:")
    click.echo("  uv sync                # install dependencies")
    click.echo("  cp .env.example .env   # fill in credentials")
    click.echo("  haute serve")


_DEV_DEPS_BLOCK = """
[dependency-groups]
dev = [
    "ruff>=0.8",
    "mypy>=1.13",
    "pytest>=8.3",
]
"""

_MYPY_BLOCK = """
[tool.mypy]
ignore_missing_imports = false

[[tool.mypy.overrides]]
module = ["haute.*", "catboost.*", "xgboost.*", "lightgbm.*", "sklearn.*"]
ignore_missing_imports = true
"""


def _ensure_haute_dependency(pyproject_path: Path, name: str) -> None:
    """Add ``haute`` to pyproject.toml dependencies.

    If pyproject.toml exists, insert ``"haute"`` into the dependencies
    list (if not already present).  If it doesn't exist, create a
    minimal pyproject.toml.

    Also ensures a ``[dependency-groups]`` dev section exists with
    ruff, mypy, and pytest so that the generated CI workflows work.
    """
    if pyproject_path.exists():
        text = pyproject_path.read_text(encoding="utf-8")
        if "haute" not in text:
            # Insert into existing dependencies list
            if "dependencies = [" in text:
                text = text.replace(
                    "dependencies = [",
                    'dependencies = [\n    "haute",',
                    1,
                )
            else:
                # No dependencies key - append a section
                text += '\n[project]\ndependencies = [\n    "haute",\n]\n'
        if "[dependency-groups]" not in text:
            text += _DEV_DEPS_BLOCK
        if "[tool.mypy]" not in text:
            text += _MYPY_BLOCK
        pyproject_path.write_text(text, encoding="utf-8")
    else:
        pyproject_path.write_text(
            f'[project]\nname = "{name}"\nversion = "0.1.0"\n'
            f'requires-python = ">=3.11"\n'
            f'dependencies = [\n    "haute",\n]\n' + _DEV_DEPS_BLOCK + _MYPY_BLOCK,
            encoding="utf-8",
        )


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
        from haute.discovery import discover_pipelines

        found = discover_pipelines()
        if found:
            pipeline_file = str(found[0])

    if not pipeline_file:
        click.echo(
            "Error: No pipeline file found. Pass a path or create main.py",
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
        click.echo(f"\nOutput - {last_nid} ({last['row_count']:,} rows):")
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
                reload_excludes=["*.haute.json"],
                log_level="warning",
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
@click.option(
    "--endpoint-suffix",
    default=None,
    help='Suffix appended to endpoint name (e.g. "-staging").',
)
@click.option("--dry-run", is_flag=True, help="Validate and score test quotes without deploying.")
def deploy(
    pipeline_file: str | None,
    model_name: str | None,
    endpoint_suffix: str | None,
    dry_run: bool,
) -> None:
    """Deploy a pipeline as a live scoring API.

    Reads config from haute.toml + credentials from .env.
    Pipeline file, model name, and target are all optional -
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
    if endpoint_suffix:
        overrides["endpoint_suffix"] = endpoint_suffix
    config = config.override(**overrides)

    click.echo(f"\nDeploying pipeline: {config.model_name}")
    click.echo(f"  Pipeline: {config.pipeline_file}")
    click.echo(f"  Endpoint: {config.effective_endpoint_name}")

    # 2. Resolve (parse, prune, detect I/O, collect artifacts, infer schemas)
    try:
        resolved = resolve_config(config)
    except Exception as e:
        click.echo(f"  ✗ Resolution failed: {e}", err=True)
        raise SystemExit(1)

    n_kept = len(resolved.pruned_graph.get("nodes", []))
    n_removed = len(resolved.removed_node_ids)
    click.echo(
        f"  ✓ Parsed pipeline ({n_kept + n_removed} nodes, "
        f"{len(resolved.pruned_graph.get('edges', []))} edges)"
    )
    click.echo(f"  ✓ Pruned to output ancestors ({n_kept} nodes)")
    if n_removed:
        click.echo(
            f"  ✓ Skipped {n_removed} nodes not in scoring path "
            f"({', '.join(resolved.removed_node_ids)})"
        )
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
        click.echo("\n  Dry run complete - no model was deployed.")
        return

    # 5. Deploy to MLflow
    try:
        from haute.deploy._mlflow import deploy_to_mlflow

        result = deploy_to_mlflow(resolved, progress=lambda msg: click.echo(f"  … {msg}"))
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
@click.option(
    "--version-only",
    is_flag=True,
    help="Print only the latest version number (for scripting).",
)
def status(model_name: str | None, version_only: bool) -> None:
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

    if version_only:
        click.echo(info.get("latest_version", 0))
        return

    if info.get("status") == "not_found":
        click.echo(f"Model '{model_name}' not found in MLflow Model Registry.")
        return

    click.echo(f"Model: {info['model_name']}")
    click.echo(f"  Latest version: {info['latest_version']}")
    click.echo(f"  Stage: {info.get('latest_stage', 'N/A')}")
    click.echo(f"  Status: {info['status']}")
    click.echo(f"  Run ID: {info.get('run_id', 'N/A')}")


@cli.command()
@click.argument("pipeline_file", required=False)
def lint(pipeline_file: str | None) -> None:
    """Validate pipeline structure without deploying.

    Parses the pipeline, checks for structural issues (orphan nodes,
    missing edges, syntax errors), and reports any problems found.
    """
    from haute.parser import parse_pipeline_file

    if pipeline_file is None:
        toml_path = Path.cwd() / "haute.toml"
        if toml_path.exists():
            import tomllib

            with open(toml_path, "rb") as f:
                data = tomllib.load(f)
            pipeline_file = data.get("project", {}).get("pipeline", "main.py")
        else:
            pipeline_file = "main.py"

    filepath = Path(pipeline_file)
    if not filepath.exists():
        click.echo(f"Error: Pipeline file not found: {filepath}", err=True)
        raise SystemExit(1)

    click.echo(f"Linting pipeline: {filepath}")

    try:
        graph = parse_pipeline_file(filepath)
    except Exception as e:
        click.echo(f"  ✗ Parse error: {e}", err=True)
        raise SystemExit(1)

    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])
    node_ids = {n["id"] for n in nodes}

    if not nodes:
        click.echo("  ✗ No nodes found in pipeline.", err=True)
        raise SystemExit(1)

    errors: list[str] = []

    # Check for edges referencing non-existent nodes
    for edge in edges:
        if edge["source"] not in node_ids:
            errors.append(f"Edge references missing source node: {edge['source']}")
        if edge["target"] not in node_ids:
            errors.append(f"Edge references missing target node: {edge['target']}")

    # Check for nodes with parse errors
    for node in nodes:
        data = node.get("data", {})
        if data.get("parseError"):
            errors.append(f"Node '{node['id']}' has parse error: {data['parseError']}")

    # Check for orphan nodes (no edges at all, in a multi-node graph)
    if len(nodes) > 1:
        connected = set()
        for edge in edges:
            connected.add(edge["source"])
            connected.add(edge["target"])
        orphans = node_ids - connected
        for orphan in orphans:
            errors.append(f"Node '{orphan}' is disconnected (no edges)")

    if errors:
        click.echo(f"\n  Found {len(errors)} issue(s):", err=True)
        for err in errors:
            click.echo(f"  ✗ {err}", err=True)
        raise SystemExit(1)

    name = graph.get("pipeline_name", filepath.stem)
    click.echo(f"  ✓ Pipeline '{name}': {len(nodes)} nodes, {len(edges)} edges")
    click.echo("  ✓ No structural issues found.")


@cli.command()
@click.option(
    "--endpoint-suffix",
    default=None,
    help='Suffix appended to endpoint name (e.g. "-staging").',
)
def smoke(endpoint_suffix: str | None) -> None:
    """Score test quotes against a live serving endpoint.

    Sends each test quote JSON file as an HTTP request to the deployed
    endpoint and validates the response. Used after staging deploys to
    verify the endpoint is functional.
    """
    from haute.deploy._config import DeployConfig
    from haute.deploy._validators import load_test_quote_file

    toml_path = Path.cwd() / "haute.toml"
    if not toml_path.exists():
        click.echo("Error: No haute.toml found.", err=True)
        raise SystemExit(1)

    config = DeployConfig.from_toml(toml_path)
    if endpoint_suffix:
        config = config.override(endpoint_suffix=endpoint_suffix)

    endpoint_name = config.effective_endpoint_name
    tq_dir = config.test_quotes_dir

    if tq_dir is None or not tq_dir.is_dir():
        click.echo(f"Error: No test quotes directory found (resolved: {tq_dir}).", err=True)
        raise SystemExit(1)

    json_files = sorted(tq_dir.glob("*.json"))
    if not json_files:
        click.echo("Error: No .json files in tests/quotes/.", err=True)
        raise SystemExit(1)

    click.echo(f"Smoke testing endpoint: {endpoint_name}")
    click.echo(f"  Target: {config.target}")

    if config.target != "databricks":
        click.echo(f"  ⚠ Smoke test not yet implemented for target '{config.target}'.", err=True)
        click.echo("  Skipping smoke test.")
        return

    # Databricks endpoint scoring
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        click.echo(
            "Error: databricks-sdk not installed. Install with: uv add haute[databricks]",
            err=True,
        )
        raise SystemExit(1)

    from haute.deploy._config import _load_env

    _load_env(Path.cwd())

    ws = WorkspaceClient()

    # Wait for endpoint to be ready (provisioning can take 10-15 min)
    max_wait = 30 * 60  # 30 minutes
    poll_interval = 30  # seconds
    click.echo(f"  … Waiting for endpoint '{endpoint_name}' to be ready...")
    waited = 0
    while waited < max_wait:
        try:
            ep = ws.serving_endpoints.get(endpoint_name)
            state = ep.state
            ready = getattr(state, "ready", None) if state else None
            config_update = getattr(state, "config_update", None) if state else None
            ready_str = str(ready).rsplit(".", 1)[-1] if ready else ""
            update_str = str(config_update).rsplit(".", 1)[-1] if config_update else ""
            if ready_str == "READY" and update_str in ("", "NOT_UPDATING"):
                click.echo(f"  ✓ Endpoint ready (waited {waited}s)")
                break
            status_msg = f"ready={ready}"
            if config_update and config_update != "NOT_UPDATING":
                status_msg += f", config_update={config_update}"
            click.echo(f"  … Endpoint not ready ({status_msg}), polling in {poll_interval}s...")
        except Exception as exc:
            click.echo(f"  … Endpoint not found or error ({exc}), retrying in {poll_interval}s...")
        time.sleep(poll_interval)
        waited += poll_interval
    else:
        click.echo(
            f"  ✗ Endpoint '{endpoint_name}' not ready after {max_wait // 60} minutes.",
            err=True,
        )
        raise SystemExit(1)

    all_ok = True

    for jf in json_files:
        try:
            cleaned = load_test_quote_file(jf)

            response = ws.serving_endpoints.query(
                name=endpoint_name,
                dataframe_records=cleaned,
            )

            predictions = response.predictions
            if predictions is None:
                raise ValueError("Endpoint returned no predictions")

            n_rows = len(predictions) if isinstance(predictions, list) else 1
            click.echo(f"  ✓ {jf.name}: {n_rows} predictions returned")
        except Exception as exc:
            click.echo(f"  ✗ {jf.name}: {exc}", err=True)
            all_ok = False

    if not all_ok:
        click.echo("\n  ✗ Smoke test failed.", err=True)
        raise SystemExit(1)

    click.echo("\n  ✓ All smoke tests passed.")
