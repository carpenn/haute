"""``haute deploy`` command."""

from pathlib import Path

import click

from haute.cli._helpers import _load_deploy_config


@click.command()
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
    import os

    from haute.deploy._config import resolve_config
    from haute.deploy._validators import score_test_quotes, validate_deploy

    # 1. Load config
    config = _load_deploy_config(pipeline_file=pipeline_file, model_name=model_name)

    # Block local deploys - production changes must go through CI/CD
    is_ci = os.environ.get("CI") or os.environ.get("TF_BUILD") or os.environ.get("GITLAB_CI")
    if not dry_run and not is_ci:
        click.echo(
            "Error: Deploys must go through CI/CD.",
            err=True,
        )
        click.echo(
            "  Use --dry-run to validate locally without deploying.",
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
        click.echo(f"  \u2717 Resolution failed: {e}", err=True)
        raise SystemExit(1)

    n_kept = len(resolved.pruned_graph.nodes)
    n_removed = len(resolved.removed_node_ids)
    click.echo(
        f"  \u2713 Parsed pipeline ({n_kept + n_removed} nodes, "
        f"{len(resolved.pruned_graph.edges)} edges)"
    )
    click.echo(f"  \u2713 Pruned to output ancestors ({n_kept} nodes)")
    if n_removed:
        click.echo(
            f"  \u2713 Skipped {n_removed} nodes not in scoring path "
            f"({', '.join(resolved.removed_node_ids)})"
        )
    click.echo(f"  \u2713 Collected {len(resolved.artifacts)} artifacts")
    click.echo(f"  \u2713 Input node(s): {', '.join(resolved.input_node_ids)}")
    click.echo(f"  \u2713 Output node: {resolved.output_node_id}")
    click.echo(f"  \u2713 Inferred input schema ({len(resolved.input_schema)} columns)")
    click.echo(f"  \u2713 Inferred output schema ({len(resolved.output_schema)} columns)")

    # 3. Validate
    errors = validate_deploy(resolved)
    if errors:
        click.echo("\n  \u2717 Validation failed:", err=True)
        for err in errors:
            click.echo(f"    - {err}", err=True)
        raise SystemExit(1)
    click.echo("  \u2713 Validation passed")

    # 4. Score test quotes
    tq_results = score_test_quotes(resolved)
    if tq_results:
        all_ok = True
        for r in tq_results:
            status_icon = "\u2713" if r["status"] == "ok" else "\u2717"
            click.echo(
                f"  {status_icon} Test quotes: {r['file']:<30s} "
                f"{r['rows']:>3} rows  {r['status']}  ({r['time_ms']}ms)"
            )
            if r["status"] != "ok":
                click.echo(f"      Error: {r['error']}", err=True)
                all_ok = False
        if not all_ok:
            msg = "\n  \u2717 Test quote scoring failed."
            click.echo(msg + " Fix errors before deploying.", err=True)
            raise SystemExit(1)

    if dry_run:
        click.echo("\n  Dry run complete - no model was deployed.")
        return

    # 5. Deploy to target
    try:
        from haute.deploy import deploy as _deploy_fn

        result = _deploy_fn(config)
        click.echo(f"  \u2713 Deployed: {result.model_name} v{result.model_version}")
        if result.endpoint_url:
            click.echo(f"\nEndpoint ready:\n  POST {result.endpoint_url}")
        elif result.model_uri:
            click.echo("\nDeploy complete. Serve locally with:")
            click.echo(f'  mlflow models serve -m "{result.model_uri}" -p 5001')
    except ImportError as e:
        click.echo(f"\n  \u2717 Missing dependency: {e}", err=True)
        click.echo(
            "  Install the right extras for your target, e.g.: uv add 'haute[databricks]'",
            err=True,
        )
        raise SystemExit(1)
    except NotImplementedError as e:
        click.echo(f"\n  \u2717 {e}", err=True)
        raise SystemExit(1)
    except Exception as e:
        click.echo(f"\n  \u2717 Deployment failed: {e}", err=True)
        raise SystemExit(1)
