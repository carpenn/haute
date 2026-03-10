"""``haute run`` command."""

import click


@click.command()
@click.argument("pipeline_file", required=False)
def run(pipeline_file: str | None) -> None:
    """Execute a pipeline and print the result.

    Uses the same parse -> execute_graph path as the GUI so both
    produce identical results from the same .py file.
    """
    from haute.cli._helpers import resolve_pipeline_file
    from haute.executor import execute_graph
    from haute.parser import parse_pipeline_file

    filepath = resolve_pipeline_file(pipeline_file)
    click.echo(f"Running pipeline: {filepath}")

    try:
        graph = parse_pipeline_file(filepath)
    except Exception as e:
        click.echo(f"Error parsing pipeline: {e}", err=True)
        raise SystemExit(1)

    nodes = graph.nodes
    if not nodes:
        click.echo("Error: No pipeline nodes found in file.", err=True)
        raise SystemExit(1)

    name = graph.pipeline_name or filepath.stem
    click.echo(f"Pipeline: {name} ({len(nodes)} nodes)")

    try:
        results = execute_graph(graph)
    except Exception as e:
        click.echo(f"Error executing pipeline: {e}", err=True)
        raise SystemExit(1)

    # Report per-node results
    errors = 0
    for nid, res in results.items():
        if res.status == "ok":
            click.echo(f"  \u2713 {nid}: {res.row_count:,} rows \u00d7 {res.column_count} cols")
        else:
            errors += 1
            click.echo(f"  \u2717 {nid}: {res.error or 'unknown error'}")

    if errors:
        click.echo(f"\n{errors} node(s) failed.", err=True)
        raise SystemExit(1)

    # Print the last node's preview
    last_nid = list(results.keys())[-1]
    last = results[last_nid]
    if last.preview:
        import polars as pl

        df = pl.DataFrame(last.preview)
        click.echo(f"\nOutput - {last_nid} ({last.row_count:,} rows):")
        click.echo(df)
