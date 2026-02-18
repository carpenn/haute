"""Databricks table I/O via the SQL Connector.

Reads Unity Catalog tables through a Databricks SQL Warehouse and returns
Polars LazyFrames, fitting seamlessly into the existing eager/lazy
execution model.

Connection details live on the data source node (``http_path`` in config).
Secrets are resolved from the environment with fallback:
    DATABRICKS_DATA_HOST  → DATABRICKS_HOST
    DATABRICKS_DATA_TOKEN → DATABRICKS_TOKEN
"""

from __future__ import annotations

import os

import polars as pl


class DatabricksConfigError(Exception):
    """Raised when required Databricks data credentials are missing."""


def _get_credentials(http_path: str | None = None) -> tuple[str, str, str]:
    """Resolve Databricks data credentials.

    Args:
        http_path: SQL Warehouse HTTP path from the node config.
            Falls back to ``DATABRICKS_DATA_HTTP_PATH`` env var.

    Returns (host, token, http_path).
    """
    host = os.getenv("DATABRICKS_DATA_HOST") or os.getenv("DATABRICKS_HOST", "")
    token = os.getenv("DATABRICKS_DATA_TOKEN") or os.getenv("DATABRICKS_TOKEN", "")
    resolved_http_path = http_path or os.getenv("DATABRICKS_DATA_HTTP_PATH", "")

    missing: list[str] = []
    if not host:
        missing.append("DATABRICKS_DATA_HOST (or DATABRICKS_HOST)")
    if not token:
        missing.append("DATABRICKS_DATA_TOKEN (or DATABRICKS_TOKEN)")
    if not resolved_http_path:
        missing.append(
            "http_path on the data source node "
            "(or DATABRICKS_DATA_HTTP_PATH env var)"
        )

    if missing:
        raise DatabricksConfigError(
            "Missing Databricks data credentials:\n  "
            + "\n  ".join(missing)
            + "\nSet host/token in .env and http_path on the data source node."
        )

    # Strip protocol for the SQL connector (it wants bare hostname)
    host = host.rstrip("/")
    if host.startswith("https://"):
        host = host[len("https://"):]
    elif host.startswith("http://"):
        host = host[len("http://"):]

    return host, token, resolved_http_path


def read_databricks_table(
    table: str,
    row_limit: int | None = None,
    http_path: str | None = None,
    query: str | None = None,
) -> pl.LazyFrame:
    """Read a Unity Catalog table via the Databricks SQL Connector.

    Args:
        table: Fully-qualified table name (e.g. ``catalog.schema.table``).
        row_limit: If set, wraps the query in a LIMIT so only that many
            rows are fetched from the warehouse.  Used by the
            preview/trace eager paths to avoid pulling entire tables.
        http_path: SQL Warehouse HTTP path from the node config.
            Falls back to ``DATABRICKS_DATA_HTTP_PATH`` env var.
        query: Custom select clause (e.g. ``SELECT col1, col2`` or
            ``SELECT * ... WHERE x > 0``).  Combined with ``table`` as
            ``{query} FROM {table}``.  Defaults to ``SELECT *``.

    Returns:
        A Polars LazyFrame wrapping the fetched data.
    """
    from databricks import sql as dbsql

    host, token, http_path = _get_credentials(http_path)

    select_clause = query.strip().rstrip(";") if query else "SELECT *"
    sql_query = f"{select_clause} FROM {table}"  # noqa: S608
    if row_limit is not None and row_limit > 0:
        sql_query = f"SELECT * FROM ({sql_query}) _limited LIMIT {row_limit}"

    with dbsql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            arrow_table = cursor.fetchall_arrow()

    return pl.from_arrow(arrow_table).lazy()
