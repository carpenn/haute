"""File browsing and schema inspection endpoints."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException

from haute._logging import get_logger
from haute.routes._helpers import validate_safe_path
from haute.schemas import (
    BrowseFilesResponse,
    FileItem,
    SchemaResponse,
)

logger = get_logger(component="server.files")

router = APIRouter(prefix="/api", tags=["files"])


@router.get("/files", response_model=BrowseFilesResponse)
async def browse_files(
    dir: str = ".",
    extensions: str = ".parquet,.csv,.json,.xml",
) -> BrowseFilesResponse:
    """Browse files on disk for the file picker UI."""
    base = Path.cwd()
    target = validate_safe_path(base, dir)
    if not target.is_dir():
        raise HTTPException(status_code=404, detail=f"Directory not found: {dir}")

    ext_list = [e.strip() for e in extensions.split(",")]
    items: list[FileItem] = []

    for entry in sorted(target.iterdir()):
        rel = str(entry.relative_to(base))
        if entry.name.startswith("."):
            continue
        if entry.is_dir():
            items.append(FileItem(name=entry.name, path=rel, type="directory"))
        elif any(entry.name.endswith(ext) for ext in ext_list):
            items.append(
                FileItem(
                    name=entry.name,
                    path=rel,
                    type="file",
                    size=entry.stat().st_size,
                )
            )

    return BrowseFilesResponse(
        dir=str(target.relative_to(base)),
        items=items,
    )


@router.get("/schema", response_model=SchemaResponse)
async def get_schema(path: str) -> SchemaResponse:
    """Read a data file and return its schema + preview."""
    import polars as pl

    base = Path.cwd()
    target = validate_safe_path(base, path)
    if not target.is_file():
        raise HTTPException(status_code=404, detail=f"File not found: {path}")

    try:
        from haute.graph_utils import read_source

        lf = read_source(str(target))

        from haute.schemas import ColumnInfo

        schema = lf.collect_schema()
        columns = [ColumnInfo(name=c, dtype=str(d)) for c, d in schema.items()]
        preview_df = lf.head(5).collect()
        row_count = lf.select(pl.len()).collect().item()

        return SchemaResponse(
            path=path,
            columns=columns,
            row_count=row_count,
            column_count=len(columns),
            preview=preview_df.to_dicts(),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error("schema_read_failed", path=path, error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read schema for '{path}'. Check the server logs for details.",
        )


@router.get("/schema/databricks", response_model=SchemaResponse)
async def get_databricks_schema(table: str) -> SchemaResponse:
    """Return schema + preview from the local parquet cache of a Databricks table."""
    import polars as pl

    from haute._databricks_io import cached_path

    p = cached_path(table)
    if p is None:
        raise HTTPException(
            status_code=404,
            detail=f'Table "{table}" has not been fetched yet. '
            f"Click Fetch Data on the data source node to download it.",
        )

    try:
        import pyarrow.parquet as pq

        from haute.schemas import ColumnInfo

        df = pl.scan_parquet(p).head(1000).collect()
        columns = [ColumnInfo(name=c, dtype=str(df[c].dtype)) for c in df.columns]
        preview_df = df.head(5)
        row_count = pq.read_metadata(str(p)).num_rows

        return SchemaResponse(
            path=table,
            columns=columns,
            row_count=row_count,
            column_count=len(columns),
            preview=preview_df.to_dicts(),
        )
    except Exception as e:
        logger.error("databricks_schema_read_failed", table=table, error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read schema for table '{table}'. Check the server logs for details.",
        )
