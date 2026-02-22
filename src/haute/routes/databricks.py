"""Databricks Unity Catalog and data fetching endpoints."""

from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, HTTPException

from haute._logging import get_logger
from haute.schemas import (
    CacheStatusResponse,
    CatalogItem,
    CatalogListResponse,
    FetchProgressResponse,
    FetchTableRequest,
    FetchTableResponse,
    SchemaItem,
    SchemaListResponse,
    TableItem,
    TableListResponse,
    WarehouseItem,
    WarehouseListResponse,
)

logger = get_logger(component="server.databricks")

router = APIRouter(prefix="/api/databricks", tags=["databricks"])


def _get_databricks_client() -> Any:
    """Return a Databricks WorkspaceClient using credentials from .env."""
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        raise HTTPException(
            status_code=400,
            detail="databricks-sdk is not installed. "
            "Install with: pip install haute[databricks]",
        )

    host = os.getenv("DATABRICKS_HOST", "")
    token = os.getenv("DATABRICKS_TOKEN", "")

    if not host or not token:
        raise HTTPException(
            status_code=400,
            detail="DATABRICKS_HOST and DATABRICKS_TOKEN must be set in .env",
        )

    return WorkspaceClient(host=host, token=token)


@router.get("/warehouses", response_model=WarehouseListResponse)
async def list_databricks_warehouses() -> WarehouseListResponse:
    """List available Databricks SQL Warehouses."""
    try:
        w = _get_databricks_client()
        warehouses = [
            WarehouseItem(
                id=wh.id,
                name=wh.name,
                http_path=f"/sql/1.0/warehouses/{wh.id}",
                state=wh.state.value if wh.state else "UNKNOWN",
                size=wh.cluster_size or "",
            )
            for wh in w.warehouses.list()
        ]
        return WarehouseListResponse(warehouses=warehouses)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/catalogs", response_model=CatalogListResponse)
async def list_databricks_catalogs() -> CatalogListResponse:
    """List Unity Catalog catalogs."""
    try:
        w = _get_databricks_client()
        catalogs = [
            CatalogItem(name=c.name, comment=c.comment or "")
            for c in w.catalogs.list()
            if c.name
        ]
        return CatalogListResponse(catalogs=catalogs)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/schemas", response_model=SchemaListResponse)
async def list_databricks_schemas(catalog: str) -> SchemaListResponse:
    """List schemas within a Unity Catalog catalog."""
    try:
        w = _get_databricks_client()
        schemas = [
            SchemaItem(name=s.name, comment=s.comment or "")
            for s in w.schemas.list(catalog_name=catalog)
            if s.name
        ]
        return SchemaListResponse(schemas=schemas)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tables", response_model=TableListResponse)
async def list_databricks_tables(catalog: str, schema: str) -> TableListResponse:
    """List tables within a Unity Catalog schema."""
    try:
        w = _get_databricks_client()
        tables = [
            TableItem(
                name=t.name,
                full_name=t.full_name or f"{catalog}.{schema}.{t.name}",
                table_type=t.table_type.value if t.table_type else "",
                comment=t.comment or "",
            )
            for t in w.tables.list(catalog_name=catalog, schema_name=schema)
            if t.name
        ]
        return TableListResponse(tables=tables)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/fetch", response_model=FetchTableResponse)
async def fetch_databricks_table(body: FetchTableRequest) -> FetchTableResponse:
    """Fetch a Databricks table and cache it locally as parquet."""
    try:
        import asyncio

        from haute._databricks_io import fetch_and_cache

        result = await asyncio.wait_for(
            asyncio.to_thread(
                fetch_and_cache,
                table=body.table,
                http_path=body.http_path,
                query=body.query,
            ),
            timeout=600.0,
        )
        return FetchTableResponse.model_validate(result)
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Databricks fetch timed out (600s limit)")
    except ImportError:
        raise HTTPException(
            status_code=400,
            detail="databricks-sql-connector is not installed. "
            "Install with: pip install haute[databricks]",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fetch/progress", response_model=FetchProgressResponse)
async def get_fetch_progress(table: str) -> FetchProgressResponse:
    """Poll fetch progress for a table currently being downloaded."""
    from haute._databricks_io import fetch_progress

    progress = fetch_progress(table)
    if progress is None:
        return FetchProgressResponse(active=False)
    return FetchProgressResponse.model_validate({"active": True, **progress})


@router.get("/cache", response_model=CacheStatusResponse)
async def get_databricks_cache_status(table: str) -> CacheStatusResponse:
    """Check whether a Databricks table has been fetched and cached locally."""
    from haute._databricks_io import cache_info

    info = cache_info(table)
    if info is None:
        return CacheStatusResponse(cached=False, table=table)
    return CacheStatusResponse.model_validate({"cached": True, **info})


@router.delete("/cache", response_model=CacheStatusResponse)
async def delete_databricks_cache(table: str) -> CacheStatusResponse:
    """Delete the local parquet cache for a Databricks table."""
    from haute._databricks_io import clear_cache

    clear_cache(table)
    return CacheStatusResponse(cached=False, table=table)
