"""Haute — Open-source pricing engine for insurance teams on Databricks."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("haute")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"

from haute.pipeline import Pipeline

__all__ = ["Pipeline"]
