"""Train/test split strategies for model training."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Literal

import polars as pl


@dataclass
class SplitConfig:
    """Configuration for train/test splitting."""

    strategy: Literal["random", "temporal", "group"] = "random"
    test_size: float = 0.2
    seed: int = 42
    date_column: str | None = None
    cutoff_date: str | None = None
    group_column: str | None = None

    def __post_init__(self) -> None:
        if not 0 < self.test_size < 1:
            raise ValueError(f"test_size must be between 0 and 1, got {self.test_size}")
        if self.strategy == "temporal":
            if not self.date_column:
                raise ValueError("date_column is required for temporal split")
            if not self.cutoff_date:
                raise ValueError("cutoff_date is required for temporal split")
        if self.strategy == "group" and not self.group_column:
            raise ValueError("group_column is required for group split")


def split_data(
    df: pl.DataFrame, config: SplitConfig,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Split a DataFrame into train and test sets.

    Returns (train_df, test_df).
    """
    if len(df) == 0:
        raise ValueError("Cannot split an empty DataFrame")

    if config.strategy == "random":
        return _random_split(df, config.test_size, config.seed)
    elif config.strategy == "temporal":
        assert config.date_column is not None
        assert config.cutoff_date is not None
        return _temporal_split(df, config.date_column, config.cutoff_date)
    elif config.strategy == "group":
        assert config.group_column is not None
        return _group_split(df, config.group_column, config.test_size, config.seed)
    else:
        raise ValueError(f"Unknown split strategy: {config.strategy}")


def _random_split(
    df: pl.DataFrame, test_size: float, seed: int,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Split by random sampling with seed for reproducibility."""
    n = len(df)
    train_n = int(n * (1 - test_size))

    # Add row index, sample train indices, derive test via anti-join
    indexed = df.with_row_index("__split_idx__")
    train = indexed.sample(n=train_n, seed=seed).sort("__split_idx__")
    test = (
        indexed.join(train.select("__split_idx__"), on="__split_idx__", how="anti")
        .sort("__split_idx__")
    )
    return train.drop("__split_idx__"), test.drop("__split_idx__")


def _temporal_split(
    df: pl.DataFrame, date_column: str, cutoff_date: str,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Split by date column: train before cutoff, test on or after."""
    if date_column not in df.columns:
        raise ValueError(f"Date column '{date_column}' not found in DataFrame")

    cutoff = pl.lit(cutoff_date).str.to_date()
    date_col = pl.col(date_column)

    # Cast to Date if needed for comparison
    if df[date_column].dtype == pl.Utf8:
        date_col = date_col.str.to_date()

    train = df.filter(date_col < cutoff)
    test = df.filter(date_col >= cutoff)
    return train, test


def _group_split(
    df: pl.DataFrame,
    group_column: str,
    test_size: float,
    seed: int,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Split by hashing group values — all rows in a group go to the same set."""
    if group_column not in df.columns:
        raise ValueError(f"Group column '{group_column}' not found in DataFrame")

    # Get unique group values and deterministically assign to train/test
    unique_groups = df[group_column].unique().to_list()

    test_groups: set = set()
    for g in sorted(str(v) for v in unique_groups):
        h = hashlib.md5(f"{seed}:{g}".encode()).hexdigest()
        # Use first 8 hex chars as a fraction
        frac = int(h[:8], 16) / 0xFFFFFFFF
        if frac < test_size:
            test_groups.add(g)

    # If no groups assigned to test (small dataset), force at least one
    if not test_groups and len(unique_groups) > 1:
        test_groups.add(str(sorted(str(v) for v in unique_groups)[0]))

    is_test = pl.col(group_column).cast(pl.Utf8).is_in(list(test_groups))
    train = df.filter(~is_test)
    test = df.filter(is_test)
    return train, test
