"""Small helpers around delta-rs used by the loaders."""

from __future__ import annotations

import pyarrow as pa
from deltalake import DeltaTable


def open_table(uri: str) -> DeltaTable | None:
    return DeltaTable(uri) if DeltaTable.is_deltatable(uri) else None


def row_count(dt: DeltaTable) -> int:
    actions = pa.table(dt.get_add_actions(flatten=True))
    return int(sum(v or 0 for v in actions.column("num_records").to_pylist())) if actions.num_rows else 0


def partition_values(uri: str, column: str) -> set[str]:
    """Distinct values of a partition column that currently have data."""
    dt = open_table(uri)
    if dt is None:
        return set()
    return {str(p[column]) for p in dt.partitions() if p.get(column) is not None}


def partition_row_counts(uri: str, column: str) -> dict[str, int]:
    dt = open_table(uri)
    if dt is None:
        return {}
    actions = pa.table(dt.get_add_actions(flatten=True))
    key = f"partition.{column}"
    counts: dict[str, int] = {}
    if actions.num_rows and key in actions.column_names:
        for value, n in zip(actions.column(key).to_pylist(), actions.column("num_records").to_pylist(), strict=True):
            counts[str(value)] = counts.get(str(value), 0) + int(n or 0)
    return counts
