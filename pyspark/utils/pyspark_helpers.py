"""
Shared PySpark utility functions replacing dbt cross-database macros.

Each function in this module corresponds to a Jinja macro under
``macros/cross_database_utils/`` and provides equivalent functionality
using the PySpark DataFrame / Column API.

Seed-related macros (``load_seed``, ``versioned_seed_paths``) are
intentionally excluded — seeds remain SQL-managed.
"""

from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING, List, Optional, Sequence

from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, IntegerType, TimestampType

if TYPE_CHECKING:
    pass


# ---------------------------------------------------------------------------
# safe_cast_date  (replaces try_to_cast_date.sql)
# ---------------------------------------------------------------------------
def safe_cast_date(col: Column | str) -> Column:
    """Attempt to cast *col* to ``DateType``, returning ``NULL`` on failure.

    Mirrors the cross-database ``try_to_cast_date`` macro.  PySpark's
    ``to_date`` already returns ``NULL`` for un-parseable values when used
    with ``try_to_cast``.
    """
    c = F.col(col) if isinstance(col, str) else col
    return F.to_date(c.cast("string"))


# ---------------------------------------------------------------------------
# safe_cast_timestamp  (replaces try_to_cast_datetime.sql)
# ---------------------------------------------------------------------------
def safe_cast_timestamp(col: Column | str) -> Column:
    """Attempt to cast *col* to ``TimestampType``, returning ``NULL`` on
    failure.

    Mirrors the cross-database ``try_to_cast_datetime`` macro.
    """
    c = F.col(col) if isinstance(col, str) else col
    return F.to_timestamp(c.cast("string"))


# ---------------------------------------------------------------------------
# safe_cast_int  (replaces try_to_cast_int.sql)
# ---------------------------------------------------------------------------
def safe_cast_int(col: Column | str) -> Column:
    """Attempt to cast *col* to ``IntegerType``, returning ``NULL`` on
    failure.

    Uses a regex guard so non-numeric strings yield ``NULL`` instead of
    raising at runtime.
    """
    c = F.col(col) if isinstance(col, str) else col
    trimmed = F.trim(c.cast("string"))
    return (
        F.when(trimmed.rlike(r"^-?\d+$"), trimmed.cast(IntegerType()))
        .otherwise(F.lit(None).cast(IntegerType()))
    )


# ---------------------------------------------------------------------------
# year_month  (replaces year_month.sql)
# ---------------------------------------------------------------------------
def year_month(col: Column | str) -> Column:
    """Format a date/timestamp column as a ``YYYYMM`` string.

    Mirrors the cross-database ``year_month`` macro.
    """
    c = F.col(col) if isinstance(col, str) else col
    return F.date_format(c.cast(DateType()), "yyyyMM")


# ---------------------------------------------------------------------------
# apply_regex  (replaces apply_regex.sql)
# ---------------------------------------------------------------------------
def apply_regex(col: Column | str, pattern: str) -> Column:
    """Return a boolean column indicating whether *col* matches *pattern*.

    Mirrors the cross-database ``apply_regex`` macro (``regexp_like`` /
    ``regexp_contains`` equivalents).
    """
    c = F.col(col) if isinstance(col, str) else col
    return c.rlike(pattern)


# ---------------------------------------------------------------------------
# substring_col  (replaces substring.sql)
# ---------------------------------------------------------------------------
def substring_col(col: Column | str, start: int, length: int) -> Column:
    """Return a substring of *col* starting at *start* for *length* chars.

    Mirrors the cross-database ``substring`` macro.  PySpark's
    ``F.substring`` is 1-based, matching SQL semantics.
    """
    c = F.col(col) if isinstance(col, str) else col
    return F.substring(c, start, length)


# ---------------------------------------------------------------------------
# create_json_object  (replaces create_json_object.sql)
# ---------------------------------------------------------------------------

def _snake_to_camel(name: str) -> str:
    """Convert a ``snake_case`` name to ``camelCase``."""
    parts = name.split("_")
    return parts[0] + "".join(p.capitalize() for p in parts[1:])


def create_json_object(
    df: DataFrame,
    group_cols: str | List[str],
    obj_col: str,
    obj_fields: List[str],
) -> DataFrame:
    """Group *df* by *group_cols* and aggregate *obj_fields* into a JSON
    array column named *obj_col*.

    Mirrors the cross-database ``create_json_object`` macro which builds
    per-group JSON arrays using ``object_construct`` / ``struct`` +
    ``array_agg`` / ``collect_list``.

    Parameters
    ----------
    df:
        Source DataFrame (equivalent to the ``table_ref`` parameter).
    group_cols:
        Column name(s) to group by.
    obj_col:
        Name of the resulting JSON column.
    obj_fields:
        Column names to include inside each JSON object.  Column names
        containing ``"list"`` (case-insensitive) are parsed from JSON
        strings before nesting to prevent double-escaping.
    """
    if isinstance(group_cols, str):
        group_cols = [group_cols]

    struct_fields = []
    for field in obj_fields:
        camel = _snake_to_camel(field)
        if "list" in field.lower():
            struct_fields.append(
                F.from_json(F.col(field), "array<string>").alias(camel)
            )
        else:
            struct_fields.append(F.col(field).alias(camel))

    return (
        df.groupBy(*group_cols)
        .agg(F.to_json(F.collect_list(F.struct(*struct_fields))).alias(obj_col))
    )


# ---------------------------------------------------------------------------
# union_relations  (replaces custom_union_relations.sql)
# ---------------------------------------------------------------------------
def union_relations(
    dfs: Sequence[DataFrame],
    allow_missing: bool = True,
    source_column_name: Optional[str] = "_dbt_source_relation",
) -> DataFrame:
    """Union multiple DataFrames, aligning schemas by column name.

    Mirrors the cross-database ``custom_union_relations`` macro.

    Parameters
    ----------
    dfs:
        Sequence of DataFrames to union.
    allow_missing:
        When ``True`` (default), columns present in one DataFrame but
        absent in another are filled with ``NULL``.  When ``False``, all
        DataFrames must share the same schema.
    source_column_name:
        If not ``None``, a literal string column is added to each
        DataFrame identifying its ordinal position (``"relation_0"``,
        ``"relation_1"``, …).  Set to ``None`` to omit.
    """
    if not dfs:
        raise ValueError("union_relations requires at least one DataFrame")

    if len(dfs) == 1:
        result = dfs[0]
        if source_column_name:
            result = result.withColumn(source_column_name, F.lit("relation_0"))
        return result

    if allow_missing:
        all_cols: dict[str, None] = {}
        for df in dfs:
            for c in df.columns:
                all_cols.setdefault(c, None)
        ordered_cols = list(all_cols.keys())

        aligned: list[DataFrame] = []
        for idx, df in enumerate(dfs):
            existing = set(df.columns)
            selected = [
                F.col(c) if c in existing else F.lit(None).alias(c)
                for c in ordered_cols
            ]
            tmp = df.select(*selected)
            if source_column_name:
                tmp = tmp.withColumn(
                    source_column_name, F.lit(f"relation_{idx}")
                )
            aligned.append(tmp)
    else:
        aligned = []
        for idx, df in enumerate(dfs):
            tmp = df
            if source_column_name:
                tmp = tmp.withColumn(
                    source_column_name, F.lit(f"relation_{idx}")
                )
            aligned.append(tmp)

    return reduce(DataFrame.unionByName, aligned)
