import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark):
    """Normalize: combine custom_mapped codes with all unmapped codes.

    Produces a union-distinct of custom_mapped rows and unmapped rows
    that are not already represented in custom_mapped.
    """
    custom_mapped = spark.table("custom_mapped")
    all_unmapped = spark.table("normalize__all_unmapped")

    cm_selected = custom_mapped.select(
        "source_code_type",
        "source_code",
        "source_description",
        "item_count",
        "domains",
        "data_sources",
        "normalized_code_type",
        "normalized_code",
        "normalized_description",
        "not_mapped",
        "added_by",
        "added_date",
        "reviewed_by",
        "reviewed_date",
        "notes",
    )

    un_not_in_cm = (
        all_unmapped.alias("un")
        .join(
            custom_mapped.alias("cm"),
            on=(
                F.lower(F.col("un.source_code_type")).eqNullSafe(
                    F.lower(F.col("cm.source_code_type"))
                )
                & F.col("un.source_code").eqNullSafe(F.col("cm.source_code"))
                & F.col("un.source_description").eqNullSafe(
                    F.col("cm.source_description")
                )
            ),
            how="left",
        )
        .filter(
            F.col("cm.source_code_type").isNull()
            & F.col("cm.source_code").isNull()
            & F.col("cm.source_description").isNull()
        )
        .select(
            F.col("un.source_code_type"),
            F.col("un.source_code"),
            F.col("un.source_description"),
            F.col("un.item_count"),
            F.col("un.domains"),
            F.col("un.data_sources"),
            F.col("un.normalized_code_type"),
            F.col("un.normalized_code"),
            F.col("un.normalized_description"),
            F.col("un.not_mapped"),
            F.col("un.added_by"),
            F.col("un.added_date"),
            F.col("un.reviewed_by"),
            F.col("un.reviewed_date"),
            F.col("un.notes"),
        )
    )

    result = cm_selected.unionByName(un_not_in_cm).distinct()

    return result
