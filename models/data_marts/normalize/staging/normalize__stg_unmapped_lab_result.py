import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark, enable_normalize_engine=True):
    """Normalize staging: unmapped lab result codes.

    Args:
        spark: SparkSession
        enable_normalize_engine: When True, filters out rows already
            present in custom_mapped. When False, returns all unmapped rows.
    """
    lab_result = spark.table("core__lab_result")

    orders = lab_result.select(
        F.col("source_order_type").alias("source_code_type"),
        F.col("source_order_code").alias("source_code"),
        F.col("source_order_description").alias("source_description"),
        F.col("normalized_order_type").alias("normalized_code_type"),
        F.col("normalized_order_code").alias("normalized_code"),
        F.col("normalized_order_description").alias("normalized_description"),
        F.col("data_source"),
    ).distinct()

    components = lab_result.select(
        F.col("source_component_type").alias("source_code_type"),
        F.col("source_component_code").alias("source_code"),
        F.col("source_component_description").alias("source_description"),
        F.col("normalized_component_type").alias("normalized_code_type"),
        F.col("normalized_component_code").alias("normalized_code"),
        F.col("normalized_component_description").alias("normalized_description"),
        F.col("data_source"),
    ).distinct()

    unioned = orders.unionByName(components)

    if enable_normalize_engine:
        custom_mapped = spark.table("custom_mapped")

        result = (
            unioned.alias("u")
            .join(
                custom_mapped.alias("cm"),
                on=(
                    F.lower(F.col("u.source_code_type")).eqNullSafe(
                        F.lower(F.col("cm.source_code_type"))
                    )
                    & F.col("u.source_code").eqNullSafe(F.col("cm.source_code"))
                    & F.col("u.source_description").eqNullSafe(
                        F.col("cm.source_description")
                    )
                ),
                how="left",
            )
            .filter(
                F.col("u.normalized_code").isNull()
                & F.col("u.normalized_description").isNull()
                & ~(
                    F.col("u.source_code").isNull()
                    & F.col("u.source_description").isNull()
                )
                & F.col("cm.not_mapped").isNull()
            )
            .groupBy(
                F.col("u.source_code_type"),
                F.col("u.source_code"),
                F.col("u.source_description"),
                F.col("u.data_source"),
            )
            .agg(F.count(F.lit(1)).alias("item_count"))
            .select(
                "source_code_type",
                "source_code",
                "source_description",
                "item_count",
                F.lit("lab_result").alias("domain"),
                "data_source",
            )
        )
    else:
        result = (
            unioned.filter(
                F.col("normalized_code").isNull()
                & F.col("normalized_description").isNull()
                & ~(
                    F.col("source_code").isNull()
                    & F.col("source_description").isNull()
                )
            )
            .groupBy(
                "source_code_type",
                "source_code",
                "source_description",
                "data_source",
            )
            .agg(F.count(F.lit(1)).alias("item_count"))
            .select(
                "source_code_type",
                "source_code",
                "source_description",
                "item_count",
                F.lit("lab_result").alias("domain"),
                "data_source",
            )
        )

    return result
