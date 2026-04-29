import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark, enable_normalize_engine=True):
    """Normalize staging: unmapped medication codes.

    Args:
        spark: SparkSession
        enable_normalize_engine: When True, filters out rows already
            present in custom_mapped. When False, returns all unmapped rows.
    """
    i = spark.table("core__medication")

    if enable_normalize_engine:
        custom_mapped = spark.table("custom_mapped")

        result = (
            i.alias("i")
            .join(
                custom_mapped.alias("cm"),
                on=(
                    F.lower(F.col("i.source_code_type")).eqNullSafe(
                        F.lower(F.col("cm.source_code_type"))
                    )
                    & F.col("i.source_code").eqNullSafe(F.col("cm.source_code"))
                    & F.col("i.source_description").eqNullSafe(
                        F.col("cm.source_description")
                    )
                ),
                how="left",
            )
            .filter(
                F.col("i.ndc_code").isNull()
                & F.col("i.ndc_description").isNull()
                & F.col("i.rxnorm_code").isNull()
                & F.col("i.rxnorm_description").isNull()
                & ~(
                    F.col("i.source_code").isNull()
                    & F.col("i.source_description").isNull()
                )
                & F.col("cm.not_mapped").isNull()
            )
            .groupBy(
                F.col("i.source_code_type"),
                F.col("i.source_code"),
                F.col("i.source_description"),
                F.col("i.data_source"),
            )
            .agg(F.count(F.lit(1)).alias("item_count"))
            .select(
                "source_code_type",
                "source_code",
                "source_description",
                "item_count",
                F.lit("medication").alias("domain"),
                "data_source",
            )
        )
    else:
        result = (
            i.filter(
                F.col("ndc_code").isNull()
                & F.col("ndc_description").isNull()
                & F.col("rxnorm_code").isNull()
                & F.col("rxnorm_description").isNull()
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
                F.lit("medication").alias("domain"),
                "data_source",
            )
        )

    return result
