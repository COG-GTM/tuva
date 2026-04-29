import os
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    """
    Unions lab order-level and component-level results from core__lab_result,
    deduplicates, and returns a standardised lab result set.
    """
    lab_result = spark.table("core__lab_result")

    lab_order = (
        lab_result
        .filter(F.coalesce(F.col("normalized_component_code"), F.col("source_component_code")).isNull())
        .select(
            F.col("person_id"),
            F.col("result"),
            F.col("result_datetime").alias("result_date"),
            F.col("collection_datetime").alias("collection_date"),
            F.lower(F.coalesce(F.col("normalized_order_type"), F.col("source_order_type"))).alias("code_type"),
            F.coalesce(F.col("normalized_order_code"), F.col("source_order_code")).alias("code"),
        )
    )

    lab_component = (
        lab_result
        .filter(F.coalesce(F.col("normalized_component_code"), F.col("source_component_code")).isNotNull())
        .select(
            F.col("person_id"),
            F.col("result"),
            F.col("result_datetime").alias("result_date"),
            F.col("collection_datetime").alias("collection_date"),
            F.lower(F.coalesce(F.col("normalized_component_type"), F.col("source_component_type"))).alias("code_type"),
            F.coalesce(F.col("normalized_component_code"), F.col("source_component_code")).alias("code"),
        )
    )

    unioned = lab_order.unionByName(lab_component)

    result = unioned.select(
        "person_id",
        "result",
        "result_date",
        "collection_date",
        "code_type",
        "code",
    ).distinct()

    return result
