import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    """Staging model for core lab results.

    The original SQL has branching logic based on dbt vars and splits
    results into order-level vs component-level rows, then unions them.
    In PySpark we assume core__lab_result exists.
    """
    lab = spark.table("core__lab_result")

    lab_order = lab.filter(
        F.coalesce(F.col("normalized_component_code"), F.col("source_component_code")).isNull()
    ).select(
        F.col("lab_result_id"),
        F.col("person_id"),
        F.col("encounter_id"),
        F.col("status"),
        F.lower(F.coalesce(F.col("normalized_order_type"), F.col("source_order_type"))).alias("code_type"),
        F.coalesce(F.col("normalized_order_code"), F.col("source_order_code")).alias("code"),
        F.coalesce(F.col("normalized_order_description"), F.col("source_order_description")).alias("description"),
        F.col("result_datetime"),
        F.col("result"),
        F.coalesce(F.col("normalized_units"), F.col("source_units")).alias("units"),
        F.col("data_source"),
    )

    lab_component = lab.filter(
        F.coalesce(F.col("normalized_component_code"), F.col("source_component_code")).isNotNull()
    ).select(
        F.col("lab_result_id"),
        F.col("person_id"),
        F.col("encounter_id"),
        F.col("status"),
        F.lower(F.coalesce(F.col("normalized_component_type"), F.col("source_component_type"))).alias("code_type"),
        F.coalesce(F.col("normalized_component_code"), F.col("source_component_code")).alias("code"),
        F.coalesce(F.col("normalized_component_description"), F.col("source_component_description")).alias("description"),
        F.col("result_datetime"),
        F.col("result"),
        F.coalesce(F.col("normalized_units"), F.col("source_units")).alias("units"),
        F.col("data_source"),
    )

    return lab_order.unionByName(lab_component).distinct()
