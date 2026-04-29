import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    coverage = spark.table("fhir_preprocessing__int_coverage")
    coverage_type = spark.table("fhir_preprocessing__int_coverage_type")

    joined = (
        coverage.alias("cov")
        .join(
            coverage_type.alias("ct"),
            (F.col("cov.patient_internal_id") == F.col("ct.patient_internal_id"))
            & (F.col("cov.resource_internal_id") == F.col("ct.resource_internal_id")),
            how="left",
        )
    )

    return joined.select(
        F.col("cov.patient_internal_id").cast("string").alias("patient_internal_id"),
        F.col("cov.resource_internal_id").cast("string").alias("resource_internal_id"),
        F.col("cov.organization_name").cast("string").alias("organization_name"),
        F.col("cov.coverage_plan").cast("string").alias("coverage_plan"),
        F.col("cov.coverage_period_start").cast("date").alias("coverage_period_start"),
        F.col("cov.coverage_period_end").cast("date").alias("coverage_period_end"),
        F.col("cov.coverage_relationship").cast("string").alias("coverage_relationship"),
        F.col("cov.coverage_status").cast("string").alias("coverage_status"),
        F.col("cov.coverage_subscriber_id").cast("string").alias("coverage_subscriber_id"),
        F.col("ct.coverage_type_list").cast("string").alias("coverage_type_list"),
        F.col("cov.data_source").cast("string").alias("data_source"),
    )
