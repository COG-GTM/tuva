import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    """Staging model for core eligibility.

    The original SQL has branching logic based on dbt vars
    (clinical_enabled / claims_enabled). In PySpark we assume
    core__eligibility exists and select from it directly.
    """
    eligibility = spark.table("core__eligibility")

    return eligibility.select(
        F.col("eligibility_id"),
        F.col("person_id"),
        F.col("member_id"),
        F.col("payer_type"),
        F.col("payer"),
        F.col("plan"),
        F.col("enrollment_start_date"),
        F.col("enrollment_end_date"),
        F.col("subscriber_relation"),
        F.col("subscriber_id"),
        F.col("data_source"),
    )
