import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    """Staging model for core pharmacy claims.

    The original SQL has branching logic based on dbt vars.
    In PySpark we assume core__pharmacy_claim exists.
    """
    pc = spark.table("core__pharmacy_claim")

    return pc.select(
        F.col("pharmacy_claim_id"),
        F.col("person_id"),
        F.col("claim_id"),
        F.col("claim_line_number"),
        F.col("payer"),
        F.col("plan"),
        F.col("dispensing_provider_id"),
        F.col("dispensing_provider_name"),
        F.col("dispensing_date"),
        F.col("paid_date"),
        F.col("days_supply"),
        F.col("refills"),
        F.col("paid_amount"),
        F.col("in_network_flag"),
        F.col("ndc_code"),
        F.col("data_source"),
    )
