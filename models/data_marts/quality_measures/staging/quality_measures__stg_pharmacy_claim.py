import os
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    """
    Reads from core__pharmacy_claim. When claims data is enabled, selects
    the full pharmacy claim set. In PySpark we always read the table directly.
    """
    pharmacy_claim = spark.table("core__pharmacy_claim")

    result = pharmacy_claim.select(
        F.col("person_id"),
        F.col("dispensing_date"),
        F.col("ndc_code"),
        F.col("days_supply"),
        F.col("paid_date"),
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
