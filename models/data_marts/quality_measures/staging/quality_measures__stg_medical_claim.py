import os
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    """
    Reads from core__medical_claim. When claims data is enabled, selects
    the full medical claim set. In PySpark we always read the table directly.
    """
    medical_claim = spark.table("core__medical_claim")

    result = medical_claim.select(
        F.col("person_id"),
        F.col("claim_id"),
        F.col("claim_start_date"),
        F.col("claim_end_date"),
        F.col("place_of_service_code"),
        F.col("hcpcs_code"),
        F.col("hcpcs_modifier_1"),
        F.col("hcpcs_modifier_2"),
        F.col("hcpcs_modifier_3"),
        F.col("hcpcs_modifier_4"),
        F.col("hcpcs_modifier_5"),
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
