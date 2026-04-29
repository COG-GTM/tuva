import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    """Staging model for core medical claims.

    The original SQL has branching logic based on dbt vars.
    In PySpark we assume core__medical_claim exists.
    """
    mc = spark.table("core__medical_claim")

    return mc.select(
        F.col("medical_claim_id"),
        F.col("person_id"),
        F.col("claim_id"),
        F.col("claim_line_number"),
        F.col("claim_type"),
        F.col("encounter_group"),
        F.col("claim_start_date"),
        F.col("claim_end_date"),
        F.col("claim_line_start_date"),
        F.col("payer"),
        F.col("plan"),
        F.col("billing_npi"),
        F.col("billing_name"),
        F.col("rendering_npi"),
        F.col("rendering_name"),
        F.col("admission_date"),
        F.col("discharge_date"),
        F.col("bill_type_code"),
        F.col("revenue_center_code"),
        F.col("revenue_center_description"),
        F.col("place_of_service_code"),
        F.col("place_of_service_description"),
        F.col("hcpcs_code"),
        F.col("hcpcs_modifier_1"),
        F.col("hcpcs_modifier_2"),
        F.col("hcpcs_modifier_3"),
        F.col("hcpcs_modifier_4"),
        F.col("hcpcs_modifier_5"),
        F.col("paid_date"),
        F.col("paid_amount"),
        F.col("in_network_flag"),
        F.col("data_source"),
    )
