import os
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    """
    Patients in institutional special needs plans (SNP) or residing in
    long-term care. When referencing this model, patients >= 66 years of
    age should be filtered by the consuming model.
    """
    patients = spark.table("quality_measures__stg_core__patient").select(
        F.col("person_id")
    )

    medical_claim = (
        spark.table("quality_measures__stg_medical_claim")
        .select(
            F.col("person_id"),
            F.col("claim_start_date"),
            F.col("claim_end_date"),
            F.col("hcpcs_code"),
            F.col("place_of_service_code"),
        )
    )

    exclusions = (
        patients.alias("pat")
        .join(
            medical_claim.alias("mc"),
            F.col("pat.person_id") == F.col("mc.person_id"),
            "inner",
        )
        .filter(F.col("mc.place_of_service_code").isin("32", "33", "34", "54", "56"))
        .filter(
            F.datediff(F.col("mc.claim_end_date"), F.col("mc.claim_start_date")) >= 90
        )
        .select(
            F.col("pat.person_id"),
            F.coalesce(F.col("mc.claim_start_date"), F.col("mc.claim_end_date")).alias("exclusion_date"),
            F.lit("institutional or long term care").alias("exclusion_reason"),
        )
    )

    result = exclusions.select(
        F.col("person_id"),
        F.col("exclusion_date"),
        F.col("exclusion_reason"),
        F.lit("institutional_snp").alias("exclusion_type"),
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
