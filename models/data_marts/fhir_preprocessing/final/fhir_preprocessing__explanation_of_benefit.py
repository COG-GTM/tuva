import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    medical_eob = spark.table("fhir_preprocessing__int_medical_claim_eob")
    pharmacy_eob = spark.table("fhir_preprocessing__int_pharmacy_claim_eob")

    unioned = union_relations([medical_eob, pharmacy_eob])

    return unioned.select(
        F.col("patient_internal_id").cast("string").alias("patient_internal_id"),
        F.col("resource_internal_id").cast("string").alias("resource_internal_id"),
        F.col("unique_claim_id").cast("string").alias("unique_claim_id"),
        F.col("eob_type_code").cast("string").alias("eob_type_code"),
        F.col("eob_subtype_code").cast("string").alias("eob_subtype_code"),
        F.col("eob_billable_period_start").cast("date").alias("eob_billable_period_start"),
        F.col("eob_billable_period_end").cast("date").alias("eob_billable_period_end"),
        F.col("eob_created").cast("date").alias("eob_created"),
        F.col("organization_name").cast("string").alias("organization_name"),
        F.col("practitioner_internal_id").cast("string").alias("practitioner_internal_id"),
        F.col("practitioner_name_text").cast("string").alias("practitioner_name_text"),
        F.col("coverage_internal_id").cast("string").alias("coverage_internal_id"),
        F.col("eob_diagnosis_list").cast("string").alias("eob_diagnosis_list"),
        F.col("eob_procedure_list").cast("string").alias("eob_procedure_list"),
        F.col("eob_supporting_info_list").cast("string").alias("eob_supporting_info_list"),
        F.col("eob_item_list").cast("string").alias("eob_item_list"),
        F.col("eob_total_list").cast("string").alias("eob_total_list"),
        F.col("data_source").cast("string").alias("data_source"),
    )
