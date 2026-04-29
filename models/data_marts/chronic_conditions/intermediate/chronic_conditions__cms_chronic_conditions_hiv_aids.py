import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:

    condition_filter = "Human Immunodeficiency Virus and/or Acquired Immunodeficiency Syndrome (HIV/AIDS)"

    chronic_conditions = (
        spark.table("chronic_conditions__cms_chronic_conditions_hierarchy")
        .filter(F.col("condition") == condition_filter)
    )

    patient_conditions = (
        spark.table("cms_chronic_conditions__stg_core__condition")
        .select(
            F.col("person_id"),
            F.col("claim_id"),
            F.col("recorded_date").alias("start_date"),
            F.col("normalized_code_type").alias("code_type"),
            F.regexp_replace(F.col("normalized_code"), r"\.", "").alias("code"),
            F.col("data_source"),
        )
    )

    patient_ms_drgs = (
        spark.table("cms_chronic_conditions__stg_core__medical_claim")
        .filter(F.col("drg_code_type") == "ms-drg")
        .select(
            F.col("person_id"),
            F.col("claim_id"),
            F.col("claim_start_date").alias("start_date"),
            F.col("drg_code_type"),
            F.col("drg_code").alias("code"),
            F.col("data_source"),
        )
    )

    # Exception logic: exclude encounters with the screening code R75
    # Those will be evaluated separately in exception_diagnosis
    inclusions_diagnosis = (
        patient_conditions.alias("pc")
        .join(
            chronic_conditions.alias("cc"),
            F.col("pc.code") == F.col("cc.code"),
        )
        .filter(F.col("cc.inclusion_type") == "Include")
        .filter(F.col("cc.code_system") == "ICD-10-CM")
        .filter(F.col("cc.code") != "R75")
        .select(
            F.col("pc.person_id"),
            F.col("pc.claim_id"),
            F.col("pc.start_date"),
            F.col("pc.data_source"),
            F.col("cc.chronic_condition_type"),
            F.col("cc.condition_category"),
            F.col("cc.condition"),
        )
    )

    inclusions_ms_drg = (
        patient_ms_drgs.alias("pm")
        .join(
            chronic_conditions.alias("cc2"),
            F.col("pm.code") == F.col("cc2.code"),
        )
        .filter(F.col("cc2.inclusion_type") == "Include")
        .filter(F.col("cc2.code_system") == "MS-DRG")
        .select(
            F.col("pm.person_id"),
            F.col("pm.claim_id"),
            F.col("pm.start_date"),
            F.col("pm.data_source"),
            F.col("cc2.chronic_condition_type"),
            F.col("cc2.condition_category"),
            F.col("cc2.condition"),
        )
    )

    # Exception logic: R75 encounters are included only where the patient
    # has another qualifying encounter that is not R75
    exception_diagnosis = (
        patient_conditions.alias("pc2")
        .join(
            chronic_conditions.alias("cc3"),
            F.col("pc2.code") == F.col("cc3.code"),
        )
        .join(
            inclusions_diagnosis.alias("id"),
            F.col("pc2.person_id") == F.col("id.person_id"),
        )
        .filter(F.col("cc3.inclusion_type") == "Include")
        .filter(F.col("cc3.code_system") == "ICD-10-CM")
        .filter(F.col("cc3.code") == "R75")
        .select(
            F.col("pc2.person_id"),
            F.col("pc2.claim_id"),
            F.col("pc2.start_date"),
            F.col("pc2.data_source"),
            F.col("cc3.chronic_condition_type"),
            F.col("cc3.condition_category"),
            F.col("cc3.condition"),
        )
    )

    inclusions_unioned = (
        inclusions_diagnosis
        .unionByName(inclusions_ms_drg)
        .unionByName(exception_diagnosis)
        .distinct()
    )

    result = (
        inclusions_unioned
        .select(
            F.col("person_id").cast("string").alias("person_id"),
            F.col("claim_id").cast("string").alias("claim_id"),
            F.col("start_date").cast("date").alias("start_date"),
            F.col("chronic_condition_type").cast("string").alias("chronic_condition_type"),
            F.col("condition_category").cast("string").alias("condition_category"),
            F.col("condition").cast("string").alias("condition"),
            F.col("data_source").cast("string").alias("data_source"),
            F.current_timestamp().alias("tuva_last_run"),
        )
        .distinct()
    )

    return result
