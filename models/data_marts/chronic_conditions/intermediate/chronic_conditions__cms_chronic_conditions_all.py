import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:

    chronic_conditions = spark.table("chronic_conditions__cms_chronic_conditions_hierarchy")

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

    patient_procedures = (
        spark.table("cms_chronic_conditions__stg_core__procedure")
        .select(
            F.col("person_id"),
            F.col("claim_id"),
            F.col("procedure_date").alias("start_date"),
            F.col("normalized_code_type").alias("code_type"),
            F.regexp_replace(F.col("normalized_code"), r"\.", "").alias("code"),
            F.col("data_source"),
        )
    )

    inclusions_diagnosis = (
        patient_conditions.alias("pc")
        .join(
            chronic_conditions.alias("cc"),
            F.col("pc.code") == F.col("cc.code"),
        )
        .filter(F.col("cc.inclusion_type") == "Include")
        .filter(F.col("cc.code_system") == "ICD-10-CM")
        .filter(F.col("cc.additional_logic") == "None")
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
        .filter(F.col("cc2.additional_logic") == "None")
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

    inclusions_procedure = (
        patient_procedures.alias("pp")
        .join(
            chronic_conditions.alias("cc3"),
            F.col("pp.code") == F.col("cc3.code"),
        )
        .filter(F.col("cc3.inclusion_type") == "Include")
        .filter(F.col("cc3.code_system").isin("ICD-10-PCS", "HCPCS"))
        .filter(F.col("cc3.additional_logic") == "None")
        .select(
            F.col("pp.person_id"),
            F.col("pp.claim_id"),
            F.col("pp.start_date"),
            F.col("pp.data_source"),
            F.col("cc3.chronic_condition_type"),
            F.col("cc3.condition_category"),
            F.col("cc3.condition"),
        )
    )

    exclusions_diagnosis = (
        patient_conditions.alias("pc2")
        .join(
            chronic_conditions.alias("cc4"),
            F.col("pc2.code") == F.col("cc4.code"),
        )
        .filter(F.col("cc4.inclusion_type") == "Exclude")
        .filter(F.col("cc4.code_system") == "ICD-10-CM")
        .select(
            F.col("pc2.claim_id"),
            F.col("cc4.condition"),
        )
        .distinct()
    )

    inclusions_unioned = (
        inclusions_diagnosis
        .unionByName(inclusions_procedure)
        .unionByName(inclusions_ms_drg)
        .distinct()
    )

    result = (
        inclusions_unioned.alias("iu")
        .join(
            exclusions_diagnosis.alias("ed"),
            (F.col("iu.claim_id") == F.col("ed.claim_id"))
            & (F.col("iu.condition") == F.col("ed.condition")),
            "left_outer",
        )
        .filter(F.col("ed.claim_id").isNull())
        .select(
            F.col("iu.person_id").cast("string").alias("person_id"),
            F.col("iu.claim_id").cast("string").alias("claim_id"),
            F.col("iu.start_date").cast("date").alias("start_date"),
            F.col("iu.chronic_condition_type").cast("string").alias("chronic_condition_type"),
            F.col("iu.condition_category").cast("string").alias("condition_category"),
            F.col("iu.condition").cast("string").alias("condition"),
            F.col("iu.data_source").cast("string").alias("data_source"),
            F.current_timestamp().alias("tuva_last_run"),
        )
        .distinct()
    )

    return result
