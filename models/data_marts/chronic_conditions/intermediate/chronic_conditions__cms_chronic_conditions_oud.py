import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:

    condition_filter = "Opioid Use Disorder (OUD)"

    naltrexone_ndcs = [
        '00056001122', '00056001130', '00056001170', '00056007950', '00056008050',
        '00185003901', '00185003930', '00406009201', '00406009203', '00406117001',
        '00406117003', '00555090201', '00555090202', '00904703604', '16729008101',
        '16729008110', '42291063230', '43063059115', '47335032683', '47335032688',
        '50090286600', '50436010501', '51224020630', '51224020650', '51285027501',
        '51285027502', '52152010502', '52152010504', '52152010530', '54868557400',
        '63459030042', '63629104601', '63629104701', '65694010003', '65694010010',
        '65757030001', '65757030202', '68084029111', '68084029121', '68094085362',
        '68115068030',
    ]

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

    patient_medications = (
        spark.table("cms_chronic_conditions__stg_core__pharmacy_claim")
        .select(
            F.col("person_id"),
            F.col("claim_id"),
            F.col("paid_date").alias("start_date"),
            F.regexp_replace(F.col("ndc_code"), r"\.", "").alias("code"),
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

    inclusions_procedure = (
        patient_procedures.alias("pp")
        .join(
            chronic_conditions.alias("cc2"),
            F.col("pp.code") == F.col("cc2.code"),
        )
        .filter(F.col("cc2.inclusion_type") == "Include")
        .filter(F.col("cc2.code_system").isin("ICD-10-PCS", "HCPCS"))
        .select(
            F.col("pp.person_id"),
            F.col("pp.claim_id"),
            F.col("pp.start_date"),
            F.col("pp.data_source"),
            F.col("cc2.chronic_condition_type"),
            F.col("cc2.condition_category"),
            F.col("cc2.condition"),
        )
    )

    # Exclusion logic: Naltrexone NDCs are excluded if there is evidence
    # of an alcohol or other drug use disorder where opioid DX is not present.
    # This CTE excludes medication encounters with the Naltrexone exception codes.
    inclusions_medication = (
        patient_medications.alias("pm")
        .join(
            chronic_conditions.alias("cc3"),
            F.col("pm.code") == F.col("cc3.code"),
        )
        .filter(F.col("cc3.inclusion_type") == "Include")
        .filter(F.col("cc3.code_system") == "NDC")
        .filter(~F.col("cc3.code").isin(naltrexone_ndcs))
        .select(
            F.col("pm.person_id"),
            F.col("pm.claim_id"),
            F.col("pm.start_date"),
            F.col("pm.data_source"),
            F.col("cc3.chronic_condition_type"),
            F.col("cc3.condition_category"),
            F.col("cc3.condition"),
        )
    )

    # Patients with evidence of Alcohol Use Disorders or Drug Use Disorders
    exclusions_other_chronic_conditions = (
        spark.table("chronic_conditions__cms_chronic_conditions_all")
        .filter(F.col("condition").isin("Alcohol Use Disorders", "Drug Use Disorders"))
        .select("person_id")
        .distinct()
    )

    # Exclusion list: patients with Naltrexone medication encounters having
    # Alcohol Use Disorder or Drug Use Disorder and missing OUD diagnosis codes
    exclusions_medication = (
        patient_medications.alias("pm2")
        .join(
            chronic_conditions.alias("cc4"),
            F.col("pm2.code") == F.col("cc4.code"),
        )
        .join(
            exclusions_other_chronic_conditions.alias("eoc"),
            F.col("pm2.person_id") == F.col("eoc.person_id"),
        )
        .join(
            inclusions_diagnosis.alias("id"),
            F.col("pm2.person_id") == F.col("id.person_id"),
            "left_outer",
        )
        .filter(F.col("cc4.inclusion_type") == "Include")
        .filter(F.col("cc4.code_system") == "NDC")
        .filter(F.col("cc4.code").isin(naltrexone_ndcs))
        .filter(F.col("id.person_id").isNull())
        .select(F.col("pm2.person_id"))
        .distinct()
    )

    inclusions_unioned = (
        inclusions_diagnosis
        .unionByName(inclusions_procedure)
        .unionByName(inclusions_medication)
        .distinct()
    )

    result = (
        inclusions_unioned.alias("iu")
        .join(
            exclusions_medication.alias("em"),
            F.col("iu.person_id") == F.col("em.person_id"),
            "left_outer",
        )
        .filter(F.col("em.person_id").isNull())
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
