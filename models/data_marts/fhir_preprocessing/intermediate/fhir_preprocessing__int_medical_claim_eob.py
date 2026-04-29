import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    eligibility = spark.table("fhir_preprocessing__stg_core__eligibility").select(
        "person_id", "eligibility_id", "payer", "plan",
        "enrollment_start_date", "enrollment_end_date",
    )
    claim_diagnosis = spark.table("fhir_preprocessing__int_medical_claim_diagnosis")
    claim_procedure = spark.table("fhir_preprocessing__int_medical_claim_procedure")
    claim_supporting_info = spark.table("fhir_preprocessing__int_medical_claim_supporting_info")
    claim_item = spark.table("fhir_preprocessing__int_medical_claim_item")
    claim_total = spark.table("fhir_preprocessing__int_medical_claim_total")
    medical_claim = spark.table("fhir_preprocessing__stg_core__medical_claim")

    mc_header = medical_claim.filter(F.col("claim_line_number") == 1)

    add_coverage = (
        mc_header.join(
            eligibility,
            (mc_header["person_id"] == eligibility["person_id"])
            & (mc_header["payer"] == eligibility["payer"])
            & (mc_header["plan"] == eligibility["plan"])
            & (mc_header["claim_start_date"] >= eligibility["enrollment_start_date"])
            & (mc_header["claim_start_date"] <= eligibility["enrollment_end_date"]),
            how="left",
        )
        .select(
            mc_header["person_id"],
            mc_header["medical_claim_id"],
            mc_header["claim_id"],
            mc_header["claim_type"],
            mc_header["encounter_group"],
            mc_header["claim_start_date"],
            mc_header["claim_end_date"],
            mc_header["paid_date"],
            mc_header["payer"],
            mc_header["billing_npi"],
            mc_header["billing_name"],
            mc_header["rendering_npi"],
            mc_header["rendering_name"],
            eligibility["eligibility_id"],
            mc_header["data_source"],
            F.row_number().over(
                Window.partitionBy(mc_header["person_id"], mc_header["medical_claim_id"])
                .orderBy(
                    eligibility["enrollment_start_date"].desc(),
                    eligibility["enrollment_end_date"].desc(),
                )
            ).alias("coverage_row_num"),
        )
    )

    dedupe = add_coverage.filter(F.col("coverage_row_num") == 1).select(
        "person_id", "medical_claim_id", "claim_id", "claim_type",
        "encounter_group", "claim_start_date", "claim_end_date",
        "paid_date", "payer", "billing_npi", "billing_name",
        "rendering_npi", "rendering_name", "eligibility_id", "data_source",
    )

    medical_eob = (
        dedupe.alias("mc")
        .join(claim_diagnosis.alias("cd"), F.col("mc.claim_id") == F.col("cd.claim_id"), how="left")
        .join(claim_procedure.alias("cp"), F.col("mc.claim_id") == F.col("cp.claim_id"), how="left")
        .join(claim_supporting_info.alias("cs"), F.col("mc.claim_id") == F.col("cs.claim_id"), how="left")
        .join(claim_item.alias("ci"), F.col("mc.claim_id") == F.col("ci.claim_id"), how="left")
        .join(claim_total.alias("ct"), F.col("mc.claim_id") == F.col("ct.claim_id"), how="left")
        .select(
            F.col("mc.person_id").cast("string").alias("patient_internal_id"),
            F.col("mc.medical_claim_id").cast("string").alias("resource_internal_id"),
            F.col("mc.claim_id").cast("string").alias("unique_claim_id"),
            F.col("mc.claim_type").cast("string").alias("eob_type_code"),
            F.when(F.col("mc.encounter_group") == "inpatient", F.lit("inpatient"))
             .otherwise(F.lit("outpatient"))
             .alias("eob_subtype_code"),
            F.col("mc.claim_start_date").cast("date").alias("eob_billable_period_start"),
            F.col("mc.claim_end_date").cast("date").alias("eob_billable_period_end"),
            F.coalesce(
                F.col("mc.paid_date").cast("date"),
                F.col("mc.claim_start_date").cast("date"),
            ).alias("eob_created"),
            F.col("mc.payer").cast("string").alias("organization_name"),
            F.coalesce(
                F.col("mc.billing_npi").cast("string"),
                F.col("mc.rendering_npi").cast("string"),
                F.lit("9999999999"),
            ).alias("practitioner_internal_id"),
            F.coalesce(
                F.col("mc.billing_name").cast("string"),
                F.col("mc.rendering_name").cast("string"),
                F.lit("Dummy Practitioner"),
            ).alias("practitioner_name_text"),
            F.md5(F.coalesce(F.col("mc.eligibility_id").cast("string"), F.lit(""))).cast("string").alias("coverage_internal_id"),
            F.col("cd.eob_diagnosis_list").cast("string").alias("eob_diagnosis_list"),
            F.col("cp.eob_procedure_list").cast("string").alias("eob_procedure_list"),
            F.col("cs.eob_supporting_info_list").cast("string").alias("eob_supporting_info_list"),
            F.col("ci.eob_item_list").cast("string").alias("eob_item_list"),
            F.col("ct.eob_total_list").cast("string").alias("eob_total_list"),
            F.col("mc.data_source").cast("string").alias("data_source"),
        )
    )

    return medical_eob.select(
        "patient_internal_id",
        "resource_internal_id",
        "unique_claim_id",
        "eob_type_code",
        "eob_subtype_code",
        "eob_billable_period_start",
        "eob_billable_period_end",
        "eob_created",
        "organization_name",
        "practitioner_internal_id",
        "practitioner_name_text",
        "coverage_internal_id",
        "eob_diagnosis_list",
        "eob_procedure_list",
        "eob_supporting_info_list",
        "eob_item_list",
        "eob_total_list",
        "data_source",
    )
