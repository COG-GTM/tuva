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
    claim_supporting_info = spark.table("fhir_preprocessing__int_pharmacy_claim_supporting_info")
    claim_item = spark.table("fhir_preprocessing__int_pharmacy_claim_item")
    claim_total = spark.table("fhir_preprocessing__int_pharmacy_claim_total")
    pharmacy_claim = spark.table("fhir_preprocessing__stg_core__pharmacy_claim")

    pc_header = pharmacy_claim.filter(F.col("claim_line_number") == 1)

    add_coverage = (
        pc_header.alias("pc")
        .join(
            eligibility.alias("el"),
            (F.col("pc.person_id") == F.col("el.person_id"))
            & (F.col("pc.payer") == F.col("el.payer"))
            & (F.col("pc.plan") == F.col("el.plan"))
            & (F.col("pc.paid_date") >= F.col("el.enrollment_start_date"))
            & (F.col("pc.paid_date") <= F.col("el.enrollment_end_date")),
            how="left",
        )
        .select(
            F.col("pc.person_id"),
            F.col("pc.pharmacy_claim_id"),
            F.col("pc.claim_id"),
            F.col("pc.paid_date"),
            F.col("pc.payer"),
            F.col("pc.dispensing_provider_id"),
            F.col("pc.dispensing_provider_name"),
            F.col("el.eligibility_id"),
            F.col("pc.data_source"),
            F.row_number().over(
                Window.partitionBy(F.col("pc.person_id"), F.col("pc.pharmacy_claim_id"))
                .orderBy(
                    F.col("el.enrollment_start_date").desc(),
                    F.col("el.enrollment_end_date").desc(),
                )
            ).alias("coverage_row_num"),
        )
    )

    dedupe = add_coverage.filter(F.col("coverage_row_num") == 1).select(
        "person_id", "pharmacy_claim_id", "claim_id", "paid_date",
        "payer", "dispensing_provider_id", "dispensing_provider_name",
        "eligibility_id", "data_source",
    )

    pharmacy_eob = (
        dedupe.alias("pc")
        .join(claim_supporting_info.alias("cs"), F.col("pc.claim_id") == F.col("cs.claim_id"), how="left")
        .join(claim_item.alias("ci"), F.col("pc.claim_id") == F.col("ci.claim_id"), how="left")
        .join(claim_total.alias("ct"), F.col("pc.claim_id") == F.col("ct.claim_id"), how="left")
        .select(
            F.col("pc.person_id").cast("string").alias("patient_internal_id"),
            F.col("pc.pharmacy_claim_id").cast("string").alias("resource_internal_id"),
            F.col("pc.claim_id").cast("string").alias("unique_claim_id"),
            F.lit("pharmacy").alias("eob_type_code"),
            F.lit(None).cast("string").alias("eob_subtype_code"),
            F.lit(None).cast("date").alias("eob_billable_period_start"),
            F.lit(None).cast("date").alias("eob_billable_period_end"),
            F.col("pc.paid_date").cast("date").alias("eob_created"),
            F.col("pc.payer").cast("string").alias("organization_name"),
            F.coalesce(
                F.col("pc.dispensing_provider_id").cast("string"),
                F.lit("9999999999"),
            ).alias("practitioner_internal_id"),
            F.coalesce(
                F.col("pc.dispensing_provider_name").cast("string"),
                F.lit("Dummy Practitioner"),
            ).alias("practitioner_name_text"),
            F.md5(F.coalesce(F.col("pc.eligibility_id").cast("string"), F.lit(""))).cast("string").alias("coverage_internal_id"),
            F.lit(None).cast("string").alias("eob_diagnosis_list"),
            F.lit(None).cast("string").alias("eob_procedure_list"),
            F.col("cs.eob_supporting_info_list").cast("string").alias("eob_supporting_info_list"),
            F.col("ci.eob_item_list").cast("string").alias("eob_item_list"),
            F.col("ct.eob_total_list").cast("string").alias("eob_total_list"),
            F.col("pc.data_source").cast("string").alias("data_source"),
        )
    )

    return pharmacy_eob.select(
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
