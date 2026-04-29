import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    medical_claim = spark.table("provider_attribution__stg_core__medical_claim")
    calendar = spark.table("provider_attribution__stg_reference_data__calendar")
    member_months = spark.table("provider_attribution__stg_core__member_months")
    primary_care_hcpcs = spark.table("cms_provider_attribution__primary_care_hcpcs_codes")
    provider_class = spark.table("provider_attribution__provider_classification")
    terminology_provider = spark.table("provider_attribution__stg_terminology__provider")

    # claim_month: join medical claims to calendar to get year/month fields
    claim_month = (
        medical_claim.alias("mc")
        .join(
            calendar.alias("cal"),
            F.col("mc.claim_start_date").cast("date") == F.col("cal.full_date"),
            "left"
        )
        .select(
            F.col("mc.person_id"),
            F.col("mc.claim_id"),
            F.col("mc.claim_line_number"),
            F.col("mc.encounter_id").cast("string").alias("encounter_id"),
            F.col("mc.claim_start_date"),
            F.col("mc.claim_end_date"),
            F.col("cal.year").alias("claim_year"),
            F.col("cal.month").alias("claim_month"),
            F.col("cal.year_month_int").alias("claim_year_month_int"),
            F.col("cal.year_month_int").cast("string").alias("claim_year_month"),
            F.coalesce(
                F.when(F.col("mc.allowed_amount") != 0, F.col("mc.allowed_amount")),
                F.col("mc.paid_amount"),
                F.lit(0)
            ).alias("allowed_amount"),
            F.col("mc.rendering_npi").cast("string").alias("provider_id"),
            F.col("mc.hcpcs_code")
        )
    )

    # eligible_claims: only claims in months where the person had membership
    eligible_claims = (
        claim_month.alias("c")
        .join(
            member_months.alias("mm"),
            (F.col("c.person_id") == F.col("mm.person_id"))
            & (F.col("c.claim_year_month") == F.col("mm.year_month")),
            "inner"
        )
        .select("c.*")
    )

    # primary_care_claims: filter to primary care HCPCS codes
    primary_care_claims = (
        eligible_claims.alias("e")
        .join(
            primary_care_hcpcs.alias("pc"),
            F.col("e.hcpcs_code") == F.col("pc.hcpcs_code"),
            "inner"
        )
        .select(
            F.col("e.person_id"),
            F.col("e.claim_id"),
            F.col("e.claim_line_number"),
            F.col("e.encounter_id"),
            F.col("e.claim_start_date"),
            F.col("e.claim_end_date"),
            F.col("e.claim_year"),
            F.col("e.claim_month"),
            F.col("e.claim_year_month"),
            F.col("e.claim_year_month_int"),
            F.col("e.allowed_amount"),
            F.col("e.provider_id"),
            F.col("e.hcpcs_code")
        )
    )

    # with_bucket: add provider classification bucket and specialty
    with_bucket = (
        primary_care_claims.alias("pcc")
        .join(
            provider_class.alias("pc"),
            F.col("pcc.provider_id") == F.col("pc.provider_id"),
            "left"
        )
        .join(
            terminology_provider.alias("sp"),
            (F.col("pcc.provider_id").cast("string") == F.col("sp.npi").cast("string"))
            & (F.lower(F.trim(F.col("sp.entity_type_description"))) == "individual"),
            "inner"
        )
        .select(
            F.col("pcc.person_id"),
            F.col("pcc.claim_id"),
            F.col("pcc.claim_line_number"),
            F.col("pcc.encounter_id"),
            F.col("pcc.claim_start_date"),
            F.col("pcc.claim_end_date"),
            F.col("pcc.claim_year"),
            F.col("pcc.claim_month"),
            F.col("pcc.claim_year_month"),
            F.col("pcc.claim_year_month_int"),
            F.col("pcc.allowed_amount"),
            F.col("pcc.provider_id"),
            F.col("pcc.hcpcs_code"),
            F.coalesce(F.col("pc.provider_bucket"), F.lit("other_individual")).alias("provider_bucket"),
            F.coalesce(F.col("pc.prov_specialty"), F.col("sp.primary_specialty_description")).alias("prov_specialty")
        )
    )

    return with_bucket
