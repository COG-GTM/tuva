import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    primary_care_claims = spark.table("provider_attribution__int_primary_care_claims")
    calendar = spark.table("provider_attribution__stg_reference_data__calendar")
    medical_claim = spark.table("provider_attribution__stg_core__medical_claim")
    member_months = spark.table("provider_attribution__stg_core__member_months")
    terminology_provider = spark.table("provider_attribution__stg_terminology__provider")
    provider_class = spark.table("provider_attribution__provider_classification")

    # claim_bounds: get the max claim_end_date from primary care claims
    claim_bounds = primary_care_claims.agg(
        F.max("claim_end_date").alias("max_claim_end_date")
    )

    # params: determine as_of_date (data-driven, no override in PySpark)
    params = claim_bounds.select(
        F.when(
            F.col("max_claim_end_date").isNotNull()
            & (F.col("max_claim_end_date") <= F.current_date()),
            F.col("max_claim_end_date")
        ).otherwise(F.current_date()).alias("as_of_date")
    )

    as_of_date_val = params.collect()[0]["as_of_date"]

    # months_12: last 12 calendar months ending at as_of_date
    months_12 = (
        calendar
        .where(
            (F.col("full_date") >= F.date_add(F.lit(as_of_date_val).cast("date"), -330))
            & (F.col("full_date") >= F.add_months(F.lit(as_of_date_val).cast("date"), -11))
            & (F.col("full_date") <= F.lit(as_of_date_val).cast("date"))
        )
        .select(
            F.col("year_month_int"),
            F.col("first_day_of_month"),
            F.col("last_day_of_month")
        )
        .distinct()
    )

    # months_24: last 24 calendar months ending at as_of_date
    months_24 = (
        calendar
        .where(
            (F.col("full_date") >= F.add_months(F.lit(as_of_date_val).cast("date"), -23))
            & (F.col("full_date") <= F.lit(as_of_date_val).cast("date"))
        )
        .select(
            F.col("year_month_int"),
            F.col("first_day_of_month"),
            F.col("last_day_of_month")
        )
        .distinct()
    )

    # claims_12: primary care claims within the 12-month window
    claims_12 = (
        primary_care_claims.alias("c")
        .join(months_12.alias("m"), F.col("c.claim_year_month_int") == F.col("m.year_month_int"), "inner")
        .where(F.col("c.claim_end_date") <= F.lit(as_of_date_val).cast("date"))
        .select(
            F.col("c.person_id"), F.col("c.provider_id"), F.col("c.provider_bucket"),
            F.col("c.prov_specialty"), F.col("c.encounter_id"), F.col("c.claim_id"),
            F.col("c.claim_year_month"), F.col("c.claim_year_month_int"),
            F.col("c.claim_end_date"), F.col("c.allowed_amount")
        )
    )

    # claims_24: primary care claims within the 24-month window
    claims_24 = (
        primary_care_claims.alias("c")
        .join(months_24.alias("m"), F.col("c.claim_year_month_int") == F.col("m.year_month_int"), "inner")
        .where(F.col("c.claim_end_date") <= F.lit(as_of_date_val).cast("date"))
        .select(
            F.col("c.person_id"), F.col("c.provider_id"), F.col("c.provider_bucket"),
            F.col("c.prov_specialty"), F.col("c.encounter_id"), F.col("c.claim_id"),
            F.col("c.claim_year_month"), F.col("c.claim_year_month_int"),
            F.col("c.claim_end_date"), F.col("c.allowed_amount")
        )
    )

    # all_claim_month: all medical claims with calendar info
    all_claim_month = (
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
            F.col("cal.year_month_int").alias("claim_year_month_int"),
            F.col("cal.year_month_int").cast("string").alias("claim_year_month"),
            F.coalesce(
                F.when(F.col("mc.allowed_amount") != 0, F.col("mc.allowed_amount")),
                F.col("mc.paid_amount"),
                F.lit(0)
            ).alias("allowed_amount"),
            F.col("mc.rendering_npi").cast("string").alias("provider_id")
        )
    )

    # eligible_all_claims: filter to claims in eligible member months
    eligible_all_claims = (
        all_claim_month.alias("ac")
        .join(
            member_months.alias("mm"),
            (F.col("ac.person_id") == F.col("mm.person_id"))
            & (F.col("ac.claim_year_month") == F.col("mm.year_month")),
            "inner"
        )
        .select("ac.*")
    )

    # all_rendering_claims: join with provider terminology and classification
    all_rendering_claims = (
        eligible_all_claims.alias("e")
        .join(
            terminology_provider.alias("sp"),
            (F.col("e.provider_id").cast("string") == F.col("sp.npi").cast("string"))
            & (F.lower(F.trim(F.col("sp.entity_type_description"))) == "individual"),
            "inner"
        )
        .join(
            provider_class.alias("pc"),
            F.col("e.provider_id") == F.col("pc.provider_id"),
            "left"
        )
        .select(
            F.col("e.person_id"),
            F.col("e.provider_id"),
            F.col("e.encounter_id"),
            F.col("e.claim_id"),
            F.col("e.claim_year_month"),
            F.col("e.claim_year_month_int"),
            F.col("e.claim_end_date"),
            F.col("e.allowed_amount"),
            F.coalesce(F.col("pc.provider_bucket"), F.lit("other_individual")).alias("provider_bucket"),
            F.coalesce(F.col("pc.prov_specialty"), F.col("sp.primary_specialty_description")).alias("prov_specialty")
        )
    )

    # step1: PCP/NPP rendering primary-care HCPCS inside the most recent 12 months
    step1 = (
        claims_12
        .where(
            F.col("provider_bucket").isin("pcp", "npp")
            & F.col("provider_id").isNotNull()
        )
        .groupBy(
            "person_id", "provider_id",
            F.coalesce(F.col("provider_bucket"), F.lit("unknown")).alias("provider_bucket"),
            "prov_specialty"
        )
        .agg(
            F.lit(1).alias("step"),
            F.sum("allowed_amount").alias("allowed_amount"),
            F.countDistinct("encounter_id").alias("visits")
        )
    )

    step1_benes = step1.select("person_id").distinct()

    # step2: Specialist rendering primary-care HCPCS (12 months), unassigned in step1
    step2 = (
        claims_12.alias("c")
        .join(step1_benes.alias("s1"), F.col("c.person_id") == F.col("s1.person_id"), "left")
        .where(
            F.col("s1.person_id").isNull()
            & (F.col("c.provider_bucket") == "specialist")
            & F.col("c.provider_id").isNotNull()
        )
        .groupBy(
            F.col("c.person_id"),
            F.col("c.provider_id"),
            F.coalesce(F.col("c.provider_bucket"), F.lit("unknown")).alias("provider_bucket"),
            F.col("c.prov_specialty")
        )
        .agg(
            F.lit(2).alias("step"),
            F.sum("c.allowed_amount").alias("allowed_amount"),
            F.countDistinct("c.encounter_id").alias("visits")
        )
    )

    step2_benes = step2.select("person_id").distinct()

    # step3: PCP/NPP rendering primary-care HCPCS across the 24-month window, unassigned in step1/2
    step3 = (
        claims_24.alias("c")
        .join(step1_benes.alias("s1"), F.col("c.person_id") == F.col("s1.person_id"), "left")
        .join(step2_benes.alias("s2"), F.col("c.person_id") == F.col("s2.person_id"), "left")
        .where(
            F.col("s1.person_id").isNull()
            & F.col("s2.person_id").isNull()
            & F.col("c.provider_bucket").isin("pcp", "npp")
            & F.col("c.provider_id").isNotNull()
        )
        .groupBy(
            F.col("c.person_id"),
            F.col("c.provider_id"),
            F.coalesce(F.col("c.provider_bucket"), F.lit("unknown")).alias("provider_bucket"),
            F.col("c.prov_specialty")
        )
        .agg(
            F.lit(3).alias("step"),
            F.sum("c.allowed_amount").alias("allowed_amount"),
            F.countDistinct("c.encounter_id").alias("visits")
        )
    )

    step3_benes = step3.select("person_id").distinct()

    # step4: Primary-care HCPCS fallback across 24 months regardless of classification
    step4 = (
        claims_24.alias("c")
        .join(step1_benes.alias("s1"), F.col("c.person_id") == F.col("s1.person_id"), "left")
        .join(step2_benes.alias("s2"), F.col("c.person_id") == F.col("s2.person_id"), "left")
        .join(step3_benes.alias("s3"), F.col("c.person_id") == F.col("s3.person_id"), "left")
        .where(
            F.col("s1.person_id").isNull()
            & F.col("s2.person_id").isNull()
            & F.col("s3.person_id").isNull()
            & F.col("c.provider_id").isNotNull()
        )
        .groupBy(
            F.col("c.person_id"),
            F.col("c.provider_id"),
            F.coalesce(F.col("c.provider_bucket"), F.lit("unknown")).alias("provider_bucket"),
            F.col("c.prov_specialty")
        )
        .agg(
            F.lit(4).alias("step"),
            F.sum("c.allowed_amount").alias("allowed_amount"),
            F.countDistinct("c.encounter_id").alias("visits")
        )
    )

    step4_benes = step4.select("person_id").distinct()

    # assigned_pairs: all person_id/provider_id pairs assigned in steps 1-4
    assigned_pairs = (
        step1.select("person_id", "provider_id")
        .union(step2.select("person_id", "provider_id"))
        .union(step3.select("person_id", "provider_id"))
        .union(step4.select("person_id", "provider_id"))
        .distinct()
    )

    # step5: Any rendering NPI across 24 months for unassigned beneficiaries
    step5 = (
        all_rendering_claims.alias("arc")
        .join(months_24.alias("m"), F.col("arc.claim_year_month_int") == F.col("m.year_month_int"), "inner")
        .join(step1_benes.alias("s1"), F.col("arc.person_id") == F.col("s1.person_id"), "left")
        .join(step2_benes.alias("s2"), F.col("arc.person_id") == F.col("s2.person_id"), "left")
        .join(step3_benes.alias("s3"), F.col("arc.person_id") == F.col("s3.person_id"), "left")
        .join(step4_benes.alias("s4"), F.col("arc.person_id") == F.col("s4.person_id"), "left")
        .join(
            assigned_pairs.alias("assigned"),
            (F.col("arc.person_id") == F.col("assigned.person_id"))
            & (F.col("arc.provider_id") == F.col("assigned.provider_id")),
            "left"
        )
        .where(
            F.col("arc.provider_id").isNotNull()
            & (F.col("arc.claim_end_date") <= F.lit(as_of_date_val).cast("date"))
            & F.col("s1.person_id").isNull()
            & F.col("s2.person_id").isNull()
            & F.col("s3.person_id").isNull()
            & F.col("s4.person_id").isNull()
            & F.col("assigned.provider_id").isNull()
        )
        .groupBy(
            F.col("arc.person_id"),
            F.col("arc.provider_id"),
            F.coalesce(F.col("arc.provider_bucket"), F.lit("unknown")).alias("provider_bucket"),
            F.col("arc.prov_specialty")
        )
        .agg(
            F.lit(5).alias("step"),
            F.sum("arc.allowed_amount").alias("allowed_amount"),
            F.countDistinct("arc.encounter_id").alias("visits")
        )
    )

    # all_steps: union all steps
    all_steps = (
        step1.unionByName(step2)
        .unionByName(step3)
        .unionByName(step4)
        .unionByName(step5)
    )

    # Final select with step_description
    result = all_steps.select(
        "person_id",
        "provider_id",
        "provider_bucket",
        "prov_specialty",
        "step",
        F.when(F.col("step") == 1, "12-month PCP/NPP primary-care HCPCS")
        .when(F.col("step") == 2, "12-month specialist primary-care HCPCS")
        .when(F.col("step") == 3, "24-month PCP/NPP primary-care HCPCS")
        .when(F.col("step") == 4, "24-month primary-care HCPCS (any classification)")
        .when(F.col("step") == 5, "24-month any rendering NPI")
        .otherwise("Unknown").alias("step_description"),
        "allowed_amount",
        "visits"
    )

    return result
