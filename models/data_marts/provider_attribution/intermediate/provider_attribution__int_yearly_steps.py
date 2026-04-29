import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    person_years = spark.table("provider_attribution__int_person_years")
    claims = spark.table("provider_attribution__int_primary_care_claims")
    medical_claim = spark.table("provider_attribution__stg_core__medical_claim")
    calendar = spark.table("provider_attribution__stg_reference_data__calendar")
    member_months = spark.table("provider_attribution__stg_core__member_months")
    terminology_provider = spark.table("provider_attribution__stg_terminology__provider")
    provider_class = spark.table("provider_attribution__provider_classification")

    # all_claim_month: all medical claims joined with calendar
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

    # step1: PCP/NPP primary-care HCPCS within the performance year (12-month)
    step1 = (
        person_years.alias("py")
        .join(
            claims.alias("c"),
            (F.col("py.person_id") == F.col("c.person_id"))
            & (F.col("c.claim_year") == F.col("py.performance_year"))
            & F.col("c.provider_bucket").isin("pcp", "npp")
            & F.col("c.provider_id").isNotNull(),
            "inner"
        )
        .groupBy(
            F.col("py.person_id"),
            F.col("py.performance_year"),
            F.col("c.provider_id"),
            F.coalesce(F.col("c.provider_bucket"), F.lit("unknown")).alias("provider_bucket"),
            F.col("c.prov_specialty")
        )
        .agg(
            F.lit(1).alias("step"),
            F.sum("c.allowed_amount").alias("allowed_amount"),
            F.countDistinct("c.encounter_id").alias("visits")
        )
    )

    step1_benes = step1.select("person_id", "performance_year").distinct()

    # step2: Specialist primary-care HCPCS within the performance year, only unassigned from step1
    step2 = (
        person_years.alias("py")
        .join(
            claims.alias("c"),
            (F.col("py.person_id") == F.col("c.person_id"))
            & (F.col("c.claim_year") == F.col("py.performance_year"))
            & (F.col("c.provider_bucket") == "specialist"),
            "inner"
        )
        .join(
            step1_benes.alias("s1"),
            (F.col("py.person_id") == F.col("s1.person_id"))
            & (F.col("py.performance_year") == F.col("s1.performance_year")),
            "left"
        )
        .where(
            F.col("s1.person_id").isNull()
            & F.col("c.provider_id").isNotNull()
        )
        .groupBy(
            F.col("py.person_id"),
            F.col("py.performance_year"),
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

    step2_benes = step2.select("person_id", "performance_year").distinct()

    # step3: PCP/NPP primary-care HCPCS across expanded 24-month window (Jan Y-1 .. Dec Y)
    step3 = (
        person_years.alias("py")
        .join(
            claims.alias("c"),
            (F.col("py.person_id") == F.col("c.person_id"))
            & (F.col("c.claim_year_month_int").between(
                (F.col("py.performance_year") - 1) * 100 + 1,
                F.col("py.performance_year") * 100 + 12
            ))
            & F.col("c.provider_bucket").isin("pcp", "npp"),
            "inner"
        )
        .join(
            step1_benes.alias("s1"),
            (F.col("py.person_id") == F.col("s1.person_id"))
            & (F.col("py.performance_year") == F.col("s1.performance_year")),
            "left"
        )
        .join(
            step2_benes.alias("s2"),
            (F.col("py.person_id") == F.col("s2.person_id"))
            & (F.col("py.performance_year") == F.col("s2.performance_year")),
            "left"
        )
        .where(
            F.col("s1.person_id").isNull()
            & F.col("s2.person_id").isNull()
            & F.col("c.provider_id").isNotNull()
        )
        .groupBy(
            F.col("py.person_id"),
            F.col("py.performance_year"),
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

    step3_benes = step3.select("person_id", "performance_year").distinct()

    # step4: Primary-care HCPCS fallback across expanded 24-month window, any classification
    step4 = (
        person_years.alias("py")
        .join(
            claims.alias("c"),
            (F.col("py.person_id") == F.col("c.person_id"))
            & (F.col("c.claim_year_month_int").between(
                (F.col("py.performance_year") - 1) * 100 + 1,
                F.col("py.performance_year") * 100 + 12
            )),
            "inner"
        )
        .join(
            step1_benes.alias("s1"),
            (F.col("py.person_id") == F.col("s1.person_id"))
            & (F.col("py.performance_year") == F.col("s1.performance_year")),
            "left"
        )
        .join(
            step2_benes.alias("s2"),
            (F.col("py.person_id") == F.col("s2.person_id"))
            & (F.col("py.performance_year") == F.col("s2.performance_year")),
            "left"
        )
        .join(
            step3_benes.alias("s3"),
            (F.col("py.person_id") == F.col("s3.person_id"))
            & (F.col("py.performance_year") == F.col("s3.performance_year")),
            "left"
        )
        .where(
            F.col("s1.person_id").isNull()
            & F.col("s2.person_id").isNull()
            & F.col("s3.person_id").isNull()
            & F.col("c.provider_id").isNotNull()
        )
        .groupBy(
            F.col("py.person_id"),
            F.col("py.performance_year"),
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

    step4_benes = step4.select("person_id", "performance_year").distinct()

    # step4_pairs: all assigned person/year/provider triples from steps 1-4
    step4_pairs = (
        step1.select("person_id", "performance_year", "provider_id")
        .union(step2.select("person_id", "performance_year", "provider_id"))
        .union(step3.select("person_id", "performance_year", "provider_id"))
        .union(step4.select("person_id", "performance_year", "provider_id"))
        .distinct()
    )

    # step5: Any rendering NPI across expanded 24-month window, for unassigned beneficiaries
    step5 = (
        person_years.alias("py")
        .join(
            all_rendering_claims.alias("arc"),
            (F.col("py.person_id") == F.col("arc.person_id"))
            & (F.col("arc.claim_year_month_int").between(
                (F.col("py.performance_year") - 1) * 100 + 1,
                F.col("py.performance_year") * 100 + 12
            )),
            "inner"
        )
        .join(
            step1_benes.alias("s1"),
            (F.col("py.person_id") == F.col("s1.person_id"))
            & (F.col("py.performance_year") == F.col("s1.performance_year")),
            "left"
        )
        .join(
            step2_benes.alias("s2"),
            (F.col("py.person_id") == F.col("s2.person_id"))
            & (F.col("py.performance_year") == F.col("s2.performance_year")),
            "left"
        )
        .join(
            step3_benes.alias("s3"),
            (F.col("py.person_id") == F.col("s3.person_id"))
            & (F.col("py.performance_year") == F.col("s3.performance_year")),
            "left"
        )
        .join(
            step4_benes.alias("s4"),
            (F.col("py.person_id") == F.col("s4.person_id"))
            & (F.col("py.performance_year") == F.col("s4.performance_year")),
            "left"
        )
        .join(
            step4_pairs.alias("p4"),
            (F.col("py.person_id") == F.col("p4.person_id"))
            & (F.col("py.performance_year") == F.col("p4.performance_year"))
            & (F.col("arc.provider_id") == F.col("p4.provider_id")),
            "left"
        )
        .where(
            F.col("arc.provider_id").isNotNull()
            & F.col("s1.person_id").isNull()
            & F.col("s2.person_id").isNull()
            & F.col("s3.person_id").isNull()
            & F.col("s4.person_id").isNull()
            & F.col("p4.provider_id").isNull()
        )
        .groupBy(
            F.col("py.person_id"),
            F.col("py.performance_year"),
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
        "performance_year",
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
