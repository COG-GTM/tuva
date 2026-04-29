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
    member_months = spark.table("provider_attribution__stg_core__member_months")
    provider_ranking = spark.table("provider_attribution__provider_ranking")

    # claim_bounds and params: determine as_of_date
    claim_bounds = primary_care_claims.agg(
        F.max("claim_end_date").alias("max_claim_end_date")
    )

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
            (F.col("full_date") >= F.add_months(F.lit(as_of_date_val).cast("date"), -11))
            & (F.col("full_date") <= F.lit(as_of_date_val).cast("date"))
        )
        .select("year_month_int", "first_day_of_month", "last_day_of_month")
        .distinct()
    )

    # months_24: last 24 calendar months ending at as_of_date
    months_24 = (
        calendar
        .where(
            (F.col("full_date") >= F.add_months(F.lit(as_of_date_val).cast("date"), -23))
            & (F.col("full_date") <= F.lit(as_of_date_val).cast("date"))
        )
        .select("year_month_int", "first_day_of_month", "last_day_of_month")
        .distinct()
    )

    lookback_start_24 = months_24.agg(F.min("first_day_of_month")).collect()[0][0]

    # eligible: beneficiaries with at least one member month in the 12-month window
    eligible = (
        member_months.alias("mm")
        .join(
            months_12.alias("m"),
            F.col("mm.year_month") == F.col("m.year_month_int").cast("string"),
            "inner"
        )
        .select("mm.person_id")
        .distinct()
    )

    # assigned: top-ranked provider per beneficiary in current scope
    assigned = (
        provider_ranking.alias("pr")
        .join(eligible.alias("e"), F.col("pr.person_id") == F.col("e.person_id"), "inner")
        .where(
            (F.col("pr.scope") == "current")
            & (F.col("pr.as_of_date") == F.lit(as_of_date_val).cast("date"))
            & (F.col("pr.ranking") == 1)
        )
        .select(
            F.col("pr.person_id"),
            F.col("pr.as_of_date"),
            F.col("pr.provider_id"),
            F.col("pr.provider_bucket"),
            F.col("pr.prov_specialty"),
            F.col("pr.step").alias("assigned_step"),
            F.col("pr.step_description"),
            F.col("pr.allowed_amount"),
            F.col("pr.visits"),
            F.col("pr.lookback_start_date"),
            F.col("pr.lookback_end_date"),
            F.col("pr.attribution_key")
        )
    )

    # missing: eligible beneficiaries not assigned
    missing = (
        eligible.alias("e")
        .join(assigned.alias("a"), F.col("e.person_id") == F.col("a.person_id"), "left")
        .where(F.col("a.person_id").isNull())
        .select(F.col("e.person_id"))
    )

    # fallback: placeholder row for missing beneficiaries
    fallback = (
        missing
        .select(
            F.col("person_id"),
            F.lit(as_of_date_val).cast("date").alias("as_of_date"),
            F.lit("9999999999").alias("provider_id"),
            F.lit("no_eligible_history").alias("provider_bucket"),
            F.lit("No assignable claims history").alias("prov_specialty"),
            F.lit(0).alias("assigned_step"),
            F.lit("No assignable history").alias("step_description"),
            F.lit(0).cast("decimal(28,6)").alias("allowed_amount"),
            F.lit(0).alias("visits"),
            F.lit(lookback_start_24).cast("date").alias("lookback_start_date"),
            F.lit(as_of_date_val).cast("date").alias("lookback_end_date"),
            F.concat(
                F.lit("current|"),
                F.regexp_replace(F.lit(as_of_date_val).cast("string"), "-", ""),
                F.lit("|"),
                F.col("person_id")
            ).alias("attribution_key")
        )
    )

    tuva_last_run = F.current_timestamp()

    result = (
        assigned.withColumn("tuva_last_run", tuva_last_run)
        .unionByName(
            fallback.withColumn("tuva_last_run", tuva_last_run)
        )
    )

    return result
