import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    person_years = spark.table("provider_attribution__int_person_years")
    calendar = spark.table("provider_attribution__stg_reference_data__calendar")
    provider_ranking = spark.table("provider_attribution__provider_ranking")

    # eligible: person_id + performance_year from person_years
    eligible = person_years.select("person_id", "performance_year")

    # calendar_months: distinct year_month_int with first/last day of month
    calendar_months = (
        calendar
        .select("year_month_int", "first_day_of_month", "last_day_of_month")
        .distinct()
    )

    # assigned: top-ranked provider per beneficiary/year in yearly scope
    assigned = (
        provider_ranking
        .where(
            (F.col("scope") == "yearly")
            & (F.col("ranking") == 1)
        )
        .select(
            F.col("person_id"),
            F.col("performance_year"),
            F.col("provider_id"),
            F.col("provider_bucket"),
            F.col("prov_specialty"),
            F.col("step").alias("assigned_step"),
            F.col("step_description"),
            F.col("allowed_amount"),
            F.col("visits"),
            F.col("lookback_start_date"),
            F.col("lookback_end_date"),
            F.col("attribution_key")
        )
    )

    # missing: eligible beneficiary/years not assigned
    missing = (
        eligible.alias("e")
        .join(
            assigned.alias("a"),
            (F.col("e.person_id") == F.col("a.person_id"))
            & (F.col("e.performance_year") == F.col("a.performance_year")),
            "left"
        )
        .where(F.col("a.person_id").isNull())
        .select(F.col("e.person_id"), F.col("e.performance_year"))
    )

    # fallback: placeholder rows for missing beneficiary/years
    fallback = (
        missing.alias("m")
        .join(
            calendar_months.alias("start_curr"),
            F.col("start_curr.year_month_int") == (F.col("m.performance_year") * 100 + 1),
            "left"
        )
        .join(
            calendar_months.alias("end_curr"),
            F.col("end_curr.year_month_int") == (F.col("m.performance_year") * 100 + 12),
            "left"
        )
        .select(
            F.col("m.person_id"),
            F.col("m.performance_year"),
            F.lit("9999999999").alias("provider_id"),
            F.lit("no_eligible_history").alias("provider_bucket"),
            F.lit("No assignable claims history").alias("prov_specialty"),
            F.lit(0).alias("assigned_step"),
            F.lit("No assignable history").alias("step_description"),
            F.lit(0).cast("decimal(28,6)").alias("allowed_amount"),
            F.lit(0).alias("visits"),
            F.col("start_curr.first_day_of_month").alias("lookback_start_date"),
            F.col("end_curr.last_day_of_month").alias("lookback_end_date"),
            F.concat(
                F.lit("yearly|"),
                F.col("m.performance_year").cast("string"),
                F.lit("|"),
                F.col("m.person_id")
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
