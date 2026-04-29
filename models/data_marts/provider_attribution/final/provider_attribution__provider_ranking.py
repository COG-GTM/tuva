import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    calendar_full = spark.table("provider_attribution__stg_reference_data__calendar")
    primary_care_claims = spark.table("provider_attribution__int_primary_care_claims")
    medical_claim = spark.table("provider_attribution__stg_core__medical_claim")
    member_months = spark.table("provider_attribution__stg_core__member_months")
    terminology_provider = spark.table("provider_attribution__stg_terminology__provider")
    provider_class = spark.table("provider_attribution__provider_classification")
    person_years = spark.table("provider_attribution__int_person_years")

    # calendar_months: distinct year_month_int with first/last day of month
    calendar_months = (
        calendar_full
        .select("year_month_int", "first_day_of_month", "last_day_of_month")
        .distinct()
    )

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

    # months_12 / months_24: rolling window helpers
    months_12 = (
        calendar_full
        .where(
            (F.col("full_date") >= F.add_months(F.lit(as_of_date_val).cast("date"), -11))
            & (F.col("full_date") <= F.lit(as_of_date_val).cast("date"))
        )
        .select("year_month_int", "first_day_of_month", "last_day_of_month")
        .distinct()
    )

    months_24 = (
        calendar_full
        .where(
            (F.col("full_date") >= F.add_months(F.lit(as_of_date_val).cast("date"), -23))
            & (F.col("full_date") <= F.lit(as_of_date_val).cast("date"))
        )
        .select("year_month_int", "first_day_of_month", "last_day_of_month")
        .distinct()
    )

    lookback_start_12 = months_12.agg(F.min("first_day_of_month")).collect()[0][0]
    lookback_start_24 = months_24.agg(F.min("first_day_of_month")).collect()[0][0]

    # eligible_current: beneficiaries with at least one member month in the 12-month window
    eligible_current = (
        member_months.alias("mm")
        .join(
            months_12.alias("m"),
            F.col("mm.year_month") == F.col("m.year_month_int").cast("string"),
            "inner"
        )
        .select("mm.person_id")
        .distinct()
    )

    # claims_12 / claims_24: primary care claims in the respective windows
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
            calendar_full.alias("cal"),
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

    # eligible_all_claims
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

    # all_rendering_claims
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

    # --- CURRENT SCOPE: Build all potential providers per step ---

    # Step 1: 12-month PCP/NPP
    curr_step1 = (
        claims_12
        .where(F.col("provider_id").isNotNull() & F.col("provider_bucket").isin("pcp", "npp"))
        .groupBy("person_id", "provider_id", "provider_bucket", "prov_specialty")
        .agg(
            F.lit(1).alias("step"),
            F.sum("allowed_amount").alias("allowed_amount"),
            F.countDistinct("encounter_id").alias("visits")
        )
    )

    # Step 2: 12-month specialist
    curr_step2 = (
        claims_12
        .where(F.col("provider_id").isNotNull() & (F.col("provider_bucket") == "specialist"))
        .groupBy("person_id", "provider_id", "provider_bucket", "prov_specialty")
        .agg(
            F.lit(2).alias("step"),
            F.sum("allowed_amount").alias("allowed_amount"),
            F.countDistinct("encounter_id").alias("visits")
        )
    )

    # Step 3: 24-month PCP/NPP
    curr_step3 = (
        claims_24
        .where(F.col("provider_id").isNotNull() & F.col("provider_bucket").isin("pcp", "npp"))
        .groupBy("person_id", "provider_id", "provider_bucket", "prov_specialty")
        .agg(
            F.lit(3).alias("step"),
            F.sum("allowed_amount").alias("allowed_amount"),
            F.countDistinct("encounter_id").alias("visits")
        )
    )

    # Step 4: 24-month any classification
    curr_step4 = (
        claims_24
        .where(F.col("provider_id").isNotNull())
        .groupBy("person_id", "provider_id", "provider_bucket", "prov_specialty")
        .agg(
            F.lit(4).alias("step"),
            F.sum("allowed_amount").alias("allowed_amount"),
            F.countDistinct("encounter_id").alias("visits")
        )
    )

    # Step 5: 24-month any rendering NPI
    curr_step5 = (
        all_rendering_claims.alias("arc")
        .join(months_24.alias("m"), F.col("arc.claim_year_month_int") == F.col("m.year_month_int"), "inner")
        .where(
            F.col("arc.provider_id").isNotNull()
            & (F.col("arc.claim_end_date") <= F.lit(as_of_date_val).cast("date"))
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

    current_all_steps = (
        curr_step1.unionByName(curr_step2)
        .unionByName(curr_step3)
        .unionByName(curr_step4)
        .unionByName(curr_step5)
    )

    # current_unique: collapse to earliest qualifying step per person/provider
    w_curr = Window.partitionBy("person_id", "provider_id").orderBy("step")
    current_unique = (
        current_all_steps
        .withColumn("step_choice_rank", F.row_number().over(w_curr))
        .where(F.col("step_choice_rank") == 1)
        .drop("step_choice_rank")
    )

    # --- YEARLY SCOPE ---

    # Step 1: performance year PCP/NPP
    yr_step1 = (
        person_years.alias("py")
        .join(
            primary_care_claims.alias("c"),
            (F.col("py.person_id") == F.col("c.person_id"))
            & (F.col("c.claim_year") == F.col("py.performance_year"))
            & F.col("c.provider_id").isNotNull()
            & F.col("c.provider_bucket").isin("pcp", "npp"),
            "inner"
        )
        .groupBy(
            F.col("py.person_id"), F.col("py.performance_year"),
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

    # Step 2: performance year specialist
    yr_step2 = (
        person_years.alias("py")
        .join(
            primary_care_claims.alias("c"),
            (F.col("py.person_id") == F.col("c.person_id"))
            & (F.col("c.claim_year") == F.col("py.performance_year"))
            & F.col("c.provider_id").isNotNull()
            & (F.col("c.provider_bucket") == "specialist"),
            "inner"
        )
        .groupBy(
            F.col("py.person_id"), F.col("py.performance_year"),
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

    # Step 3: 24-month expanded PCP/NPP
    yr_step3 = (
        person_years.alias("py")
        .join(
            primary_care_claims.alias("c"),
            (F.col("py.person_id") == F.col("c.person_id"))
            & (F.col("c.claim_year_month_int").between(
                (F.col("py.performance_year") - 1) * 100 + 1,
                F.col("py.performance_year") * 100 + 12
            ))
            & F.col("c.provider_id").isNotNull()
            & F.col("c.provider_bucket").isin("pcp", "npp"),
            "inner"
        )
        .groupBy(
            F.col("py.person_id"), F.col("py.performance_year"),
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

    # Step 4: 24-month expanded any classification
    yr_step4 = (
        person_years.alias("py")
        .join(
            primary_care_claims.alias("c"),
            (F.col("py.person_id") == F.col("c.person_id"))
            & (F.col("c.claim_year_month_int").between(
                (F.col("py.performance_year") - 1) * 100 + 1,
                F.col("py.performance_year") * 100 + 12
            ))
            & F.col("c.provider_id").isNotNull(),
            "inner"
        )
        .groupBy(
            F.col("py.person_id"), F.col("py.performance_year"),
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

    # Step 5: 24-month expanded any rendering NPI (yearly scope)
    yr_arc = (
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
            F.col("e.claim_year_month_int"),
            F.col("e.allowed_amount"),
            F.coalesce(F.col("pc.provider_bucket"), F.lit("other_individual")).alias("provider_bucket"),
            F.coalesce(F.col("pc.prov_specialty"), F.col("sp.primary_specialty_description")).alias("prov_specialty")
        )
    )

    yr_step5 = (
        person_years.alias("py")
        .join(
            yr_arc.alias("arc"),
            (F.col("py.person_id") == F.col("arc.person_id"))
            & (F.col("arc.claim_year_month_int").between(
                (F.col("py.performance_year") - 1) * 100 + 1,
                F.col("py.performance_year") * 100 + 12
            )),
            "inner"
        )
        .where(F.col("arc.provider_id").isNotNull())
        .groupBy(
            F.col("py.person_id"), F.col("py.performance_year"),
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

    yearly_all_steps = (
        yr_step1.unionByName(yr_step2)
        .unionByName(yr_step3)
        .unionByName(yr_step4)
        .unionByName(yr_step5)
    )

    # yearly_unique: collapse to earliest qualifying step per person/year/provider
    w_yr = Window.partitionBy("person_id", "performance_year", "provider_id").orderBy("step")
    yearly_unique = (
        yearly_all_steps
        .withColumn("step_choice_rank", F.row_number().over(w_yr))
        .where(F.col("step_choice_rank") == 1)
        .drop("step_choice_rank")
    )

    # --- Build yearly scope with lookback dates ---
    step_desc = (
        F.when(F.col("step") == 1, "12-month PCP/NPP primary-care HCPCS")
        .when(F.col("step") == 2, "12-month specialist primary-care HCPCS")
        .when(F.col("step") == 3, "24-month PCP/NPP primary-care HCPCS")
        .when(F.col("step") == 4, "24-month primary-care HCPCS (any classification)")
        .when(F.col("step") == 5, "24-month any rendering NPI")
        .otherwise("Unknown")
    )

    yearly = (
        yearly_unique.alias("y")
        .join(
            calendar_months.alias("start_curr"),
            F.col("start_curr.year_month_int") == (F.col("y.performance_year") * 100 + 1),
            "left"
        )
        .join(
            calendar_months.alias("start_prev"),
            F.col("start_prev.year_month_int") == ((F.col("y.performance_year") - 1) * 100 + 1),
            "left"
        )
        .join(
            calendar_months.alias("end_curr"),
            F.col("end_curr.year_month_int") == (F.col("y.performance_year") * 100 + 12),
            "left"
        )
        .select(
            F.col("y.person_id"),
            F.col("y.performance_year").cast("int").alias("performance_year"),
            F.lit(None).cast("date").alias("as_of_date"),
            F.col("y.provider_id"),
            F.col("y.provider_bucket"),
            F.col("y.prov_specialty"),
            F.col("y.step"),
            step_desc.alias("step_description"),
            F.col("y.allowed_amount"),
            F.col("y.visits"),
            F.lit("yearly").alias("scope"),
            F.when(
                F.col("y.step").isin(3, 4, 5),
                F.coalesce(F.col("start_prev.first_day_of_month"), F.col("start_curr.first_day_of_month"))
            ).otherwise(F.col("start_curr.first_day_of_month")).alias("lookback_start_date"),
            F.col("end_curr.last_day_of_month").alias("lookback_end_date"),
            F.concat(
                F.lit("yearly|"),
                F.col("y.performance_year").cast("string"),
                F.lit("|"),
                F.col("y.person_id")
            ).alias("attribution_key")
        )
    )

    w_yr_rank = Window.partitionBy("person_id", "performance_year").orderBy(
        F.col("step").asc(), F.col("allowed_amount").desc(),
        F.col("visits").desc(), F.col("provider_id")
    )
    yearly = yearly.withColumn("ranking", F.rank().over(w_yr_rank))

    # --- Build current scope ---
    current_scope = (
        current_unique.alias("s")
        .join(
            eligible_current.alias("ec"),
            F.col("s.person_id") == F.col("ec.person_id"),
            "inner"
        )
        .select(
            F.col("s.person_id"),
            F.lit(None).cast("int").alias("performance_year"),
            F.lit(as_of_date_val).cast("date").alias("as_of_date"),
            F.col("s.provider_id"),
            F.col("s.provider_bucket"),
            F.col("s.prov_specialty"),
            F.col("s.step"),
            step_desc.alias("step_description"),
            F.col("s.allowed_amount"),
            F.col("s.visits"),
            F.lit("current").alias("scope"),
            F.when(
                F.col("s.step").isin(1, 2), F.lit(lookback_start_12).cast("date")
            ).otherwise(F.lit(lookback_start_24).cast("date")).alias("lookback_start_date"),
            F.lit(as_of_date_val).cast("date").alias("lookback_end_date"),
            F.concat(
                F.lit("current|"),
                F.regexp_replace(F.lit(as_of_date_val).cast("string"), "-", ""),
                F.lit("|"),
                F.col("s.person_id")
            ).alias("attribution_key")
        )
    )

    w_curr_rank = Window.partitionBy("person_id").orderBy(
        F.col("step").asc(), F.col("allowed_amount").desc(),
        F.col("visits").desc(), F.col("provider_id")
    )
    current_scope = current_scope.withColumn("ranking", F.rank().over(w_curr_rank))

    # tuva_last_run placeholder
    tuva_last_run = F.current_timestamp()

    # Final union of yearly and current
    result = (
        yearly.select(
            "person_id", "performance_year", "as_of_date", "provider_id",
            "provider_bucket", "prov_specialty", "step", "step_description",
            "allowed_amount", "visits", "scope", "lookback_start_date",
            "lookback_end_date", "ranking", "attribution_key"
        ).withColumn("tuva_last_run", tuva_last_run)
        .unionByName(
            current_scope.select(
                "person_id", "performance_year", "as_of_date", "provider_id",
                "provider_bucket", "prov_specialty", "step", "step_description",
                "allowed_amount", "visits", "scope", "lookback_start_date",
                "lookback_end_date", "ranking", "attribution_key"
            ).withColumn("tuva_last_run", tuva_last_run)
        )
    )

    return result
