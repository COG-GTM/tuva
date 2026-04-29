import os
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import substring_col


def run(spark: SparkSession) -> DataFrame:
    seed_adjustment_rates = (
        spark.table("cms_hcc__adjustment_rates")
        .select("model_version", "payment_year", "normalization_factor",
                "ma_coding_pattern_adjustment")
    )

    risk_factors = (
        spark.table("cms_hcc__patient_risk_factors_monthly")
        .select(
            "person_id", "payer", "factor_type", "coefficient",
            "risk_model_code", "enrollment_status",
            "enrollment_status_default", "orec_default",
            "model_version", "payment_year",
            "collection_start_date", "collection_end_date",
        )
    )

    member_months_tbl = (
        spark.table("cms_hcc__stg_core__member_months")
        .withColumn("eligible_year", F.substring(F.col("year_month"), 1, 4).cast("int"))
        .groupBy("person_id", "payer", "eligible_year")
        .agg(F.count(F.lit(1)).alias("member_months"))
    )

    # raw_score
    raw_score = (
        risk_factors
        .groupBy(
            "person_id", "payer", "factor_type", "risk_model_code",
            "enrollment_status", "enrollment_status_default", "orec_default",
            "model_version", "payment_year",
            "collection_start_date", "collection_end_date",
        )
        .agg(F.sum("coefficient").alias("risk_score"))
    )

    # transition_scores
    rs = raw_score.alias("rs")
    adj = seed_adjustment_rates.alias("adj")

    transition_scores = (
        rs.join(
            adj,
            (F.col("rs.payment_year") == F.col("adj.payment_year"))
            & (F.col("rs.model_version") == F.col("adj.model_version")),
            "left",
        )
        .select(
            F.col("rs.person_id"), F.col("rs.payer"), F.col("rs.factor_type"),
            F.col("rs.risk_model_code"), F.col("rs.enrollment_status"),
            F.col("rs.enrollment_status_default"), F.col("rs.orec_default"),
            F.col("rs.risk_score").alias("raw_risk_score"),
            # weighted_raw_risk_score with v24/v28 transition blending
            F.when((F.col("rs.payment_year") <= 2023) & (F.col("rs.model_version") == "CMS-HCC-V24"), F.col("rs.risk_score"))
            .when((F.col("rs.payment_year") == 2024) & (F.col("rs.model_version") == "CMS-HCC-V24"), F.col("rs.risk_score") * 0.67)
            .when((F.col("rs.payment_year") == 2025) & (F.col("rs.model_version") == "CMS-HCC-V24"), F.col("rs.risk_score") * 0.33)
            .when((F.col("rs.payment_year") >= 2026) & (F.col("rs.model_version") == "CMS-HCC-V24"), F.lit(0))
            .when((F.col("rs.payment_year") <= 2023) & (F.col("rs.model_version") == "CMS-HCC-V28"), F.lit(0))
            .when((F.col("rs.payment_year") == 2024) & (F.col("rs.model_version") == "CMS-HCC-V28"), F.col("rs.risk_score") * 0.33)
            .when((F.col("rs.payment_year") == 2025) & (F.col("rs.model_version") == "CMS-HCC-V28"), F.col("rs.risk_score") * 0.67)
            .when((F.col("rs.payment_year") >= 2026) & (F.col("rs.model_version") == "CMS-HCC-V28"), F.col("rs.risk_score"))
            .alias("weighted_raw_risk_score"),
            # normalization_factor with overrides for 2024-2026
            F.when((F.col("rs.payment_year") == 2026) & (F.col("rs.model_version") == "CMS-HCC-V28"), F.lit(1.067))
            .when((F.col("rs.payment_year") == 2025) & (F.col("rs.model_version") == "CMS-HCC-V24"), F.lit(1.153))
            .when((F.col("rs.payment_year") == 2024) & (F.col("rs.model_version") == "CMS-HCC-V24"), F.lit(1.146))
            .otherwise(F.col("adj.normalization_factor"))
            .alias("normalization_factor"),
            # ma_coding_pattern_adjustment
            F.when(F.col("rs.payment_year").isin(2024, 2025, 2026), F.lit(0.059))
            .otherwise(F.col("adj.ma_coding_pattern_adjustment"))
            .alias("ma_coding_pattern_adjustment"),
            F.col("rs.model_version"),
            F.col("rs.payment_year"),
            F.col("rs.collection_start_date"),
            F.col("rs.collection_end_date"),
        )
    )

    # normalized
    normalized = (
        transition_scores
        .withColumn(
            "normalized_risk_score",
            F.round(F.col("weighted_raw_risk_score") / F.col("normalization_factor"), 3),
        )
    )

    # payment
    payment = (
        normalized
        .withColumn(
            "payment_risk_score",
            F.round(F.col("normalized_risk_score") * (1 - F.col("ma_coding_pattern_adjustment")), 3),
        )
    )

    # blended: pivot by model_version
    blended = (
        payment
        .groupBy(
            "person_id", "payer", "factor_type", "risk_model_code",
            "enrollment_status", "enrollment_status_default", "orec_default",
            "payment_year", "collection_start_date", "collection_end_date",
        )
        .agg(
            F.max(F.when(F.col("model_version") == "CMS-HCC-V24", F.col("weighted_raw_risk_score"))).alias("v24_risk_score"),
            F.max(F.when(F.col("model_version") == "CMS-HCC-V28", F.col("weighted_raw_risk_score"))).alias("v28_risk_score"),
            F.sum("weighted_raw_risk_score").alias("blended_risk_score"),
            F.sum("normalized_risk_score").alias("normalized_risk_score"),
            F.sum("payment_risk_score").alias("payment_risk_score"),
        )
    )

    # weighted_score with member_months
    bl = blended.alias("bl")
    mm = member_months_tbl.alias("mm")

    weighted_score = (
        bl.join(
            mm,
            (F.col("bl.person_id") == F.col("mm.person_id"))
            & (F.col("bl.payer") == F.col("mm.payer"))
            & (F.col("bl.payment_year") == F.col("mm.eligible_year")),
            "left",
        )
        .select(
            F.col("bl.person_id"), F.col("bl.payer"), F.col("bl.factor_type"),
            F.col("bl.risk_model_code"), F.col("bl.enrollment_status"),
            F.col("bl.enrollment_status_default"), F.col("bl.orec_default"),
            F.col("bl.v24_risk_score"), F.col("bl.v28_risk_score"),
            F.col("bl.blended_risk_score"), F.col("bl.normalized_risk_score"),
            F.col("bl.payment_risk_score"),
            F.col("mm.member_months"),
            (F.col("bl.payment_risk_score") * F.col("mm.member_months")).alias("payment_risk_score_weighted_by_months"),
            F.col("bl.payment_year"), F.col("bl.collection_start_date"),
            F.col("bl.collection_end_date"),
        )
    )

    # add_data_types
    add_data_types = weighted_score.select(
        F.col("person_id").cast("string"),
        F.col("payer").cast("string"),
        F.col("factor_type").cast("string"),
        F.col("risk_model_code").cast("string"),
        F.col("enrollment_status").cast("string"),
        F.col("enrollment_status_default").cast("boolean"),
        F.col("orec_default").cast("boolean"),
        F.round(F.col("v24_risk_score").cast("decimal(28,6)"), 3).alias("v24_risk_score"),
        F.round(F.col("v28_risk_score").cast("decimal(28,6)"), 3).alias("v28_risk_score"),
        F.round(F.col("blended_risk_score").cast("decimal(28,6)"), 3).alias("blended_risk_score"),
        F.round(F.col("normalized_risk_score").cast("decimal(28,6)"), 3).alias("normalized_risk_score"),
        F.round(F.col("payment_risk_score").cast("decimal(28,6)"), 3).alias("payment_risk_score"),
        F.round(F.col("payment_risk_score_weighted_by_months").cast("decimal(28,6)"), 3).alias("payment_risk_score_weighted_by_months"),
        F.col("member_months").cast("int"),
        F.col("payment_year").cast("int"),
        F.col("collection_start_date").cast("date"),
        F.col("collection_end_date").cast("date"),
    )

    result = add_data_types.select(
        "person_id", "payer", "factor_type", "risk_model_code",
        "enrollment_status", "enrollment_status_default", "orec_default",
        "v24_risk_score", "v28_risk_score", "blended_risk_score",
        "normalized_risk_score", "payment_risk_score",
        "payment_risk_score_weighted_by_months", "member_months",
        "payment_year", "collection_start_date", "collection_end_date",
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
