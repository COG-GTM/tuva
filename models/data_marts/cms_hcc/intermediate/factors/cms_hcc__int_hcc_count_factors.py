import os
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', 'pyspark', 'utils'))


def run(spark: SparkSession) -> DataFrame:
    demographics = (
        spark.table("cms_hcc__int_demographic_factors")
        .select(
            "person_id", "payer", "enrollment_status", "medicaid_status",
            "dual_status", "orec", "institutional_status", "model_version",
            "payment_year", "collection_start_date", "collection_end_date",
        )
    )

    seed_payment_hcc_count_factors = (
        spark.table("cms_hcc__payment_hcc_count_factors")
        .withColumn(
            "enrollment_status",
            F.when(F.col("institutional_status") == "Yes", F.lit("Institutional"))
            .otherwise(F.col("enrollment_status")),
        )
        .select("model_version", "factor_type", "enrollment_status",
                "medicaid_status", "dual_status", "orec",
                "institutional_status", "payment_hcc_count",
                "description", "coefficient")
    )

    hcc_hierarchy = (
        spark.table("cms_hcc__int_hcc_hierarchy")
        .select("person_id", "payer", "hcc_code", "model_version",
                "payment_year", "collection_start_date", "collection_end_date")
    )

    d = demographics.alias("d")
    h = hcc_hierarchy.alias("h")

    demographics_with_hcc_counts = (
        d.join(
            h,
            (F.col("d.person_id") == F.col("h.person_id"))
            & (F.col("d.payer") == F.col("h.payer"))
            & (F.col("d.model_version") == F.col("h.model_version"))
            & (F.col("d.payment_year") == F.col("h.payment_year"))
            & (F.col("d.collection_end_date") == F.col("h.collection_end_date")),
            "inner",
        )
        .groupBy(
            F.col("d.person_id"), F.col("d.payer"), F.col("d.enrollment_status"),
            F.col("d.medicaid_status"), F.col("d.dual_status"), F.col("d.orec"),
            F.col("d.institutional_status"), F.col("d.model_version"),
            F.col("d.payment_year"), F.col("d.collection_start_date"),
            F.col("d.collection_end_date"),
        )
        .agg(F.count(F.col("h.hcc_code")).alias("hcc_count"))
    )

    # hcc_counts_normalized
    hcc_counts_normalized = (
        demographics_with_hcc_counts
        .withColumn(
            "hcc_count_string",
            F.when(F.col("hcc_count") >= 10, F.lit(">=10"))
            .otherwise(F.col("hcc_count").cast("string")),
        )
    )

    hcn = hcc_counts_normalized.alias("hcn")
    sph = seed_payment_hcc_count_factors.alias("sph")

    hcc_counts = (
        hcn.join(
            sph,
            (F.col("hcn.enrollment_status") == F.col("sph.enrollment_status"))
            & (F.col("hcn.institutional_status") == F.col("sph.institutional_status"))
            & (F.col("hcn.hcc_count_string") == F.col("sph.payment_hcc_count"))
            & (F.col("hcn.model_version") == F.col("sph.model_version"))
            & (
                (F.col("hcn.institutional_status") == "Yes")
                | (
                    (F.col("hcn.medicaid_status") == F.col("sph.medicaid_status"))
                    & (F.col("hcn.dual_status") == F.col("sph.dual_status"))
                    & (F.col("hcn.orec") == F.col("sph.orec"))
                )
            ),
            "inner",
        )
        .select(
            F.col("hcn.person_id"), F.col("hcn.payer"), F.col("hcn.model_version"),
            F.col("hcn.payment_year"), F.col("hcn.collection_start_date"),
            F.col("hcn.collection_end_date"), F.col("sph.factor_type"),
            F.col("sph.description"), F.col("sph.coefficient"),
        )
    )

    add_data_types = hcc_counts.select(
        F.col("person_id").cast("string"),
        F.col("payer").cast("string"),
        F.col("description").cast("string"),
        F.round(F.col("coefficient").cast("decimal(28,6)"), 3).alias("coefficient"),
        F.col("factor_type").cast("string"),
        F.col("model_version").cast("string"),
        F.col("payment_year").cast("int"),
        F.col("collection_start_date").cast("date"),
        F.col("collection_end_date").cast("date"),
    )

    result = add_data_types.select(
        "person_id", "payer", "description", "coefficient", "factor_type",
        "model_version", "payment_year", "collection_start_date", "collection_end_date",
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
