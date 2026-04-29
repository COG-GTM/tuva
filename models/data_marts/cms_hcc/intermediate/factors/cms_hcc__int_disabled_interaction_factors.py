import os
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', 'pyspark', 'utils'))


def run(spark: SparkSession) -> DataFrame:
    demographics = (
        spark.table("cms_hcc__int_demographic_factors")
        .select("person_id", "payer", "enrollment_status", "institutional_status",
                "orec", "age_group", "model_version", "payment_year",
                "collection_start_date", "collection_end_date")
    )

    hcc_hierarchy = (
        spark.table("cms_hcc__int_hcc_hierarchy")
        .select("person_id", "payer", "hcc_code", "model_version",
                "payment_year", "collection_start_date", "collection_end_date")
    )

    seed_interaction_factors = (
        spark.table("cms_hcc__disabled_interaction_factors")
        .withColumn(
            "enrollment_status",
            F.when(F.col("institutional_status") == "Yes", F.lit("Institutional"))
            .otherwise(F.col("enrollment_status")),
        )
        .select("model_version", "factor_type", "enrollment_status",
                "institutional_status", "short_name", "description",
                "hcc_code", "coefficient")
    )

    d = demographics.alias("d")
    h = hcc_hierarchy.alias("h")

    demographics_with_hccs = (
        d.join(
            h,
            (F.col("d.person_id") == F.col("h.person_id"))
            & (F.col("d.payer") == F.col("h.payer"))
            & (F.col("d.model_version") == F.col("h.model_version"))
            & (F.col("d.payment_year") == F.col("h.payment_year"))
            & (F.col("d.collection_end_date") == F.col("h.collection_end_date")),
            "inner",
        )
        .select(
            F.col("d.person_id"), F.col("d.payer"), F.col("d.enrollment_status"),
            F.col("d.institutional_status"), F.col("d.orec"), F.col("d.age_group"),
            F.col("d.model_version"), F.col("d.payment_year"),
            F.col("d.collection_start_date"), F.col("d.collection_end_date"),
            F.col("h.hcc_code"),
        )
    )

    dwh = demographics_with_hccs.alias("dwh")
    sif = seed_interaction_factors.alias("sif")

    aged_age_groups = ["65-69", "70-74", "75-79", "80-84", "85-89", "90-94", ">=95"]

    interactions = (
        dwh.join(
            sif,
            (F.col("dwh.enrollment_status") == F.col("sif.enrollment_status"))
            & (F.col("dwh.institutional_status") == F.col("sif.institutional_status"))
            & (F.col("dwh.hcc_code") == F.col("sif.hcc_code"))
            & (F.col("dwh.model_version") == F.col("sif.model_version")),
            "inner",
        )
        .where(
            ~F.col("dwh.age_group").isin(aged_age_groups)
            & (F.col("dwh.orec") != "Aged")
        )
        .select(
            F.col("dwh.person_id"), F.col("dwh.payer"), F.col("dwh.model_version"),
            F.col("dwh.payment_year"), F.col("dwh.collection_start_date"),
            F.col("dwh.collection_end_date"), F.col("sif.factor_type"),
            F.col("sif.description"), F.col("sif.coefficient"),
        )
    )

    add_data_types = interactions.select(
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
