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
            "person_id", "payer", "enrollment_status", "gender", "age_group",
            "medicaid_status", "dual_status", "orec", "originally_disabled_flag",
            "institutional_status", "model_version", "payment_year",
            "collection_start_date", "collection_end_date",
        )
    )

    seed_interaction_factors = (
        spark.table("cms_hcc__enrollment_interaction_factors")
        .withColumn(
            "enrollment_status",
            F.when(F.col("institutional_status") == "Yes", F.lit("Institutional"))
            .otherwise(F.col("enrollment_status")),
        )
        .select("model_version", "factor_type", "gender", "enrollment_status",
                "medicaid_status", "dual_status", "institutional_status",
                "description", "coefficient")
    )

    d = demographics.alias("d")
    sif = seed_interaction_factors.alias("sif")

    # non_institutional_interactions: originally disabled, non-institutional, >= 65
    non_institutional_interactions = (
        d.join(
            sif,
            (F.col("d.gender") == F.col("sif.gender"))
            & (F.col("d.enrollment_status") == F.col("sif.enrollment_status"))
            & (F.col("d.medicaid_status") == F.col("sif.medicaid_status"))
            & (F.col("d.dual_status") == F.col("sif.dual_status"))
            & (F.col("d.institutional_status") == F.col("sif.institutional_status"))
            & (F.col("d.model_version") == F.col("sif.model_version")),
            "inner",
        )
        .where(
            (F.col("d.institutional_status") == "No")
            & (F.col("d.originally_disabled_flag") == "Yes")
        )
        .select(
            F.col("d.person_id"), F.col("d.payer"), F.col("d.model_version"),
            F.col("d.payment_year"), F.col("d.collection_start_date"),
            F.col("d.collection_end_date"), F.col("sif.factor_type"),
            F.col("sif.description"), F.col("sif.coefficient"),
        )
    )

    # institutional_interactions: institutional + medicaid
    institutional_interactions = (
        d.join(
            sif,
            (F.col("d.enrollment_status") == F.col("sif.enrollment_status"))
            & (F.col("d.institutional_status") == F.col("sif.institutional_status"))
            & (F.col("d.model_version") == F.col("sif.model_version")),
            "inner",
        )
        .where(
            (F.col("d.institutional_status") == "Yes")
            & (F.col("d.medicaid_status") == "Yes")
        )
        .select(
            F.col("d.person_id"), F.col("d.payer"), F.col("d.model_version"),
            F.col("d.payment_year"), F.col("d.collection_start_date"),
            F.col("d.collection_end_date"), F.col("sif.factor_type"),
            F.col("sif.description"), F.col("sif.coefficient"),
        )
    )

    unioned = non_institutional_interactions.unionByName(institutional_interactions)

    add_data_types = unioned.select(
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
