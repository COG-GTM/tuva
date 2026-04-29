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
            "medicaid_status", "dual_status", "orec", "institutional_status",
            "model_version", "payment_year", "collection_start_date", "collection_end_date",
        )
    )

    hcc_hierarchy = (
        spark.table("cms_hcc__int_hcc_hierarchy")
        .select("person_id", "payer", "hcc_code", "model_version",
                "payment_year", "collection_start_date", "collection_end_date")
    )

    seed_interaction_factors = (
        spark.table("cms_hcc__disease_interaction_factors")
        .withColumn(
            "enrollment_status",
            F.when(F.col("institutional_status") == "Yes", F.lit("Institutional"))
            .otherwise(F.col("enrollment_status")),
        )
        .select("model_version", "factor_type", "enrollment_status",
                "medicaid_status", "dual_status", "orec",
                "institutional_status", "short_name", "description",
                "hcc_code_1", "hcc_code_2", "coefficient")
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
            F.col("d.medicaid_status"), F.col("d.dual_status"), F.col("d.orec"),
            F.col("d.institutional_status"), F.col("d.model_version"),
            F.col("d.payment_year"), F.col("d.collection_start_date"),
            F.col("d.collection_end_date"), F.col("h.hcc_code"),
        )
    )

    dwh = demographics_with_hccs.alias("dwh")
    sif = seed_interaction_factors.alias("sif")

    demographics_with_interactions = (
        dwh.join(
            sif,
            (F.col("dwh.enrollment_status") == F.col("sif.enrollment_status"))
            & (F.col("dwh.institutional_status") == F.col("sif.institutional_status"))
            & (F.col("dwh.hcc_code") == F.col("sif.hcc_code_1"))
            & (F.col("dwh.model_version") == F.col("sif.model_version"))
            & (
                (F.col("dwh.institutional_status") == "Yes")
                | (
                    (F.col("dwh.medicaid_status") == F.col("sif.medicaid_status"))
                    & (F.col("dwh.dual_status") == F.col("sif.dual_status"))
                    & (F.col("dwh.orec") == F.col("sif.orec"))
                )
            ),
            "inner",
        )
        .select(
            F.col("dwh.person_id"), F.col("dwh.payer"), F.col("dwh.model_version"),
            F.col("dwh.payment_year"), F.col("dwh.collection_start_date"),
            F.col("dwh.collection_end_date"), F.col("sif.factor_type"),
            F.col("sif.description"), F.col("sif.hcc_code_1"),
            F.col("sif.hcc_code_2"), F.col("sif.coefficient"),
        )
    )

    dwi = demographics_with_interactions.alias("dwi")
    dwh2 = demographics_with_hccs.alias("dwh2")

    disease_interactions = (
        dwi.join(
            dwh2,
            (F.col("dwi.person_id") == F.col("dwh2.person_id"))
            & (F.col("dwi.payer") == F.col("dwh2.payer"))
            & (F.col("dwi.hcc_code_2") == F.col("dwh2.hcc_code"))
            & (F.col("dwi.model_version") == F.col("dwh2.model_version"))
            & (F.col("dwi.payment_year") == F.col("dwh2.payment_year"))
            & (F.col("dwi.collection_end_date") == F.col("dwh2.collection_end_date")),
            "inner",
        )
        .select(
            F.col("dwi.person_id"), F.col("dwi.payer"), F.col("dwi.factor_type"),
            F.col("dwi.hcc_code_1"), F.col("dwi.hcc_code_2"),
            F.col("dwi.description"), F.col("dwi.coefficient"),
            F.col("dwi.model_version"), F.col("dwi.payment_year"),
            F.col("dwi.collection_start_date"), F.col("dwi.collection_end_date"),
        )
    )

    add_data_types = disease_interactions.select(
        F.col("person_id").cast("string"),
        F.col("payer").cast("string"),
        F.col("hcc_code_1").cast("string"),
        F.col("hcc_code_2").cast("string"),
        F.col("description").cast("string"),
        F.round(F.col("coefficient").cast("decimal(28,6)"), 3).alias("coefficient"),
        F.col("factor_type").cast("string"),
        F.col("model_version").cast("string"),
        F.col("payment_year").cast("int"),
        F.col("collection_start_date").cast("date"),
        F.col("collection_end_date").cast("date"),
    )

    result = add_data_types.select(
        "person_id", "payer", "hcc_code_1", "hcc_code_2", "description",
        "coefficient", "factor_type", "model_version", "payment_year",
        "collection_start_date", "collection_end_date",
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
