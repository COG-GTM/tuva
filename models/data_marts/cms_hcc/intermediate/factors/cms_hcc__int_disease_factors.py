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
            "model_version", "payment_year", "risk_model_code",
            "collection_start_date", "collection_end_date",
        )
    )

    hcc_hierarchy = (
        spark.table("cms_hcc__int_hcc_hierarchy")
        .select("person_id", "payer", "hcc_code", "model_version",
                "payment_year", "collection_start_date", "collection_end_date")
    )

    seed_disease_factors = (
        spark.table("cms_hcc__disease_factors")
        .withColumn(
            "enrollment_status",
            F.when(F.col("institutional_status") == "Yes", F.lit("Institutional"))
            .otherwise(F.col("enrollment_status")),
        )
        .select("model_version", "factor_type", "enrollment_status",
                "medicaid_status", "dual_status", "orec",
                "institutional_status", "hcc_code", "description", "coefficient")
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
        .where(F.col("d.enrollment_status") != "New")
        .select(
            F.col("d.person_id"), F.col("d.payer"), F.col("d.enrollment_status"),
            F.col("d.gender"), F.col("d.age_group"), F.col("d.medicaid_status"),
            F.col("d.dual_status"), F.col("d.orec"), F.col("d.institutional_status"),
            F.col("d.model_version"), F.col("d.payment_year"),
            F.col("d.risk_model_code"),
            F.col("d.collection_start_date"), F.col("d.collection_end_date"),
            F.col("h.hcc_code"),
        )
    )

    dwh = demographics_with_hccs.alias("dwh")
    sdf = seed_disease_factors.alias("sdf")

    disease_factors = (
        dwh.join(
            sdf,
            (F.col("dwh.enrollment_status") == F.col("sdf.enrollment_status"))
            & (F.col("dwh.institutional_status") == F.col("sdf.institutional_status"))
            & (F.col("dwh.hcc_code") == F.col("sdf.hcc_code"))
            & (F.col("dwh.model_version") == F.col("sdf.model_version"))
            & (
                (F.col("dwh.institutional_status") == "Yes")
                | (
                    (F.col("dwh.medicaid_status") == F.col("sdf.medicaid_status"))
                    & (F.col("dwh.dual_status") == F.col("sdf.dual_status"))
                    & (F.col("dwh.orec") == F.col("sdf.orec"))
                )
            ),
            "inner",
        )
        .select(
            F.col("dwh.person_id"), F.col("dwh.payer"), F.col("dwh.hcc_code"),
            F.col("dwh.model_version"), F.col("dwh.payment_year"),
            F.col("dwh.collection_start_date"), F.col("dwh.collection_end_date"),
            F.col("dwh.risk_model_code"),
            F.col("sdf.factor_type"), F.col("sdf.description"), F.col("sdf.coefficient"),
        )
    )

    add_data_types = disease_factors.select(
        F.col("person_id").cast("string"),
        F.col("payer").cast("string"),
        F.col("hcc_code").cast("string"),
        F.col("description").cast("string").alias("hcc_description"),
        F.col("risk_model_code").cast("string"),
        F.round(F.col("coefficient").cast("decimal(28,6)"), 3).alias("coefficient"),
        F.col("factor_type").cast("string"),
        F.col("model_version").cast("string"),
        F.col("payment_year").cast("int"),
        F.col("collection_start_date").cast("date"),
        F.col("collection_end_date").cast("date"),
    )

    result = add_data_types.select(
        "person_id", "payer", "hcc_code", "hcc_description", "risk_model_code",
        "coefficient", "factor_type", "model_version", "payment_year",
        "collection_start_date", "collection_end_date",
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
