import os
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))


def run(spark: SparkSession) -> DataFrame:
    demo_factors = spark.table("cms_hcc__int_demographic_factors")

    # demographic_factors with concatenated description
    demographic_factors = (
        demo_factors
        .select(
            F.col("person_id"), F.col("payer"),
            F.concat(
                F.col("gender"), F.lit(", "),
                F.col("age_group"), F.lit(" Years, "),
                F.col("enrollment_status"), F.lit(" Enrollee, "),
                F.when(F.col("medicaid_status") == "Yes", F.lit("Medicaid"))
                .otherwise(F.lit("Non-Medicaid")),
                F.lit(", "),
                F.col("dual_status"), F.lit(" Dual, "),
                F.col("orec"), F.lit(", "),
                F.when(F.col("institutional_status") == "Yes", F.lit("Institutional"))
                .otherwise(F.lit("Non-Institutional")),
            ).alias("description"),
            F.col("coefficient"),
            F.col("factor_type"),
            F.col("model_version"),
            F.col("payment_year"),
            F.col("collection_start_date"),
            F.col("collection_end_date"),
        )
    )

    # demographic_defaults
    demographic_defaults = (
        demo_factors
        .select(
            "person_id", "payer", "model_version", "enrollment_status",
            "risk_model_code", "enrollment_status_default",
            "medicaid_dual_status_default", "orec_default",
            "institutional_status_default", "payment_year",
            "collection_start_date", "collection_end_date",
        )
    )

    # disease_factors
    disease_factors = (
        spark.table("cms_hcc__int_disease_factors")
        .select(
            F.col("person_id"), F.col("payer"),
            F.concat(F.col("hcc_description"), F.lit(" (HCC "), F.col("hcc_code"), F.lit(")")).alias("description"),
            F.col("coefficient"), F.col("factor_type"), F.col("model_version"),
            F.col("payment_year"), F.col("collection_start_date"), F.col("collection_end_date"),
        )
    )

    # enrollment_interactions
    enrollment_interactions = (
        spark.table("cms_hcc__int_enrollment_interaction_factors")
        .select("person_id", "payer", "description", "coefficient", "factor_type",
                "model_version", "payment_year", "collection_start_date", "collection_end_date")
    )

    # disabled_interactions
    disabled_interactions = (
        spark.table("cms_hcc__int_disabled_interaction_factors")
        .select("person_id", "payer", "description", "coefficient", "factor_type",
                "model_version", "payment_year", "collection_start_date", "collection_end_date")
    )

    # disease_interactions
    disease_interactions = (
        spark.table("cms_hcc__int_disease_interaction_factors")
        .select("person_id", "payer", "description", "coefficient", "factor_type",
                "model_version", "payment_year", "collection_start_date", "collection_end_date")
    )

    # hcc_counts
    hcc_counts = (
        spark.table("cms_hcc__int_hcc_count_factors")
        .select("person_id", "payer", "description", "coefficient", "factor_type",
                "model_version", "payment_year", "collection_start_date", "collection_end_date")
    )

    # union all factor types
    unioned = (
        demographic_factors
        .unionByName(disease_factors)
        .unionByName(enrollment_interactions)
        .unionByName(disabled_interactions)
        .unionByName(disease_interactions)
        .unionByName(hcc_counts)
    )

    # add_defaults
    u = unioned.alias("u")
    dd = demographic_defaults.alias("dd")

    add_defaults = (
        u.join(
            dd,
            (F.col("u.person_id") == F.col("dd.person_id"))
            & (F.col("u.payer") == F.col("dd.payer"))
            & (F.col("u.model_version") == F.col("dd.model_version"))
            & (F.col("u.payment_year") == F.col("dd.payment_year"))
            & (F.col("u.collection_end_date") == F.col("dd.collection_end_date")),
            "left",
        )
        .select(
            F.col("u.person_id"), F.col("u.payer"),
            F.col("dd.enrollment_status_default"),
            F.col("dd.enrollment_status"),
            F.col("dd.medicaid_dual_status_default"),
            F.col("dd.orec_default"),
            F.col("dd.institutional_status_default"),
            F.col("dd.risk_model_code"),
            F.col("u.description").alias("risk_factor_description"),
            F.col("u.coefficient"),
            F.col("u.factor_type"),
            F.col("u.model_version"),
            F.col("u.payment_year"),
            F.col("u.collection_start_date"),
            F.col("u.collection_end_date"),
        )
    )

    # add_data_types
    add_data_types = add_defaults.select(
        F.col("person_id").cast("string"),
        F.col("payer").cast("string"),
        F.col("enrollment_status").cast("string"),
        F.col("risk_model_code").cast("string"),
        F.col("enrollment_status_default").cast("boolean"),
        F.col("medicaid_dual_status_default").cast("boolean"),
        F.col("orec_default").cast("boolean"),
        F.col("institutional_status_default").cast("boolean"),
        F.col("factor_type").cast("string"),
        F.col("risk_factor_description").cast("string"),
        F.round(F.col("coefficient").cast("decimal(28,6)"), 3).alias("coefficient"),
        F.col("model_version").cast("string"),
        F.col("payment_year").cast("int"),
        F.col("collection_start_date").cast("date"),
        F.col("collection_end_date").cast("date"),
    )

    result = add_data_types.select(
        "person_id", "payer", "enrollment_status", "risk_model_code",
        "enrollment_status_default", "medicaid_dual_status_default",
        "orec_default", "institutional_status_default",
        "factor_type", "risk_factor_description", "coefficient",
        "model_version", "payment_year", "collection_start_date", "collection_end_date",
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
