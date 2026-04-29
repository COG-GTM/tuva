import os
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', 'pyspark', 'utils'))


def run(spark: SparkSession) -> DataFrame:
    members = (
        spark.table("cms_hcc__int_members")
        .select(
            "person_id", "payer", "enrollment_status", "gender", "age_group",
            "medicaid_status", "dual_status", "orec", "originally_disabled_flag",
            "institutional_status", "institutional_snp_flag",
            "enrollment_status_default", "medicaid_dual_status_default",
            "orec_default", "institutional_status_default",
            "payment_year", "collection_start_date", "collection_end_date",
        )
    )

    # seed_demographic_factors
    demo_factors_raw = spark.table("cms_hcc__demographic_factors")

    seed_demographic_factors = (
        demo_factors_raw
        .where(F.col("plan_segment").isNull())
        .withColumn(
            "enrollment_status",
            F.when(F.col("institutional_status") == "Yes", F.lit("Institutional"))
            .otherwise(F.col("enrollment_status")),
        )
        .withColumn(
            "risk_model_code",
            F.when(F.col("enrollment_status") == "ESRD", F.lit("ESRD"))
            .when(F.col("enrollment_status") == "New", F.lit("E"))
            .when(F.col("institutional_status") == "Yes", F.lit("INS"))
            .when((F.col("medicaid_status") == "No") & (F.col("orec") == "Aged"), F.lit("CNA"))
            .when((F.col("medicaid_status") == "No") & (F.col("orec") == "Disabled"), F.lit("CND"))
            .when((F.col("dual_status") == "Full") & (F.col("orec") == "Aged"), F.lit("CFA"))
            .when((F.col("dual_status") == "Full") & (F.col("orec") == "Disabled"), F.lit("CFD"))
            .when((F.col("dual_status") == "Partial") & (F.col("orec") == "Aged"), F.lit("CPA"))
            .when((F.col("dual_status") == "Partial") & (F.col("orec") == "Disabled"), F.lit("CPD")),
        )
        .select("model_version", "factor_type", "enrollment_status", "gender",
                "age_group", "medicaid_status", "dual_status", "orec",
                "institutional_status", "coefficient", "risk_model_code")
    )

    # seed_snpne_factors
    seed_snpne_factors = (
        demo_factors_raw
        .where(F.col("plan_segment") == "C-SNP")
        .select(
            "model_version", "factor_type", "enrollment_status", "gender",
            "age_group", "medicaid_status", "dual_status", "orec",
            "institutional_status", "coefficient",
        )
        .withColumn("risk_model_code", F.lit("SNPNE").cast("string"))
    )

    m = members.alias("m")
    sd = seed_demographic_factors.alias("sd")
    sn = seed_snpne_factors.alias("sn")

    orec_derived = F.when(F.col("m.originally_disabled_flag") == "Yes", F.lit("Disabled")).otherwise(F.lit("Aged"))

    # new_enrollees
    new_enrollees = (
        m.join(
            sd,
            (F.col("m.enrollment_status") == F.col("sd.enrollment_status"))
            & (F.col("m.gender") == F.col("sd.gender"))
            & (F.col("m.age_group") == F.col("sd.age_group"))
            & (F.col("m.medicaid_status") == F.col("sd.medicaid_status"))
            & (orec_derived == F.col("sd.orec")),
            "inner",
        )
        .where(
            (F.col("m.enrollment_status") == "New")
            & (F.coalesce(F.col("m.institutional_snp_flag"), F.lit(0)) != 1)
        )
        .select(
            F.col("m.person_id"), F.col("m.payer"), F.col("m.enrollment_status"),
            F.col("m.gender"), F.col("m.age_group"), F.col("m.medicaid_status"),
            F.col("m.dual_status"), F.col("m.orec"), F.col("m.originally_disabled_flag"),
            F.col("m.institutional_status"),
            F.col("m.enrollment_status_default"), F.col("m.medicaid_dual_status_default"),
            F.col("m.orec_default"), F.col("m.institutional_status_default"),
            F.col("m.payment_year"), F.col("m.collection_start_date"), F.col("m.collection_end_date"),
            F.col("sd.model_version"), F.col("sd.factor_type"),
            F.col("sd.coefficient"), F.col("sd.risk_model_code"),
        )
    )

    # snpne_enrollees
    snpne_enrollees = (
        m.join(
            sn,
            (F.col("m.gender") == F.col("sn.gender"))
            & (F.col("m.age_group") == F.col("sn.age_group"))
            & (F.col("m.medicaid_status") == F.col("sn.medicaid_status"))
            & (orec_derived == F.col("sn.orec")),
            "inner",
        )
        .where(
            (F.col("m.enrollment_status") == "New")
            & (F.col("m.institutional_snp_flag") == 1)
        )
        .select(
            F.col("m.person_id"), F.col("m.payer"), F.col("m.enrollment_status"),
            F.col("m.gender"), F.col("m.age_group"), F.col("m.medicaid_status"),
            F.col("m.dual_status"), F.col("m.orec"), F.col("m.originally_disabled_flag"),
            F.col("m.institutional_status"),
            F.col("m.enrollment_status_default"), F.col("m.medicaid_dual_status_default"),
            F.col("m.orec_default"), F.col("m.institutional_status_default"),
            F.col("m.payment_year"), F.col("m.collection_start_date"), F.col("m.collection_end_date"),
            F.col("sn.model_version"), F.col("sn.factor_type"),
            F.col("sn.coefficient"), F.col("sn.risk_model_code"),
        )
    )

    # continuing_enrollees
    continuing_enrollees = (
        m.join(
            sd,
            (F.col("m.enrollment_status") == F.col("sd.enrollment_status"))
            & (F.col("m.gender") == F.col("sd.gender"))
            & (F.col("m.age_group") == F.col("sd.age_group"))
            & (F.col("m.medicaid_status") == F.col("sd.medicaid_status"))
            & (F.col("m.dual_status") == F.col("sd.dual_status"))
            & (F.col("m.orec") == F.col("sd.orec"))
            & (F.col("m.institutional_status") == F.col("sd.institutional_status")),
            "inner",
        )
        .where(F.col("m.enrollment_status") == "Continuing")
        .select(
            F.col("m.person_id"), F.col("m.payer"), F.col("m.enrollment_status"),
            F.col("m.gender"), F.col("m.age_group"), F.col("m.medicaid_status"),
            F.col("m.dual_status"), F.col("m.orec"), F.col("m.originally_disabled_flag"),
            F.col("m.institutional_status"),
            F.col("m.enrollment_status_default"), F.col("m.medicaid_dual_status_default"),
            F.col("m.orec_default"), F.col("m.institutional_status_default"),
            F.col("m.payment_year"), F.col("m.collection_start_date"), F.col("m.collection_end_date"),
            F.col("sd.model_version"), F.col("sd.factor_type"),
            F.col("sd.coefficient"), F.col("sd.risk_model_code"),
        )
    )

    # institutional_enrollees
    institutional_enrollees = (
        m.join(
            sd,
            (F.col("m.enrollment_status") == F.col("sd.enrollment_status"))
            & (F.col("m.gender") == F.col("sd.gender"))
            & (F.col("m.age_group") == F.col("sd.age_group")),
            "inner",
        )
        .where(F.col("m.enrollment_status") == "Institutional")
        .select(
            F.col("m.person_id"), F.col("m.payer"), F.col("m.enrollment_status"),
            F.col("m.gender"), F.col("m.age_group"), F.col("m.medicaid_status"),
            F.col("m.dual_status"), F.col("m.orec"), F.col("m.originally_disabled_flag"),
            F.col("m.institutional_status"),
            F.col("m.enrollment_status_default"), F.col("m.medicaid_dual_status_default"),
            F.col("m.orec_default"), F.col("m.institutional_status_default"),
            F.col("m.payment_year"), F.col("m.collection_start_date"), F.col("m.collection_end_date"),
            F.col("sd.model_version"), F.col("sd.factor_type"),
            F.col("sd.coefficient"), F.col("sd.risk_model_code"),
        )
    )

    # union all
    unioned = (
        new_enrollees
        .unionByName(snpne_enrollees)
        .unionByName(continuing_enrollees)
        .unionByName(institutional_enrollees)
    )

    # add_data_types
    add_data_types = unioned.select(
        F.col("person_id").cast("string"),
        F.col("payer").cast("string"),
        F.col("enrollment_status").cast("string"),
        F.col("gender").cast("string"),
        F.col("age_group").cast("string"),
        F.col("medicaid_status").cast("string"),
        F.col("dual_status").cast("string"),
        F.col("orec").cast("string"),
        F.col("institutional_status").cast("string"),
        F.col("originally_disabled_flag").cast("string"),
        F.col("enrollment_status_default").cast("boolean"),
        F.col("medicaid_dual_status_default").cast("boolean"),
        F.col("orec_default").cast("boolean"),
        F.col("institutional_status_default").cast("boolean"),
        F.round(F.col("coefficient").cast("decimal(28,6)"), 3).alias("coefficient"),
        F.col("risk_model_code").cast("string"),
        F.col("factor_type").cast("string"),
        F.col("model_version").cast("string"),
        F.col("payment_year").cast("int"),
        F.col("collection_start_date").cast("date"),
        F.col("collection_end_date").cast("date"),
    )

    # filter: 100% v28 starting in 2026
    result = (
        add_data_types
        .where(
            F.when(
                (F.col("payment_year") >= 2026) & (F.col("model_version") == "CMS-HCC-V24"),
                F.lit(False),
            ).otherwise(F.lit(True))
        )
        .select(
            "person_id", "payer", "enrollment_status", "gender", "age_group",
            "medicaid_status", "dual_status", "orec", "institutional_status",
            "originally_disabled_flag", "enrollment_status_default",
            "medicaid_dual_status_default", "orec_default", "institutional_status_default",
            "coefficient", "factor_type", "risk_model_code", "model_version",
            "payment_year", "collection_start_date", "collection_end_date",
            F.current_timestamp().alias("tuva_last_run"),
        )
    )

    return result
