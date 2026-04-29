import os
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import substring_col


def run(spark: SparkSession) -> DataFrame:
    medical_claims = (
        spark.table("cms_hcc__stg_core__medical_claim")
        .select(
            "claim_id", "claim_line_number", "claim_type", "payer",
            "person_id", "rendering_npi", "claim_start_date", "claim_end_date",
            "bill_type_code", "hcpcs_code",
        )
    )

    conditions = (
        spark.table("cms_hcc__stg_core__condition")
        .where(F.col("code_type") == "icd-10-cm")
        .select("claim_id", "payer", "person_id", "code")
    )

    # cpt_hcpcs_list: current + next year mapping
    cpt_hcpcs_base = spark.table("cms_hcc__cpt_hcpcs")
    max_py = cpt_hcpcs_base.agg(F.max("payment_year").alias("max_py")).collect()[0]["max_py"]

    cpt_current = cpt_hcpcs_base.select(
        F.col("payment_year").alias("collection_year"),
        F.col("hcpcs_cpt_code"),
    )
    cpt_next = (
        cpt_hcpcs_base
        .where(F.col("payment_year") == max_py)
        .select(
            (F.col("payment_year") + 1).alias("collection_year"),
            F.col("hcpcs_cpt_code"),
        )
    )
    cpt_hcpcs_list = cpt_current.unionByName(cpt_next)

    # yearly_collection_dates
    monthly_dates = spark.table("cms_hcc__int_monthly_collection_dates")
    yearly_collection_dates = (
        monthly_dates
        .groupBy("payment_year")
        .agg(
            F.min("collection_start_date").alias("collection_start_date"),
            F.max("collection_end_date").alias("collection_end_date"),
        )
    )

    mc = medical_claims.alias("mc")
    cpt = cpt_hcpcs_list.alias("cpt")
    yd = yearly_collection_dates.alias("yd")

    condition_date_expr = F.coalesce(F.col("mc.claim_end_date"), F.col("mc.claim_start_date"))

    # professional_claims
    professional_claims = (
        mc
        .join(cpt, F.col("mc.hcpcs_code") == F.col("cpt.hcpcs_cpt_code"), "inner")
        .join(
            yd,
            (condition_date_expr.between(F.col("yd.collection_start_date"), F.col("yd.collection_end_date")))
            & ((F.col("cpt.collection_year") + 1) == F.col("yd.payment_year")),
            "inner",
        )
        .where(F.col("mc.claim_type") == "professional")
        .select(
            F.col("mc.claim_id"), F.col("mc.claim_line_number"), F.col("mc.claim_type"),
            F.col("mc.payer"), F.col("mc.person_id"), F.col("mc.claim_start_date"),
            F.col("mc.claim_end_date"), F.col("mc.bill_type_code"), F.col("mc.hcpcs_code"),
            F.col("yd.payment_year"),
            condition_date_expr.alias("condition_date"),
        )
    )

    # inpatient_claims
    inpatient_claims = (
        mc
        .join(
            yd,
            condition_date_expr.between(F.col("yd.collection_start_date"), F.col("yd.collection_end_date")),
            "inner",
        )
        .where(
            (F.col("mc.claim_type") == "institutional")
            & (F.substring(F.col("mc.bill_type_code"), 1, 2).isin("11", "41"))
        )
        .select(
            F.col("mc.claim_id"), F.col("mc.claim_line_number"), F.col("mc.claim_type"),
            F.col("mc.payer"), F.col("mc.person_id"), F.col("mc.claim_start_date"),
            F.col("mc.claim_end_date"), F.col("mc.bill_type_code"), F.col("mc.hcpcs_code"),
            F.col("yd.payment_year"),
            condition_date_expr.alias("condition_date"),
        )
    )

    # outpatient_claims
    outpatient_claims = (
        mc
        .join(cpt, F.col("mc.hcpcs_code") == F.col("cpt.hcpcs_cpt_code"), "inner")
        .join(
            yd,
            (condition_date_expr.between(F.col("yd.collection_start_date"), F.col("yd.collection_end_date")))
            & ((F.col("cpt.collection_year") + 1) == F.col("yd.payment_year")),
            "inner",
        )
        .where(
            (F.col("mc.claim_type") == "institutional")
            & (F.substring(F.col("mc.bill_type_code"), 1, 2).isin("12", "13", "43", "71", "73", "76", "77", "85"))
        )
        .select(
            F.col("mc.claim_id"), F.col("mc.claim_line_number"), F.col("mc.claim_type"),
            F.col("mc.payer"), F.col("mc.person_id"), F.col("mc.claim_start_date"),
            F.col("mc.claim_end_date"), F.col("mc.bill_type_code"), F.col("mc.hcpcs_code"),
            F.col("yd.payment_year"),
            condition_date_expr.alias("condition_date"),
        )
    )

    # eligible_claims
    eligible_claims = professional_claims.unionByName(inpatient_claims).unionByName(outpatient_claims)

    # eligible_conditions
    ec = eligible_claims.alias("ec")
    cond = conditions.alias("cond")
    eligible_conditions = (
        ec.join(
            cond,
            (F.col("ec.claim_id") == F.col("cond.claim_id"))
            & (F.col("ec.person_id") == F.col("cond.person_id"))
            & (F.col("ec.payer") == F.col("cond.payer")),
            "inner",
        )
        .select(
            F.col("ec.claim_id"), F.col("ec.claim_line_number"), F.col("ec.payer"),
            F.col("ec.person_id"), F.col("ec.payment_year"), F.col("ec.condition_date"),
            F.col("cond.code"),
        )
        .distinct()
    )

    # eligible_conditions_monthly
    md = monthly_dates.alias("dates")
    ecm = eligible_conditions.alias("ec2")
    eligible_conditions_monthly = (
        ecm.join(
            md,
            (F.col("ec2.payment_year") == F.col("dates.payment_year"))
            & (F.col("ec2.condition_date") <= F.col("dates.collection_end_date")),
            "inner",
        )
        .select(
            F.col("ec2.claim_id"), F.col("ec2.claim_line_number"), F.col("ec2.payer"),
            F.col("ec2.person_id"), F.col("ec2.payment_year"),
            F.col("dates.collection_start_date"), F.col("dates.collection_end_date"),
            F.col("ec2.code"),
        )
        .distinct()
    )

    # add_data_types
    add_data_types = eligible_conditions_monthly.select(
        F.col("claim_id").cast("string"),
        F.col("claim_line_number").cast("string"),
        F.col("payer").cast("string"),
        F.col("person_id").cast("string"),
        F.col("code").cast("string").alias("condition_code"),
        F.col("payment_year").cast("int"),
        F.col("collection_start_date").cast("date"),
        F.col("collection_end_date").cast("date"),
    ).distinct()

    result = add_data_types.select(
        "person_id", "claim_id", "claim_line_number", "payer",
        "condition_code", "payment_year", "collection_start_date", "collection_end_date",
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
