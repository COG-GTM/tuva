import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    e = spark.table("pharmacy__pharmacy_claim_expanded")
    p = spark.table("pharmacy__stg_pharmacy_claim")
    b = spark.table("pharmacy__int_brand_with_generic_available")
    ga = spark.table("pharmacy__rxnorm_generic_available")
    n = spark.table("terminology__ndc")
    gc = spark.table("pharmacy__int_claims_current_cost")

    generic_sk = (
        e.where(F.col("generic_available_sk").isNotNull())
        .select(
            F.col("generic_available_sk"),
            F.col("claim_id"),
            F.col("claim_line_number"),
            F.col("data_source"),
        )
    )

    result = (
        p.alias("p")
        .join(
            generic_sk.alias("sk"),
            (F.col("p.claim_id") == F.col("sk.claim_id"))
            & (F.col("p.claim_line_number") == F.col("sk.claim_line_number"))
            & (F.col("p.data_source") == F.col("sk.data_source")),
            "inner"
        )
        .join(
            b.alias("b"),
            F.col("p.rxcui") == F.col("b.brand_with_generic_available"),
            "inner"
        )
        .join(
            ga.alias("ga"),
            (F.col("p.rxcui") == F.col("ga.product_rxcui"))
            & (F.col("ga.ndc_product_tty").isin("SCD", "GPCK")),
            "inner"
        )
        .join(
            n.alias("n"),
            F.col("ga.ndc") == F.col("n.ndc"),
            "left_outer"
        )
        .join(
            gc.alias("gc"),
            (F.col("ga.ndc") == F.col("gc.ndc_code"))
            & (F.col("gc.brand_vs_generic") == F.lit("generic"))
            & (F.col("gc.data_source") == F.col("p.data_source")),
            "left_outer"
        )
        .where(
            F.col("ga.product_startmarketingdate").isNotNull()
            & (F.col("ga.product_startmarketingdate").cast("date") <= F.current_date())
        )
        .select(
            F.col("sk.generic_available_sk").alias("generic_available_sk"),
            F.col("p.data_source").alias("data_source"),
            F.col("p.ndc_code").alias("brand_ndc_code"),
            F.col("p.ndc_description").alias("brand_ndc_description"),
            F.col("p.rxcui").alias("brand_rxcui"),
            F.col("p.paid_amount").alias("brand_paid_amount"),
            F.col("p.quantity").alias("brand_units"),
            F.when(
                F.col("p.quantity") == 0,
                F.lit(0)
            ).otherwise(
                F.col("p.paid_amount") / F.col("p.quantity")
            ).alias("brand_paid_per_unit"),
            F.col("ga.ndc").alias("generic_ndc"),
            F.col("n.fda_description").alias("generic_ndc_description"),
            F.when(
                F.col("gc.ndc_code").isNotNull(),
                F.lit(1)
            ).otherwise(F.lit(0)).alias("generic_prescribed_history"),
            F.col("gc.cost_per_unit").alias("generic_cost_per_unit"),
            (F.col("gc.cost_per_unit") * F.col("p.quantity")).alias("generic_cost_at_units"),
            F.current_timestamp().alias("tuva_last_run"),
        )
    )

    return result
