import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    pc = spark.table("pharmacy__stg_pharmacy_claim")
    cc = spark.table("pharmacy__int_claims_current_cost")
    gc = spark.table("pharmacy__int_generic_cost")

    cpu = (
        pc.select(
            F.col("claim_id"),
            F.col("claim_line_number"),
            F.col("data_source"),
            F.when(
                F.col("quantity") > 0,
                F.col("paid_amount") / F.col("quantity")
            ).otherwise(F.lit(None)).alias("brand_cost_per_unit"),
        )
    )

    result = (
        pc.alias("pc")
        .join(
            cpu.alias("cpu"),
            (F.col("pc.claim_id") == F.col("cpu.claim_id"))
            & (F.col("pc.claim_line_number") == F.col("cpu.claim_line_number"))
            & (F.col("pc.data_source") == F.col("cpu.data_source")),
            "inner"
        )
        .join(
            cc.alias("cc"),
            (F.col("cc.ndc_code") == F.col("pc.ndc_code"))
            & (F.col("cc.data_source") == F.col("pc.data_source")),
            "inner"
        )
        .join(
            gc.alias("gc"),
            (F.col("cc.rxcui") == F.col("gc.brand_rxcui"))
            & (F.col("gc.data_source") == F.col("cc.data_source")),
            "inner"
        )
        .where(F.col("prescribed_atleast_one_generic_history") == 1)
        .select(
            F.col("pc.data_source").alias("data_source"),
            F.col("pc.claim_id").alias("claim_id"),
            F.col("pc.claim_line_number").alias("claim_line_number"),
            F.col("cc.ndc_code").alias("ndc_code"),
            F.col("cc.ndc_description").alias("ndc_description"),
            F.col("cc.rxcui").alias("brand_rxcui"),
            F.col("cc.brand_vs_generic").alias("brand_vs_generic"),
            F.col("cc.generic_available").alias("generic_available"),
            F.col("pc.paid_amount").alias("paid_amount"),
            F.col("pc.quantity").alias("total_units"),
            F.col("cpu.brand_cost_per_unit").alias("brand_cost_per_unit"),
            F.col("gc.generic_average_cost_per_unit").alias("generic_average_cost_per_unit"),
            (F.col("cpu.brand_cost_per_unit") - F.col("gc.generic_average_cost_per_unit")).alias("brand_less_generic_cost_per_unit"),
            F.when(
                (F.col("cpu.brand_cost_per_unit") - F.col("gc.generic_average_cost_per_unit")) > 0,
                (F.col("cpu.brand_cost_per_unit") - F.col("gc.generic_average_cost_per_unit")) * F.col("pc.quantity")
            ).otherwise(F.lit(0)).alias("generic_available_total_opportunity"),
            F.current_timestamp().alias("tuva_last_run"),
        )
    )

    return result
