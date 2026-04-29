import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    p = spark.table("pharmacy__stg_pharmacy_claim")
    r = spark.table("terminology__rxnorm_brand_generic")
    ga = spark.table("pharmacy__int_brand_with_generic_available")

    joined = (
        p.alias("p")
        .join(
            r.alias("r"),
            F.col("p.rxcui") == F.col("r.product_rxcui"),
            "left_outer"
        )
        .join(
            ga.alias("ga"),
            F.col("p.rxcui") == F.col("ga.brand_with_generic_available"),
            "left_outer"
        )
        .where(F.col("p.ndc_code").isNotNull())
    )

    joined = joined.withColumn(
        "generic_available",
        F.when(
            F.col("ga.brand_with_generic_available").isNotNull(),
            F.lit("brand_with_generic_available")
        ).otherwise(F.lit(None).cast("string"))
    )

    grouped = (
        joined
        .groupBy(
            F.col("generic_available"),
            F.col("r.brand_vs_generic").alias("brand_vs_generic"),
            F.col("p.ndc_code").alias("ndc_code"),
            F.col("p.rxcui").alias("rxcui"),
            F.col("p.ndc_description").alias("ndc_description"),
            F.col("p.data_source").alias("data_source"),
        )
        .agg(
            F.sum("paid_amount").alias("paid_amount"),
            F.countDistinct("claim_id").alias("claim_count"),
            F.sum("quantity").alias("total_units"),
        )
    )

    result = grouped.select(
        F.col("ndc_code"),
        F.col("ndc_description"),
        F.col("data_source"),
        F.col("rxcui"),
        F.col("brand_vs_generic"),
        F.col("generic_available"),
        F.col("paid_amount"),
        F.col("claim_count"),
        (F.col("paid_amount") / F.col("claim_count")).alias("cost_per_claim"),
        F.col("total_units"),
        F.when(
            (F.col("total_units") > 0) & (F.col("paid_amount") > 0),
            F.col("paid_amount") / F.col("total_units")
        ).otherwise(F.lit(None)).alias("cost_per_unit"),
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
