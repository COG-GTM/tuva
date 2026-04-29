import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    medical = spark.table("financial_pmpm__stg_medical_claim")

    claims_with_service_categories = medical.select(
        F.col("person_id"),
        F.col("member_id"),
        F.col("payer"),
        F.col("plan"),
        F.col("service_category_1"),
        F.col("service_category_2"),
        F.coalesce(F.col("claim_start_date"), F.col("claim_end_date")).alias("claim_date"),
        F.col("paid_amount"),
        F.col("allowed_amount"),
        F.col("data_source"),
    )

    medical_claims_year_month = claims_with_service_categories.select(
        F.col("person_id"),
        F.col("member_id"),
        F.col("payer"),
        F.col("plan"),
        F.col("service_category_1"),
        F.col("service_category_2"),
        F.concat(
            F.year(F.col("claim_date")).cast("string"),
            F.lpad(F.month(F.col("claim_date")).cast("string"), 2, "0"),
        ).alias("year_month"),
        F.col("paid_amount"),
        F.col("allowed_amount"),
        F.col("data_source"),
    )

    rx = spark.table("financial_pmpm__stg_pharmacy_claim")

    rx_claims = rx.select(
        F.col("person_id"),
        F.col("member_id"),
        F.col("payer"),
        F.col("plan"),
        F.lit("pharmacy").alias("service_category_1"),
        F.lit(None).cast("string").alias("service_category_2"),
        F.coalesce(F.col("dispensing_date"), F.col("paid_date")).alias("claim_date"),
        F.col("paid_amount"),
        F.col("allowed_amount"),
        F.col("data_source"),
    )

    rx_claims_year_month = rx_claims.select(
        F.col("person_id"),
        F.col("member_id"),
        F.col("payer"),
        F.col("plan"),
        F.col("service_category_1"),
        F.col("service_category_2"),
        F.concat(
            F.year(F.col("claim_date")).cast("string"),
            F.lpad(F.month(F.col("claim_date")).cast("string"), 2, "0"),
        ).alias("year_month"),
        F.col("paid_amount"),
        F.col("allowed_amount"),
        F.col("data_source"),
    )

    combine_medical_and_rx = medical_claims_year_month.unionAll(rx_claims_year_month)

    result = combine_medical_and_rx.groupBy(
        "person_id",
        "member_id",
        "year_month",
        "payer",
        "plan",
        "service_category_1",
        "service_category_2",
        "data_source",
    ).agg(
        F.sum("paid_amount").alias("total_paid"),
        F.sum("allowed_amount").alias("total_allowed"),
    )

    result = result.select(
        "person_id",
        "member_id",
        "year_month",
        "payer",
        "plan",
        "service_category_1",
        "service_category_2",
        "total_paid",
        "total_allowed",
        "data_source",
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
