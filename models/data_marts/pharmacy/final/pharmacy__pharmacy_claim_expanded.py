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
    opp = spark.table("pharmacy__brand_generic_opportunity")

    all_drugs = (
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
        .join(
            opp.alias("opp"),
            (F.col("p.claim_id") == F.col("opp.claim_id"))
            & (F.col("p.claim_line_number") == F.col("opp.claim_line_number"))
            & (F.col("p.data_source") == F.col("opp.data_source")),
            "left_outer"
        )
        .select(
            F.col("p.data_source").alias("data_source"),
            F.col("p.claim_id").alias("claim_id"),
            F.col("p.claim_line_number").alias("claim_line_number"),
            F.col("p.person_id").alias("person_id"),
            F.col("p.member_id").alias("member_id"),
            F.col("p.prescribing_provider_id").alias("prescribing_provider_id"),
            F.col("p.dispensing_provider_id").alias("dispensing_provider_id"),
            F.col("p.dispensing_date").alias("dispensing_date"),
            F.col("p.ndc_code").alias("ndc_code"),
            F.col("p.ndc_description").alias("ndc_description"),
            F.col("p.quantity").alias("quantity"),
            F.col("p.days_supply").alias("days_supply"),
            F.col("p.refills").alias("refills"),
            F.col("p.paid_date").alias("paid_date"),
            F.col("p.paid_amount").alias("paid_amount"),
            F.col("p.allowed_amount").alias("allowed_amount"),
            F.col("p.rxcui").alias("rxcui"),
            F.col("r.product_name").alias("product_name"),
            F.col("r.product_tty").alias("product_tty"),
            F.col("r.brand_vs_generic").alias("brand_vs_generic"),
            F.col("r.brand_name").alias("brand_name"),
            F.col("r.clinical_product_rxcui").alias("generic_rxcui"),
            F.col("r.clinical_product_name").alias("generic_rxcui_description"),
            F.col("r.clinical_product_tty").alias("generic_tty"),
            F.col("r.ingredient_name").alias("ingredient_name"),
            F.col("r.dose_form_name").alias("dose_form_name"),
            F.when(
                F.col("ga.brand_with_generic_available").isNotNull(),
                F.lit("brand_with_generic_available")
            ).otherwise(F.col("r.brand_vs_generic")).alias("generic_available"),
            F.col("opp.brand_cost_per_unit").alias("brand_cost_per_unit"),
            F.col("opp.generic_average_cost_per_unit").alias("generic_average_cost_per_unit"),
            F.col("opp.brand_less_generic_cost_per_unit").alias("brand_less_generic_cost_per_unit"),
            F.col("opp.generic_available_total_opportunity").alias("generic_available_total_opportunity"),
            F.current_timestamp().alias("tuva_last_run"),
        )
    )

    generic_available = (
        all_drugs
        .where(F.col("generic_available") == F.lit("brand_with_generic_available"))
        .withColumn(
            "generic_available_sk",
            F.row_number().over(
                Window.orderBy("ndc_code", "data_source")
            )
        )
        .select(
            F.col("claim_id"),
            F.col("claim_line_number"),
            F.col("data_source"),
            F.col("generic_available_sk"),
        )
    )

    result = (
        all_drugs.alias("a")
        .join(
            generic_available.alias("g"),
            (F.col("a.claim_id") == F.col("g.claim_id"))
            & (F.col("a.claim_line_number") == F.col("g.claim_line_number"))
            & (F.col("a.data_source") == F.col("g.data_source")),
            "left_outer"
        )
        .select(
            F.col("a.data_source").alias("data_source"),
            F.col("a.claim_id").alias("claim_id"),
            F.col("a.claim_line_number").alias("claim_line_number"),
            F.col("a.person_id").alias("person_id"),
            F.col("a.prescribing_provider_id").alias("prescribing_provider_id"),
            F.col("a.dispensing_provider_id").alias("dispensing_provider_id"),
            F.col("a.dispensing_date").alias("dispensing_date"),
            F.col("a.ndc_code").alias("ndc_code"),
            F.col("a.ndc_description").alias("ndc_description"),
            F.col("a.quantity").alias("quantity"),
            F.col("a.days_supply").alias("days_supply"),
            F.col("a.refills").alias("refills"),
            F.col("a.paid_date").alias("paid_date"),
            F.col("a.paid_amount").alias("paid_amount"),
            F.col("a.allowed_amount").alias("allowed_amount"),
            F.col("a.rxcui").alias("rxcui"),
            F.col("a.product_name").alias("product_name"),
            F.col("a.product_tty").alias("product_tty"),
            F.col("a.brand_vs_generic").alias("brand_vs_generic"),
            F.col("a.brand_name").alias("brand_name"),
            F.col("a.generic_rxcui").alias("generic_rxcui"),
            F.col("a.generic_rxcui_description").alias("generic_rxcui_description"),
            F.col("a.generic_tty").alias("generic_tty"),
            F.col("a.ingredient_name").alias("ingredient_name"),
            F.col("a.dose_form_name").alias("dose_form_name"),
            F.col("a.generic_available").alias("generic_available"),
            F.col("a.brand_cost_per_unit").alias("brand_cost_per_unit"),
            F.col("a.generic_average_cost_per_unit").alias("generic_average_cost_per_unit"),
            F.col("a.brand_less_generic_cost_per_unit").alias("brand_less_generic_cost_per_unit"),
            F.col("a.generic_available_total_opportunity").alias("generic_available_total_opportunity"),
            F.col("g.generic_available_sk").alias("generic_available_sk"),
        )
    )

    return result
