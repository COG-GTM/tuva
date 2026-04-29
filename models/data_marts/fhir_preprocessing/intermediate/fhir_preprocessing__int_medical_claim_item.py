import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    adjudication = spark.table("fhir_preprocessing__int_medical_claim_item_adjudication").select(
        "claim_id", "claim_line_number", "eob_item_adjudication_list",
    )
    modifier = spark.table("fhir_preprocessing__int_medical_claim_item_modifier").select(
        "claim_id", "claim_line_number", "eob_item_modifier_list",
    )
    medical_claim = spark.table("fhir_preprocessing__stg_core__medical_claim")

    joined = (
        medical_claim.alias("mc")
        .join(
            adjudication.alias("adj"),
            (F.col("mc.claim_id") == F.col("adj.claim_id"))
            & (F.col("mc.claim_line_number") == F.col("adj.claim_line_number")),
            how="left",
        )
        .join(
            modifier.alias("mod"),
            (F.col("mc.claim_id") == F.col("mod.claim_id"))
            & (F.col("mc.claim_line_number") == F.col("mod.claim_line_number")),
            how="left",
        )
        .select(
            F.col("mc.claim_id").alias("claim_id"),
            F.abs(F.col("mc.claim_line_number")).alias("eob_item_sequence"),
            F.col("mc.revenue_center_code").alias("eob_item_revenue_code"),
            F.col("mc.revenue_center_description").alias("eob_item_revenue_display"),
            F.when(F.col("mc.hcpcs_code").isNotNull(), F.lit("CPT")).alias("eob_item_product_or_service_system"),
            F.coalesce(F.col("mc.hcpcs_code"), F.lit("00000")).alias("eob_item_product_or_service_code"),
            F.coalesce(
                F.col("mc.claim_line_start_date"),
                F.col("mc.claim_start_date"),
            ).cast("string").alias("eob_item_serviced_date"),
            F.when(F.col("mc.place_of_service_code").isNotNull(), F.lit("POS")).alias("eob_item_location_system"),
            F.col("mc.place_of_service_code").alias("eob_item_location_code"),
            F.col("mc.place_of_service_description").alias("eob_item_location_display"),
            F.col("adj.eob_item_adjudication_list").alias("eob_item_adjudication_list"),
            F.col("mod.eob_item_modifier_list").alias("eob_item_modifier_list"),
        )
    )

    return create_json_object(
        df=joined,
        group_cols="claim_id",
        obj_col="eob_item_list",
        obj_fields=[
            "eob_item_sequence",
            "eob_item_revenue_code",
            "eob_item_revenue_display",
            "eob_item_product_or_service_system",
            "eob_item_product_or_service_code",
            "eob_item_serviced_date",
            "eob_item_location_system",
            "eob_item_location_code",
            "eob_item_location_display",
            "eob_item_adjudication_list",
            "eob_item_modifier_list",
        ],
    )
