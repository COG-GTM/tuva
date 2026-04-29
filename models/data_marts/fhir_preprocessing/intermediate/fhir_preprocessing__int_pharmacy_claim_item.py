import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    adjudication = spark.table("fhir_preprocessing__int_pharmacy_claim_item_adjudication").select(
        "claim_id", "claim_line_number", "eob_item_adjudication_list",
    )
    pharmacy_claim = spark.table("fhir_preprocessing__stg_core__pharmacy_claim")

    joined = (
        pharmacy_claim.alias("pc")
        .filter(F.col("pc.ndc_code").isNotNull())
        .join(
            adjudication.alias("adj"),
            (F.col("pc.claim_id") == F.col("adj.claim_id"))
            & (F.col("pc.claim_line_number") == F.col("adj.claim_line_number")),
            how="left",
        )
        .select(
            F.col("pc.claim_id").alias("claim_id"),
            F.abs(F.col("pc.claim_line_number")).alias("eob_item_sequence"),
            F.lit("NDC").cast("string").alias("eob_item_product_or_service_system"),
            F.col("pc.ndc_code").alias("eob_item_product_or_service_code"),
            F.coalesce(
                F.col("pc.dispensing_date"),
                F.col("pc.paid_date"),
            ).cast("string").alias("eob_item_serviced_date"),
            F.col("adj.eob_item_adjudication_list").alias("eob_item_adjudication_list"),
        )
    )

    return create_json_object(
        df=joined,
        group_cols="claim_id",
        obj_col="eob_item_list",
        obj_fields=[
            "eob_item_sequence",
            "eob_item_product_or_service_system",
            "eob_item_product_or_service_code",
            "eob_item_serviced_date",
            "eob_item_adjudication_list",
        ],
    )
