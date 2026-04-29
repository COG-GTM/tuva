import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    medical_claim = spark.table("fhir_preprocessing__stg_core__medical_claim")

    adjudication_amount = medical_claim.select(
        F.col("claim_id"),
        F.col("claim_line_number"),
        F.lit("ADJ_TYPE").alias("eob_item_adjudication_category_system"),
        F.lit("benefit").alias("eob_item_adjudication_category_code"),
        F.lit("USD").alias("eob_item_adjudication_amount_currency"),
        F.when(F.col("paid_amount") <= 0, F.lit(0.01).cast("decimal(28,6)"))
         .otherwise(F.col("paid_amount").cast("decimal(28,6)"))
         .alias("eob_item_adjudication_amount_value"),
    )

    adjudication_status = medical_claim.select(
        F.col("claim_id"),
        F.col("claim_line_number"),
        F.lit("ADJ_STATUS").alias("eob_item_adjudication_category_system"),
        F.when(F.col("in_network_flag") == 1, F.lit("innetwork"))
         .when(F.col("in_network_flag") == 0, F.lit("outofnetwork"))
         .otherwise(F.lit("other"))
         .alias("eob_item_adjudication_category_code"),
        F.lit("USD").alias("eob_item_adjudication_amount_currency"),
        F.when(F.col("paid_amount") <= 0, F.lit(0.01).cast("decimal(28,6)"))
         .otherwise(F.col("paid_amount").cast("decimal(28,6)"))
         .alias("eob_item_adjudication_amount_value"),
    )

    unioned = adjudication_amount.unionByName(adjudication_status)

    return create_json_object(
        df=unioned,
        group_cols=["claim_id", "claim_line_number"],
        obj_col="eob_item_adjudication_list",
        obj_fields=[
            "eob_item_adjudication_category_system",
            "eob_item_adjudication_category_code",
            "eob_item_adjudication_amount_currency",
            "eob_item_adjudication_amount_value",
        ],
    )
