import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    pharmacy_claim = spark.table("fhir_preprocessing__stg_core__pharmacy_claim")

    pc_header = pharmacy_claim.filter(F.col("claim_line_number") == 1)

    days_supply = (
        pc_header.filter(F.col("days_supply").isNotNull())
        .select(
            F.col("claim_id"),
            F.lit("dayssupply").alias("eob_supporting_info_category_code"),
            F.col("days_supply").cast("string").alias("eob_supporting_info_value_quantity"),
            F.lit(None).cast("string").alias("eob_supporting_info_code"),
            F.lit(None).cast("string").alias("eob_supporting_info_system"),
        )
    )

    refill = (
        pc_header.filter(F.col("refills").isNotNull())
        .select(
            F.col("claim_id"),
            F.lit("refillnum").alias("eob_supporting_info_category_code"),
            F.col("refills").cast("string").alias("eob_supporting_info_value_quantity"),
            F.lit(None).cast("string").alias("eob_supporting_info_code"),
            F.lit(None).cast("string").alias("eob_supporting_info_system"),
        )
    )

    daw = pc_header.select(
        F.col("claim_id"),
        F.lit("dawcode").alias("eob_supporting_info_category_code"),
        F.lit(None).cast("string").alias("eob_supporting_info_value_quantity"),
        F.lit("0").alias("eob_supporting_info_code"),
        F.lit("DAW").alias("eob_supporting_info_system"),
    )

    unioned = days_supply.unionByName(refill).unionByName(daw)

    add_sequence = unioned.select(
        F.col("claim_id"),
        F.col("eob_supporting_info_category_code"),
        F.col("eob_supporting_info_value_quantity").cast("decimal(28,6)").alias("eob_supporting_info_value_quantity"),
        F.col("eob_supporting_info_code"),
        F.col("eob_supporting_info_system"),
        F.row_number().over(
            Window.partitionBy("claim_id").orderBy("eob_supporting_info_category_code")
        ).alias("eob_supporting_info_sequence"),
    )

    return create_json_object(
        df=add_sequence,
        group_cols="claim_id",
        obj_col="eob_supporting_info_list",
        obj_fields=[
            "eob_supporting_info_sequence",
            "eob_supporting_info_category_code",
            "eob_supporting_info_value_quantity",
            "eob_supporting_info_code",
            "eob_supporting_info_system",
        ],
    )
