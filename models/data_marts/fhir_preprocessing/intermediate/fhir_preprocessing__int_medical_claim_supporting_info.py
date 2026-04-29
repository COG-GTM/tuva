import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    medical_claim = spark.table("fhir_preprocessing__stg_core__medical_claim")

    mc_header = medical_claim.filter(F.col("claim_line_number") == 1)

    admission_period = (
        mc_header.filter(F.col("admission_date").isNotNull())
        .select(
            F.col("claim_id"),
            F.lit("admissionperiod").alias("eob_supporting_info_category_code"),
            F.lit(None).cast("string").alias("eob_supporting_info_code"),
            F.lit(None).cast("string").alias("eob_supporting_info_system"),
            F.col("admission_date").alias("eob_supporting_info_timing_start"),
            F.col("discharge_date").alias("eob_supporting_info_timing_end"),
        )
    )

    type_of_bill = (
        mc_header.filter(F.col("bill_type_code").isNotNull())
        .select(
            F.col("claim_id"),
            F.lit("typeofbill").alias("eob_supporting_info_category_code"),
            F.col("bill_type_code").alias("eob_supporting_info_code"),
            F.lit("UBTOB").alias("eob_supporting_info_system"),
            F.lit(None).cast("date").alias("eob_supporting_info_timing_start"),
            F.lit(None).cast("date").alias("eob_supporting_info_timing_end"),
        )
    )

    unioned = admission_period.unionByName(type_of_bill)

    add_sequence = unioned.select(
        F.col("claim_id"),
        F.col("eob_supporting_info_category_code"),
        F.col("eob_supporting_info_code"),
        F.col("eob_supporting_info_system"),
        F.col("eob_supporting_info_timing_start").cast("string").alias("eob_supporting_info_timing_start"),
        F.col("eob_supporting_info_timing_end").cast("string").alias("eob_supporting_info_timing_end"),
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
            "eob_supporting_info_code",
            "eob_supporting_info_system",
            "eob_supporting_info_timing_start",
            "eob_supporting_info_timing_end",
        ],
    )
