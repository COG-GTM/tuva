import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    coverage_staging = spark.table("fhir_preprocessing__int_coverage").select(
        "patient_internal_id",
        "resource_internal_id",
        "coverage_type_product",
    )

    product_type = coverage_staging.select(
        F.col("patient_internal_id"),
        F.col("resource_internal_id"),
        F.lit("COVERAGE_TYPE").alias("coverage_type_system"),
        F.col("coverage_type_product").alias("coverage_type_code"),
    )

    medical_benefit = coverage_staging.select(
        F.col("patient_internal_id"),
        F.col("resource_internal_id"),
        F.lit("ACT_CODE").alias("coverage_type_system"),
        F.when(
            F.col("coverage_type_product").isin("PPO", "POS", "CEP", "HMO", "MMO", "MOS", "MPO", "MEP"),
            F.lit("MCPOL"),
        ).when(
            F.col("coverage_type_product").isin("MCR", "MP", "MC", "MCS", "MMP", "MDE"),
            F.lit("RETIRE"),
        ).otherwise(F.lit("SUBSIDIZ")).alias("coverage_type_code"),
    )

    pharmacy_benefit = spark.table("fhir_preprocessing__int_pharmacy_claim_eob").select(
        F.col("patient_internal_id"),
        F.col("coverage_internal_id").alias("resource_internal_id"),
        F.lit("ACT_CODE").alias("coverage_type_system"),
        F.lit("DRUGPOL").alias("coverage_type_code"),
    ).distinct()

    unioned = product_type.unionByName(medical_benefit).unionByName(pharmacy_benefit)

    return create_json_object(
        df=unioned,
        group_cols=["patient_internal_id", "resource_internal_id"],
        obj_col="coverage_type_list",
        obj_fields=["coverage_type_system", "coverage_type_code"],
    )
