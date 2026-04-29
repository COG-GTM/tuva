import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    lab = spark.table("fhir_preprocessing__stg_core__lab_result")

    return lab.select(
        F.col("person_id").cast("string").alias("patient_internal_id"),
        F.md5(F.coalesce(F.col("lab_result_id").cast("string"), F.lit(""))).cast("string").alias("resource_internal_id"),
        F.col("encounter_id").cast("string").alias("encounter_internal_id"),
        F.when(F.lower(F.col("status")).isin("final", "f"), F.lit("final"))
         .when(F.lower(F.col("status")).isin("preliminary", "p"), F.lit("preliminary"))
         .when(F.lower(F.col("status")).isin("corrected", "c"), F.lit("corrected"))
         .when(F.lower(F.col("status")).isin("cancelled", "d"), F.lit("cancelled"))
         .otherwise(F.lower(F.col("status")))
         .alias("observation_status"),
        F.lit("laboratory").alias("observation_category"),
        F.upper(F.col("code_type")).cast("string").alias("observation_code_system"),
        F.col("code").cast("string").alias("observation_code"),
        F.col("description").cast("string").alias("observation_code_text"),
        F.col("result_datetime").cast("timestamp").alias("observation_datetime"),
        F.col("result").cast("string").alias("observation_value"),
        F.col("units").cast("string").alias("observation_value_units"),
        F.col("data_source").cast("string").alias("data_source"),
    )
