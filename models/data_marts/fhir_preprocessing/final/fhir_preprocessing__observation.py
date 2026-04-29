import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    lab_result = spark.table("fhir_preprocessing__int_lab_result")
    observation = spark.table("fhir_preprocessing__int_observation")

    unioned = union_relations([lab_result, observation])

    return unioned.select(
        F.col("patient_internal_id").cast("string").alias("patient_internal_id"),
        F.col("resource_internal_id").cast("string").alias("resource_internal_id"),
        F.col("encounter_internal_id").cast("string").alias("encounter_internal_id"),
        F.col("observation_status").cast("string").alias("observation_status"),
        F.col("observation_category").cast("string").alias("observation_category"),
        F.col("observation_code_system").cast("string").alias("observation_code_system"),
        F.col("observation_code").cast("string").alias("observation_code"),
        F.col("observation_code_text").cast("string").alias("observation_code_text"),
        F.col("observation_datetime").cast("timestamp").alias("observation_datetime"),
        F.col("observation_value").cast("string").alias("observation_value"),
        F.col("observation_value_units").cast("string").alias("observation_value_units"),
        F.col("data_source").cast("string").alias("data_source"),
    )
