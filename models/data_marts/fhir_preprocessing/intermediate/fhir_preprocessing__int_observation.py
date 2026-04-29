import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    obs = spark.table("fhir_preprocessing__stg_core__observation")

    return obs.select(
        F.col("person_id").cast("string").alias("patient_internal_id"),
        F.md5(F.coalesce(F.col("observation_id").cast("string"), F.lit(""))).cast("string").alias("resource_internal_id"),
        F.col("encounter_id").cast("string").alias("encounter_internal_id"),
        F.lit("final").alias("observation_status"),
        F.when(F.lower(F.col("observation_type")).like("%social%"), F.lit("social-history"))
         .when(F.lower(F.col("observation_type")).like("%vital%"), F.lit("vital-signs"))
         .when(F.lower(F.col("observation_type")).like("%imaging%"), F.lit("imaging"))
         .when(F.lower(F.col("observation_type")).like("%laboratory%"), F.lit("laboratory"))
         .when(F.lower(F.col("observation_type")).like("%procedure%"), F.lit("procedure"))
         .when(F.lower(F.col("observation_type")).like("%survey%"), F.lit("survey"))
         .when(F.lower(F.col("observation_type")).like("%exam%"), F.lit("exam"))
         .when(F.lower(F.col("observation_type")).like("%therapy%"), F.lit("therapy"))
         .when(F.lower(F.col("observation_type")).like("%activity%"), F.lit("activity"))
         .otherwise(F.lit("other"))
         .alias("observation_category"),
        F.upper(
            F.coalesce(
                F.col("normalized_code_type").cast("string"),
                F.col("source_code_type").cast("string"),
            )
        ).alias("observation_code_system"),
        F.coalesce(
            F.col("normalized_code").cast("string"),
            F.col("source_code").cast("string"),
        ).alias("observation_code"),
        F.coalesce(
            F.col("normalized_description").cast("string"),
            F.col("source_description").cast("string"),
        ).alias("observation_code_text"),
        F.col("observation_date").cast("timestamp").alias("observation_datetime"),
        F.col("result").cast("string").alias("observation_value"),
        F.coalesce(
            F.col("normalized_units").cast("string"),
            F.col("source_units").cast("string"),
        ).alias("observation_value_units"),
        F.col("data_source").cast("string").alias("data_source"),
    )
