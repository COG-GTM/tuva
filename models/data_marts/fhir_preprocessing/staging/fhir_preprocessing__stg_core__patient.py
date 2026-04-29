import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    patient = spark.table("core__patient")

    return patient.select(
        F.col("person_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("sex"),
        F.col("race"),
        F.col("birth_date"),
        F.col("data_source"),
    )
