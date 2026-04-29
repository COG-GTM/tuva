import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    condition = spark.table("core__condition")

    return condition.select(
        F.col("condition_id"),
        F.col("person_id"),
        F.col("claim_id"),
        F.col("encounter_id"),
        F.col("recorded_date"),
        F.col("onset_date"),
        F.col("resolved_date"),
        F.col("status"),
        F.col("normalized_code_type"),
        F.col("normalized_code"),
        F.col("normalized_description"),
        F.col("condition_rank"),
        F.col("data_source"),
    )
