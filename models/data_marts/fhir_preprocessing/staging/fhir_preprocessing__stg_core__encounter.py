import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    encounter = spark.table("core__encounter")

    return encounter.select(
        F.col("person_id"),
        F.col("encounter_id"),
        F.col("encounter_group"),
        F.col("encounter_start_date"),
        F.col("encounter_end_date"),
        F.col("data_source"),
    )
