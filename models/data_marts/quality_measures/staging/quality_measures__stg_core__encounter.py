import os
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    """
    This model selects from core__encounter when clinical or claims data
    is enabled. In PySpark we always read from core__encounter. If no data
    is available the table will simply be empty.
    """
    encounter = spark.table("core__encounter")

    result = encounter.select(
        F.col("person_id"),
        F.col("encounter_id"),
        F.col("encounter_type"),
        F.col("encounter_group"),
        F.col("length_of_stay"),
        F.col("encounter_start_date"),
        F.col("encounter_end_date"),
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
