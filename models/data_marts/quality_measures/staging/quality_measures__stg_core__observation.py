import os
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    """
    Reads from core__observation. When clinical data is enabled, selects
    the full observation set. In PySpark we always read the table directly.
    """
    observation = spark.table("core__observation")

    result = observation.select(
        F.col("person_id"),
        F.col("encounter_id"),
        F.col("observation_date"),
        F.col("result"),
        F.col("source_code_type"),
        F.col("source_code"),
        F.col("normalized_code_type"),
        F.col("normalized_code"),
        F.col("normalized_description"),
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
