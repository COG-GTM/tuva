import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    """Staging model for core observation.

    The original SQL has branching logic based on dbt vars.
    In PySpark we assume core__observation exists.
    """
    obs = spark.table("core__observation")

    return obs.select(
        F.col("observation_id"),
        F.col("person_id"),
        F.col("encounter_id"),
        F.col("observation_type"),
        F.col("normalized_code_type"),
        F.col("normalized_code"),
        F.col("normalized_description"),
        F.col("source_code_type"),
        F.col("source_code"),
        F.col("source_description"),
        F.col("observation_date"),
        F.col("result"),
        F.col("normalized_units"),
        F.col("source_units"),
        F.col("data_source"),
    )
