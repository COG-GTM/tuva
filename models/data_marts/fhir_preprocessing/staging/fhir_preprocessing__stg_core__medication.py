import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    """Staging model for core medication.

    The original SQL has branching logic based on dbt vars.
    In PySpark we assume core__medication exists.
    """
    med = spark.table("core__medication")

    return med.select(
        F.col("medication_id"),
        F.col("person_id"),
        F.col("source_code_type"),
        F.col("ndc_code"),
        F.col("ndc_description"),
        F.col("rxnorm_code"),
        F.col("rxnorm_description"),
        F.col("days_supply"),
        F.col("dispensing_date"),
        F.col("data_source"),
    )
