import os
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    procedure = spark.table("core__procedure")

    result = procedure.select(
        F.col("person_id"),
        F.col("encounter_id"),
        F.col("procedure_date"),
        F.col("source_code_type"),
        F.col("source_code"),
        F.col("normalized_code_type"),
        F.col("normalized_code"),
        F.col("modifier_1"),
        F.col("modifier_2"),
        F.col("modifier_3"),
        F.col("modifier_4"),
        F.col("modifier_5"),
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
