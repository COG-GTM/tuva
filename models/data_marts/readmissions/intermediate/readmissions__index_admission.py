import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    encounter = spark.table("readmissions__encounter")
    time_req = spark.table("readmissions__index_time_requirement")
    discharge_req = spark.table("readmissions__index_discharge_requirement")
    exclusion = spark.table("readmissions__exclusion")

    result = (
        encounter.alias("a")
        .join(time_req.alias("b"), F.col("a.encounter_id") == F.col("b.encounter_id"), "inner")
        .join(discharge_req.alias("c"), F.col("a.encounter_id") == F.col("c.encounter_id"), "inner")
        .join(exclusion.alias("d"), F.col("a.encounter_id") == F.col("d.encounter_id"), "left")
        .filter(F.col("d.encounter_id").isNull())
        .select(F.col("a.encounter_id"))
        .distinct()
        .withColumn("tuva_last_run", F.current_timestamp())
    )

    return result
