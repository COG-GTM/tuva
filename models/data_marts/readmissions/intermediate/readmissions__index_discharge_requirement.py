import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    encounter = spark.table("readmissions__encounter")

    all_invalid_discharges = (
        encounter
        .filter(F.col("discharge_disposition_code").isin("02", "07", "20"))
        .select("encounter_id")
    )

    result = (
        encounter.alias("a")
        .join(
            all_invalid_discharges.alias("b"),
            F.col("a.encounter_id") == F.col("b.encounter_id"),
            "left",
        )
        .filter(F.col("b.encounter_id").isNull())
        .select(F.col("a.encounter_id"))
        .withColumn("tuva_last_run", F.current_timestamp())
    )

    return result
