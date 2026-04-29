import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:

    conditions_all = spark.table("chronic_conditions__cms_chronic_conditions_all")
    conditions_hiv_aids = spark.table("chronic_conditions__cms_chronic_conditions_hiv_aids")
    conditions_oud = spark.table("chronic_conditions__cms_chronic_conditions_oud")

    conditions_unioned = (
        conditions_all
        .unionByName(conditions_hiv_aids)
        .unionByName(conditions_oud)
        .distinct()
    )

    result = conditions_unioned.select(
        F.col("person_id"),
        F.col("claim_id"),
        F.col("start_date"),
        F.col("chronic_condition_type"),
        F.col("condition_category"),
        F.col("condition"),
        F.col("data_source"),
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
