import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    encounter_with_ccs = spark.table("readmissions__encounter_with_ccs")
    exclusion_ccs = spark.table("readmissions__exclusion_ccs_diagnosis_category")

    exclusion_categories = exclusion_ccs.select("ccs_diagnosis_category").distinct()

    exclusions = (
        encounter_with_ccs
        .filter(F.col("ccs_diagnosis_category").isNotNull())
        .join(exclusion_categories, "ccs_diagnosis_category", "inner")
        .select("encounter_id")
        .distinct()
    )

    result = exclusions.withColumn("tuva_last_run", F.current_timestamp())
    return result
