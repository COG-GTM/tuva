import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:

    all_conditions = (
        spark.table("tuva_chronic_conditions__stg_core__condition")
        .select(
            F.col("person_id"),
            F.col("normalized_code"),
            F.col("recorded_date"),
        )
    )

    conditions_with_first_and_last_diagnosis_date = (
        all_conditions
        .groupBy("person_id", F.col("normalized_code").alias("icd_10_cm"))
        .agg(
            F.min("recorded_date").alias("first_diagnosis_date"),
            F.max("recorded_date").alias("last_diagnosis_date"),
        )
    )

    value_set_members = spark.table("clinical_concept_library__value_set_member_relevant_fields")

    result = (
        conditions_with_first_and_last_diagnosis_date.alias("aa")
        .join(
            value_set_members.alias("bb"),
            F.col("aa.icd_10_cm") == F.col("bb.code"),
        )
        .groupBy(F.col("aa.person_id"), F.col("bb.concept_name"))
        .agg(
            F.min("first_diagnosis_date").alias("first_diagnosis_date"),
            F.max("last_diagnosis_date").alias("last_diagnosis_date"),
        )
        .select(
            F.col("person_id"),
            F.col("concept_name").alias("condition"),
            F.col("first_diagnosis_date"),
            F.col("last_diagnosis_date"),
            F.current_timestamp().alias("tuva_last_run"),
        )
    )

    return result
