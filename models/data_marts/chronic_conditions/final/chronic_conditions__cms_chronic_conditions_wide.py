import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:

    chronic_conditions = (
        spark.table("chronic_conditions__cms_chronic_conditions_hierarchy")
        .select("condition", "condition_column_name")
        .distinct()
    )

    conditions_long = spark.table("chronic_conditions__cms_chronic_conditions_long")

    conditions = (
        conditions_long.alias("cl")
        .join(
            chronic_conditions.alias("cc"),
            F.col("cl.condition") == F.col("cc.condition"),
        )
        .select(
            F.col("cl.person_id"),
            F.col("cc.condition_column_name"),
            F.lit(1).alias("condition_count"),
        )
    )

    patients = spark.table("cms_chronic_conditions__stg_core__patient")

    # Get distinct condition column names for pivoting
    condition_col_names = (
        spark.table("chronic_conditions__cms_chronic_conditions_hierarchy")
        .select("condition_column_name")
        .distinct()
        .orderBy("condition_column_name")
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    pivoted = (
        patients.alias("p")
        .join(
            conditions.alias("c"),
            F.col("p.person_id") == F.col("c.person_id"),
            "left_outer",
        )
        .groupBy(F.col("p.person_id"))
        .pivot("condition_column_name", condition_col_names)
        .agg(F.max(F.when(F.col("condition_count") == 1, 1).otherwise(0)))
    )

    # Fill nulls with 0 for all pivoted columns
    fill_dict = {col_name: 0 for col_name in condition_col_names}
    pivoted = pivoted.fillna(fill_dict)

    result = pivoted.withColumn("tuva_last_run", F.current_timestamp())

    return result
