import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    num_long = spark.table("ahrq_measures__pqi_num_long")
    denom_long = spark.table("ahrq_measures__pqi_denom_long")

    num = (
        num_long
        .groupBy("data_source", "year_number", "pqi_number")
        .agg(F.count("encounter_id").alias("num_count"))
    )

    denom = (
        denom_long
        .groupBy("data_source", "year_number", "pqi_number")
        .agg(F.count("person_id").alias("denom_count"))
    )

    result = (
        denom.alias("d")
        .join(
            num.alias("num"),
            (F.col("d.pqi_number") == F.col("num.pqi_number"))
            & (F.col("d.year_number") == F.col("num.year_number"))
            & (F.col("d.data_source") == F.col("num.data_source")),
            "left",
        )
        .select(
            F.col("d.data_source"),
            F.col("d.year_number"),
            F.col("d.pqi_number"),
            F.col("d.denom_count"),
            F.coalesce(F.col("num.num_count"), F.lit(0)).alias("num_count"),
            (F.coalesce(F.col("num.num_count"), F.lit(0)).cast("double") / F.col("d.denom_count").cast("double") * F.lit(100000)).alias("rate_per_100_thousand"),
            F.current_timestamp().alias("tuva_last_run"),
        )
    )

    return result
