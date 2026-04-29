import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    long_procedure_category = spark.table("ccsr__long_procedure_category")

    procedure_base = (
        long_procedure_category
        .filter(F.col("ccsr_category").isNotNull())
        .select(
            F.col("encounter_id"),
            F.col("claim_id"),
            F.col("normalized_code"),
            F.col("code_description"),
            F.col("ccsr_parent_category"),
            F.col("ccsr_category"),
            F.col("ccsr_category_description"),
            F.col("clinical_domain"),
            F.col("operation"),
            F.col("approach"),
        )
    )

    window_spec = Window.partitionBy("ccsr_category", "operation")
    procedure_base = procedure_base.withColumn(
        "n_total_occurrences",
        F.count("claim_id").over(window_spec)
    )

    procedures_aggregated = (
        procedure_base
        .groupBy(
            "ccsr_category",
            "ccsr_category_description",
            "operation",
            "approach",
            "n_total_occurrences",
        )
        .agg(
            F.count("claim_id").alias("n_occurrences_with_approach"),
        )
    )

    procedures_aggregated = procedures_aggregated.withColumn(
        "approach_rate",
        (F.col("n_occurrences_with_approach") / F.col("n_total_occurrences") * 100)
    )

    result = procedures_aggregated.withColumn(
        "tuva_last_run",
        F.current_timestamp()
    )

    return result
