import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    src = spark.table("financial_pmpm__patient_spend_with_service_categories")

    service_cat_1 = src.groupBy(
        "person_id",
        "member_id",
        "year_month",
        "payer",
        "plan",
        "service_category_1",
        "data_source",
    ).agg(
        F.sum("total_paid").alias("total_paid"),
    )

    categories = [
        "inpatient",
        "outpatient",
        "office-based",
        "ancillary",
        "other",
        "pharmacy",
    ]

    pivot_exprs = []
    for cat in categories:
        col_name = cat.replace("-", "_") + "_paid"
        pivot_exprs.append(
            F.sum(
                F.when(F.col("service_category_1") == cat, F.col("total_paid")).otherwise(0)
            ).alias(col_name)
        )

    result = service_cat_1.groupBy(
        "person_id",
        "member_id",
        "year_month",
        "payer",
        "plan",
        "data_source",
    ).agg(*pivot_exprs)

    result = result.withColumn("tuva_last_run", F.current_timestamp())

    return result
