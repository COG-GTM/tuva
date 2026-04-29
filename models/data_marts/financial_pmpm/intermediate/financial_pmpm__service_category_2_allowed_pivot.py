import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    src = spark.table("financial_pmpm__patient_spend_with_service_categories")

    service_cat_2 = src.groupBy(
        "person_id",
        "member_id",
        "year_month",
        "payer",
        "plan",
        "service_category_2",
        "data_source",
    ).agg(
        F.sum("total_allowed").alias("total_allowed"),
    )

    categories = [
        "acute inpatient",
        "ambulance",
        "ambulatory surgery center",
        "dialysis",
        "durable medical equipment",
        "emergency department",
        "home health",
        "inpatient hospice",
        "inpatient psychiatric",
        "inpatient rehabilitation",
        "lab",
        "observation",
        "office-based other",
        "office-based pt/ot/st",
        "office-based radiology",
        "office-based surgery",
        "office-based visit",
        "other",
        "outpatient hospice",
        "outpatient hospital or clinic",
        "outpatient pt/ot/st",
        "outpatient psychiatric",
        "outpatient radiology",
        "outpatient rehabilitation",
        "outpatient surgery",
        "pharmacy",
        "skilled nursing",
        "telehealth visit",
        "urgent care",
    ]

    def _col_name(cat: str) -> str:
        return cat.replace("-", "_").replace("/", "").replace(" ", "_") + "_allowed"

    pivot_exprs = []
    for cat in categories:
        pivot_exprs.append(
            F.sum(
                F.when(F.col("service_category_2") == cat, F.col("total_allowed")).otherwise(0)
            ).alias(_col_name(cat))
        )

    result = service_cat_2.groupBy(
        "person_id",
        "member_id",
        "year_month",
        "payer",
        "plan",
        "data_source",
    ).agg(*pivot_exprs)

    result = result.withColumn("tuva_last_run", F.current_timestamp())

    return result
