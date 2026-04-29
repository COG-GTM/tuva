import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import (
    safe_cast_date,
    safe_cast_timestamp,
    safe_cast_int,
    year_month,
    apply_regex,
    substring_col,
    create_json_object,
    union_relations,
)

COLNAMES = [
    "edcnnpa",
    "edcnpa",
    "epct",
    "noner",
    "injury",
    "psych",
    "alcohol",
    "drug",
]


def run(spark: SparkSession) -> DataFrame:
    condition = (
        spark.table("ed_classification__stg_encounter")
        .filter(F.col("encounter_type") == "emergency department")
    )

    icd9 = (
        spark.table("ed_classification__johnston_icd9")
        .select(
            F.col("icd9").alias("code"),
            *[F.col(c) for c in COLNAMES],
            F.lit(1).alias("ed_classification_capture"),
        )
    )

    icd10 = (
        spark.table("ed_classification__johnston_icd10")
        .select(
            F.col("icd10").alias("code"),
            *[F.col(c) for c in COLNAMES],
            F.lit(1).alias("ed_classification_capture"),
        )
    )

    icd10_joined = (
        condition.alias("a")
        .join(
            icd10.alias("icd10"),
            (F.col("a.primary_diagnosis_code") == F.col("icd10.code"))
            & (F.col("a.primary_diagnosis_code_type") == "icd-10-cm"),
            "left_outer",
        )
        .select(
            F.col("a.*"),
            *[F.col(f"icd10.{c}") for c in COLNAMES],
            F.coalesce(F.col("icd10.ed_classification_capture"), F.lit(0)).alias(
                "ed_classification_capture"
            ),
        )
    )

    icd9_joined = (
        condition.alias("a")
        .join(
            icd9.alias("icd9"),
            (F.col("a.primary_diagnosis_code") == F.col("icd9.code"))
            & (F.col("a.primary_diagnosis_code_type") == "icd-9-cm"),
            "inner",
        )
        .select(
            F.col("a.*"),
            *[F.col(f"icd9.{c}") for c in COLNAMES],
            F.coalesce(F.col("icd9.ed_classification_capture"), F.lit(0)).alias(
                "ed_classification_capture"
            ),
        )
    )

    return icd10_joined.unionAll(icd9_joined)
