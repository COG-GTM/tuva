import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    medical_claim = spark.table("fhir_preprocessing__stg_core__medical_claim")
    claim_condition = spark.table("fhir_preprocessing__stg_core__condition")

    staging = (
        medical_claim.filter(F.col("claim_line_number") == 1)
        .join(claim_condition, on="claim_id", how="inner")
        .select(
            medical_claim["claim_id"],
            F.col("condition_rank").alias("eob_diagnosis_sequence"),
            F.when(
                F.lower(F.col("normalized_code_type")) == "icd-10-cm",
                F.lit("ICD10"),
            ).otherwise(F.lit("ICD9")).alias("eob_diagnosis_system"),
            F.when(
                (F.lower(F.col("normalized_code_type")) == "icd-10-cm")
                & (F.length(F.col("normalized_code")) > 3),
                F.concat(
                    F.substring(F.col("normalized_code"), 1, 3),
                    F.lit("."),
                    F.substring(F.col("normalized_code"), 4, 100),
                ),
            ).otherwise(F.col("normalized_code")).alias("eob_diagnosis_code"),
            F.regexp_replace(F.col("normalized_description"), ",", "").alias("eob_diagnosis_display"),
            F.when(F.col("condition_rank") == 1, F.lit("principal"))
             .otherwise(F.lit("other"))
             .alias("eob_diagnosis_type_code"),
        )
    )

    return create_json_object(
        df=staging,
        group_cols="claim_id",
        obj_col="eob_diagnosis_list",
        obj_fields=[
            "eob_diagnosis_sequence",
            "eob_diagnosis_system",
            "eob_diagnosis_code",
            "eob_diagnosis_display",
            "eob_diagnosis_type_code",
        ],
    )
