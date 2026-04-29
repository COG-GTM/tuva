import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    proc = spark.table("fhir_preprocessing__stg_core__procedure")

    result = (
        proc.filter(F.col("procedure_id").isNotNull())
        .filter(F.col("normalized_code_type").isNotNull())
        .filter(F.col("claim_id").isNull())
        .select(
            F.col("person_id").cast("string").alias("patient_internal_id"),
            F.col("procedure_id").cast("string").alias("resource_internal_id"),
            F.lit("completed").alias("procedure_status"),
            F.when(
                F.lower(
                    F.coalesce(
                        F.col("normalized_code_type").cast("string"),
                        F.col("source_code_type").cast("string"),
                    )
                ) == "icd-10-pcs",
                F.lit("ICD10"),
            ).when(
                F.lower(
                    F.coalesce(
                        F.col("normalized_code_type").cast("string"),
                        F.col("source_code_type").cast("string"),
                    )
                ) == "icd-9-pcs",
                F.lit("ICD9"),
            ).otherwise(
                F.coalesce(
                    F.col("normalized_code_type").cast("string"),
                    F.col("source_code_type").cast("string"),
                )
            ).alias("procedure_code_system"),
            F.coalesce(
                F.col("normalized_code").cast("string"),
                F.col("source_code").cast("string"),
            ).alias("procedure_code"),
            F.coalesce(
                F.col("normalized_description").cast("string"),
                F.col("source_description").cast("string"),
            ).alias("procedure_display"),
            F.col("procedure_date").cast("timestamp").alias("procedure_performed_datetime"),
            F.col("practitioner_id").cast("string").alias("practitioner_npi"),
            F.col("data_source").cast("string").alias("data_source"),
        )
        .distinct()
    )

    return result
