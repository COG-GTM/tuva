import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    condition = spark.table("fhir_preprocessing__stg_core__condition")
    encounter = spark.table("fhir_preprocessing__stg_core__encounter")

    joined = (
        condition.alias("cond")
        .join(encounter.alias("enc"), F.col("cond.encounter_id") == F.col("enc.encounter_id"), how="left")
        .filter(F.col("cond.normalized_code_type").isNotNull())
        .filter(F.col("cond.claim_id").isNull())
    )

    result = joined.select(
        F.col("cond.person_id").cast("string").alias("patient_internal_id"),
        F.col("cond.condition_id").cast("string").alias("resource_internal_id"),
        F.col("cond.encounter_id").cast("string").alias("encounter_internal_id"),
        F.lit("encounter-diagnosis").alias("condition_category"),
        F.col("cond.recorded_date").cast("timestamp").alias("condition_recorded_datetime"),
        F.coalesce(
            F.col("cond.onset_date").cast("timestamp"),
            F.col("cond.recorded_date").cast("timestamp"),
        ).alias("condition_onset_datetime"),
        F.col("cond.resolved_date").cast("timestamp").alias("condition_abatement_datetime"),
        F.col("cond.status").cast("string").alias("condition_clinical_status"),
        F.when(
            (F.lower(F.col("cond.normalized_code_type")) == "icd-10-cm")
            & (F.length(F.col("cond.normalized_code")) > 3),
            F.concat(
                F.substring(F.col("cond.normalized_code"), 1, 3),
                F.lit("."),
                F.substring(F.col("cond.normalized_code"), 4, 100),
            ).cast("string"),
        ).otherwise(F.col("cond.normalized_code").cast("string")).alias("condition_code"),
        F.when(F.lower(F.col("cond.normalized_code_type")) == "icd-10-cm", F.lit("ICD10"))
         .when(F.lower(F.col("cond.normalized_code_type")) == "icd-9-cm", F.lit("ICD9"))
         .otherwise(F.col("cond.normalized_code_type").cast("string"))
         .alias("condition_code_system"),
        F.lit("finished").alias("encounter_status"),
        F.when(F.col("enc.encounter_group") == "inpatient", F.lit("IMP"))
         .when(F.col("enc.encounter_group").isin("outpatient", "office based"), F.lit("AMB"))
         .otherwise(F.lit("other"))
         .alias("encounter_class_code"),
        F.col("enc.encounter_start_date").cast("timestamp").alias("encounter_start_datetime"),
        F.col("enc.encounter_end_date").cast("timestamp").alias("encounter_end_datetime"),
        F.col("cond.data_source").cast("string").alias("data_source"),
    ).distinct()

    return result
