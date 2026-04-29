import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    denom = spark.table("ahrq_measures__stg_pqi_inpatient_encounter")
    missing_age = spark.table("ahrq_measures__int_pqi_shared_exclusion_missing_age")
    missing_gender = spark.table("ahrq_measures__int_pqi_shared_exclusion_missing_gender")
    missing_dates = spark.table("ahrq_measures__int_pqi_shared_exclusion_missing_dates")
    missing_primary_dx = spark.table("ahrq_measures__int_pqi_shared_exclusion_missing_primary_dx")
    transfer = spark.table("ahrq_measures__int_pqi_shared_exclusion_transfer")
    ungroupable_drg = spark.table("ahrq_measures__int_pqi_shared_exclusion_ungroupable_drg")

    # Missing Age Exclusion
    age_excl = (
        denom.alias("denom")
        .join(
            missing_age.alias("age"),
            (F.col("denom.person_id") == F.col("age.person_id"))
            & (F.col("denom.data_source") == F.col("age.data_source")),
            "inner",
        )
        .select(
            F.col("denom.encounter_id"),
            F.col("denom.data_source"),
            F.lit("missing age").alias("exclusion_reason"),
            F.current_timestamp().alias("tuva_last_run"),
        )
    )

    # Missing Gender Exclusion
    gender_excl = (
        denom.alias("denom")
        .join(
            missing_gender.alias("gender"),
            (F.col("denom.person_id") == F.col("gender.person_id"))
            & (F.col("denom.data_source") == F.col("gender.data_source")),
            "inner",
        )
        .select(
            F.col("denom.encounter_id"),
            F.col("denom.data_source"),
            F.lit("missing gender").alias("exclusion_reason"),
            F.current_timestamp().alias("tuva_last_run"),
        )
    )

    # Missing Dates Exclusion
    dates_excl = (
        denom.alias("denom")
        .join(
            missing_dates.alias("dates"),
            (F.col("denom.encounter_id") == F.col("dates.encounter_id"))
            & (F.col("denom.data_source") == F.col("dates.data_source")),
            "inner",
        )
        .select(
            F.col("denom.encounter_id"),
            F.col("denom.data_source"),
            F.lit("missing dates").alias("exclusion_reason"),
            F.current_timestamp().alias("tuva_last_run"),
        )
    )

    # Missing Primary Diagnosis Exclusion
    dx_excl = (
        denom.alias("denom")
        .join(
            missing_primary_dx.alias("dx"),
            (F.col("denom.encounter_id") == F.col("dx.encounter_id"))
            & (F.col("denom.data_source") == F.col("dx.data_source")),
            "inner",
        )
        .select(
            F.col("denom.encounter_id"),
            F.col("denom.data_source"),
            F.lit("missing primary diagnosis").alias("exclusion_reason"),
            F.current_timestamp().alias("tuva_last_run"),
        )
    )

    # Transfer Exclusion
    transfer_excl = (
        denom.alias("denom")
        .join(
            transfer.alias("tx"),
            (F.col("denom.encounter_id") == F.col("tx.encounter_id"))
            & (F.col("denom.data_source") == F.col("tx.data_source")),
            "inner",
        )
        .select(
            F.col("denom.encounter_id"),
            F.col("denom.data_source"),
            F.lit("transfer").alias("exclusion_reason"),
            F.current_timestamp().alias("tuva_last_run"),
        )
    )

    # Ungroupable DRG Exclusion
    drg_excl = (
        denom.alias("denom")
        .join(
            ungroupable_drg.alias("drg"),
            (F.col("denom.encounter_id") == F.col("drg.encounter_id"))
            & (F.col("denom.data_source") == F.col("drg.data_source")),
            "inner",
        )
        .select(
            F.col("denom.encounter_id"),
            F.col("denom.data_source"),
            F.lit("ungroupable DRG").alias("exclusion_reason"),
            F.current_timestamp().alias("tuva_last_run"),
        )
    )

    result = (
        age_excl
        .unionByName(gender_excl)
        .unionByName(dates_excl)
        .unionByName(dx_excl)
        .unionByName(transfer_excl)
        .unionByName(drg_excl)
    )

    return result
