import os
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    patients = spark.table("quality_measures__stg_core__patient").select(
        F.col("person_id")
    )

    exclusion_codes = (
        spark.table("quality_measures__value_sets")
        .filter(
            F.lower(F.col("concept_name")).isin(
                "frailty device",
                "frailty diagnosis",
                "frailty encounter",
                "frailty symptom",
            )
        )
        .select(
            F.col("code"),
            F.col("code_system"),
            F.col("concept_name"),
        )
    )

    conditions = (
        spark.table("quality_measures__stg_core__condition")
        .select(
            F.col("person_id"),
            F.col("recorded_date"),
            F.coalesce(
                F.col("normalized_code_type"),
                F.when(F.lower(F.col("source_code_type")) == "snomed", F.lit("snomed-ct"))
                .otherwise(F.lower(F.col("source_code_type"))),
            ).alias("code_type"),
            F.coalesce(F.col("normalized_code"), F.col("source_code")).alias("code"),
        )
    )

    medical_claim = (
        spark.table("quality_measures__stg_medical_claim")
        .select(
            F.col("person_id"),
            F.col("claim_start_date"),
            F.col("claim_end_date"),
            F.col("hcpcs_code"),
            F.col("place_of_service_code"),
        )
    )

    observations = (
        spark.table("quality_measures__stg_core__observation")
        .select(
            F.col("person_id"),
            F.col("observation_date"),
            F.coalesce(
                F.col("normalized_code_type"),
                F.when(F.lower(F.col("source_code_type")) == "cpt", F.lit("hcpcs"))
                .when(F.lower(F.col("source_code_type")) == "snomed", F.lit("snomed-ct"))
                .otherwise(F.lower(F.col("source_code_type"))),
            ).alias("code_type"),
            F.coalesce(F.col("normalized_code"), F.col("source_code")).alias("code"),
        )
    )

    procedures = (
        spark.table("quality_measures__stg_core__procedure")
        .select(
            F.col("person_id"),
            F.col("procedure_date"),
            F.coalesce(
                F.col("normalized_code_type"),
                F.when(F.lower(F.col("source_code_type")) == "cpt", F.lit("hcpcs"))
                .when(F.lower(F.col("source_code_type")) == "snomed", F.lit("snomed-ct"))
                .otherwise(F.lower(F.col("source_code_type"))),
            ).alias("code_type"),
            F.coalesce(F.col("normalized_code"), F.col("source_code")).alias("code"),
        )
    )

    condition_exclusions = (
        conditions.alias("c")
        .join(
            exclusion_codes.alias("ec"),
            (F.col("c.code") == F.col("ec.code"))
            & (F.col("c.code_type") == F.col("ec.code_system")),
            "inner",
        )
        .select(
            F.col("c.person_id"),
            F.col("c.recorded_date"),
            F.col("ec.concept_name"),
        )
    )

    med_claim_exclusions = (
        medical_claim.alias("mc")
        .join(
            exclusion_codes.alias("ec"),
            F.col("mc.hcpcs_code") == F.col("ec.code"),
            "inner",
        )
        .filter(F.col("ec.code_system") == "hcpcs")
        .select(
            F.col("mc.person_id"),
            F.col("mc.claim_start_date"),
            F.col("mc.claim_end_date"),
            F.col("mc.hcpcs_code"),
            F.col("ec.concept_name"),
        )
    )

    observation_exclusions = (
        observations.alias("o")
        .join(
            exclusion_codes.alias("ec"),
            (F.col("o.code") == F.col("ec.code"))
            & (F.col("o.code_type") == F.col("ec.code_system")),
            "inner",
        )
        .select(
            F.col("o.person_id"),
            F.col("o.observation_date"),
            F.col("ec.concept_name"),
        )
    )

    procedure_exclusions = (
        procedures.alias("p")
        .join(
            exclusion_codes.alias("ec"),
            (F.col("p.code") == F.col("ec.code"))
            & (F.col("p.code_type") == F.col("ec.code_system")),
            "inner",
        )
        .select(
            F.col("p.person_id"),
            F.col("p.procedure_date"),
            F.col("ec.concept_name"),
        )
    )

    frailty_from_conditions = (
        patients.alias("pat")
        .join(
            condition_exclusions.alias("ce"),
            F.col("pat.person_id") == F.col("ce.person_id"),
            "inner",
        )
        .select(
            F.col("pat.person_id"),
            F.col("ce.recorded_date").alias("exclusion_date"),
            F.col("ce.concept_name").alias("exclusion_reason"),
        )
    )

    frailty_from_med_claims = (
        patients.alias("pat")
        .join(
            med_claim_exclusions.alias("mce"),
            F.col("pat.person_id") == F.col("mce.person_id"),
            "inner",
        )
        .select(
            F.col("pat.person_id"),
            F.coalesce(F.col("mce.claim_start_date"), F.col("mce.claim_end_date")).alias("exclusion_date"),
            F.col("mce.concept_name").alias("exclusion_reason"),
        )
    )

    frailty_from_observations = (
        patients.alias("pat")
        .join(
            observation_exclusions.alias("oe"),
            F.col("pat.person_id") == F.col("oe.person_id"),
            "inner",
        )
        .select(
            F.col("pat.person_id"),
            F.col("oe.observation_date").alias("exclusion_date"),
            F.col("oe.concept_name").alias("exclusion_reason"),
        )
    )

    frailty_from_procedures = (
        patients.alias("pat")
        .join(
            procedure_exclusions.alias("pe"),
            F.col("pat.person_id") == F.col("pe.person_id"),
            "inner",
        )
        .select(
            F.col("pat.person_id"),
            F.col("pe.procedure_date").alias("exclusion_date"),
            F.col("pe.concept_name").alias("exclusion_reason"),
        )
    )

    patients_with_frailty = (
        frailty_from_conditions
        .unionByName(frailty_from_med_claims)
        .unionByName(frailty_from_observations)
        .unionByName(frailty_from_procedures)
    )

    result = patients_with_frailty.select(
        F.col("person_id"),
        F.col("exclusion_date"),
        F.col("exclusion_reason"),
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
