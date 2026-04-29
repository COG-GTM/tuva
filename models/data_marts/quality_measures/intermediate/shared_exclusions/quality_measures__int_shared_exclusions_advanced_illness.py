import os
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    patients_with_frailty = (
        spark.table("quality_measures__int_shared_exclusions_frailty")
        .select(
            F.col("person_id"),
            F.col("exclusion_date"),
            F.col("exclusion_reason"),
        )
    )

    exclusion_codes = (
        spark.table("quality_measures__value_sets")
        .filter(
            F.lower(F.col("concept_name")).isin(
                "advanced illness",
                "acute inpatient",
                "encounter inpatient",
                "outpatient",
                "observation",
                "emergency department visit",
                "nonacute inpatient",
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
            F.col("claim_id"),
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
            F.col("claim_id"),
            F.col("claim_start_date"),
            F.col("claim_end_date"),
            F.col("hcpcs_code"),
            F.col("place_of_service_code"),
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

    # condition_exclusions: conditions matched to 'advanced illness' codes
    condition_exclusions = (
        conditions.alias("c")
        .join(
            exclusion_codes.alias("ec"),
            (F.col("c.code") == F.col("ec.code"))
            & (F.col("c.code_type") == F.col("ec.code_system")),
            "inner",
        )
        .filter(F.lower(F.col("ec.concept_name")) == "advanced illness")
        .select(
            F.col("c.person_id"),
            F.col("c.claim_id"),
            F.col("c.recorded_date"),
            F.col("ec.concept_name"),
        )
    )

    # med_claim_exclusions: medical claims matched to hcpcs exclusion codes
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
            F.col("mc.claim_id"),
            F.col("mc.claim_start_date"),
            F.col("mc.claim_end_date"),
            F.col("mc.hcpcs_code"),
            F.col("ec.concept_name"),
        )
    )

    # procedure_exclusions: procedures matched to exclusion codes
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

    # --- acute_inpatient ---
    # Via medical claims
    acute_inpatient_claims = (
        patients_with_frailty.alias("pf")
        .join(
            med_claim_exclusions.alias("mce"),
            F.col("pf.person_id") == F.col("mce.person_id"),
            "inner",
        )
        .join(
            condition_exclusions.alias("ce"),
            F.col("mce.claim_id") == F.col("ce.claim_id"),
            "inner",
        )
        .filter(F.lower(F.col("mce.concept_name")) == "acute inpatient")
        .select(
            F.col("pf.person_id"),
            F.coalesce(F.col("mce.claim_start_date"), F.col("mce.claim_end_date")).alias("exclusion_date"),
            F.concat(
                F.col("pf.exclusion_reason"),
                F.lit(" with "),
                F.col("mce.concept_name"),
                F.lit(" and "),
                F.col("ce.concept_name"),
            ).alias("exclusion_reason"),
            F.col("mce.claim_start_date"),
            F.col("mce.claim_end_date"),
            F.lit(None).cast("date").alias("procedure_date"),
        )
        .distinct()
    )

    # Via procedures
    acute_inpatient_procedures = (
        patients_with_frailty.alias("pf")
        .join(
            procedure_exclusions.alias("pe"),
            F.col("pf.person_id") == F.col("pe.person_id"),
            "inner",
        )
        .join(
            condition_exclusions.alias("ce"),
            (F.col("pe.person_id") == F.col("ce.person_id"))
            & (F.col("pe.procedure_date") == F.col("ce.recorded_date")),
            "inner",
        )
        .filter(F.lower(F.col("pe.concept_name")) == "acute inpatient")
        .select(
            F.col("pf.person_id"),
            F.col("pe.procedure_date").alias("exclusion_date"),
            F.concat(
                F.col("pf.exclusion_reason"),
                F.lit(" with "),
                F.col("pe.concept_name"),
                F.lit(" and "),
                F.col("ce.concept_name"),
            ).alias("exclusion_reason"),
            F.lit(None).cast("date").alias("claim_start_date"),
            F.lit(None).cast("date").alias("claim_end_date"),
            F.col("pe.procedure_date"),
        )
        .distinct()
    )

    acute_inpatient = acute_inpatient_claims.unionByName(acute_inpatient_procedures)

    # --- nonacute_outpatient ---
    nonacute_concept_names = [
        "encounter inpatient",
        "outpatient",
        "observation",
        "emergency department visit",
        "nonacute inpatient",
    ]

    # Via medical claims
    nonacute_outpatient_claims = (
        patients_with_frailty.alias("pf")
        .join(
            med_claim_exclusions.alias("mce"),
            F.col("pf.person_id") == F.col("mce.person_id"),
            "inner",
        )
        .join(
            condition_exclusions.alias("ce"),
            F.col("mce.claim_id") == F.col("ce.claim_id"),
            "inner",
        )
        .filter(F.lower(F.col("mce.concept_name")).isin(*nonacute_concept_names))
        .select(
            F.col("pf.person_id"),
            F.coalesce(F.col("mce.claim_start_date"), F.col("mce.claim_end_date")).alias("exclusion_date"),
            F.concat(
                F.col("pf.exclusion_reason"),
                F.lit(" with "),
                F.col("mce.concept_name"),
                F.lit(" and "),
                F.col("ce.concept_name"),
            ).alias("exclusion_reason"),
            F.col("mce.claim_start_date"),
            F.col("mce.claim_end_date"),
            F.lit(None).cast("date").alias("procedure_date"),
        )
        .distinct()
    )

    # Via procedures
    nonacute_outpatient_procedures = (
        patients_with_frailty.alias("pf")
        .join(
            procedure_exclusions.alias("pe"),
            F.col("pf.person_id") == F.col("pe.person_id"),
            "inner",
        )
        .join(
            condition_exclusions.alias("ce"),
            (F.col("pe.person_id") == F.col("ce.person_id"))
            & (F.col("pe.procedure_date") == F.col("ce.recorded_date")),
            "inner",
        )
        .filter(F.lower(F.col("pe.concept_name")).isin(*nonacute_concept_names))
        .select(
            F.col("pf.person_id"),
            F.col("pe.procedure_date").alias("exclusion_date"),
            F.concat(
                F.col("pf.exclusion_reason"),
                F.lit(" with "),
                F.col("pe.concept_name"),
                F.lit(" and "),
                F.col("ce.concept_name"),
            ).alias("exclusion_reason"),
            F.lit(None).cast("date").alias("claim_start_date"),
            F.lit(None).cast("date").alias("claim_end_date"),
            F.col("pe.procedure_date"),
        )
        .distinct()
    )

    nonacute_outpatient = nonacute_outpatient_claims.unionByName(nonacute_outpatient_procedures)

    # --- exclusions_unioned ---
    acute_with_type = acute_inpatient.withColumn("patient_type", F.lit("acute_inpatient"))
    nonacute_with_type = nonacute_outpatient.withColumn("patient_type", F.lit("nonacute_outpatient"))

    exclusions_unioned = acute_with_type.unionByName(nonacute_with_type)

    result = exclusions_unioned.select(
        F.col("person_id"),
        F.col("exclusion_date"),
        F.col("exclusion_reason"),
        F.lit("advanced_illness").alias("exclusion_type"),
        F.col("claim_start_date"),
        F.col("claim_end_date"),
        F.col("procedure_date"),
        F.col("patient_type"),
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
