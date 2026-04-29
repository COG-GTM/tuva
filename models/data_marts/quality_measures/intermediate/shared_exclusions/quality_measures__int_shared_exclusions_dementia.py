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
        .filter(F.lower(F.col("concept_name")) == "dementia medications")
        .select(
            F.col("code"),
            F.col("code_system"),
            F.col("concept_name"),
        )
    )

    medications = (
        spark.table("quality_measures__stg_core__medication")
        .select(
            F.col("person_id"),
            F.col("dispensing_date"),
            F.col("source_code_type"),
            F.col("source_code"),
            F.col("ndc_code"),
            F.col("rxnorm_code"),
        )
    )

    pharmacy_claim = (
        spark.table("quality_measures__stg_pharmacy_claim")
        .select(
            F.col("person_id"),
            F.col("dispensing_date"),
            F.col("ndc_code"),
            F.col("paid_date"),
        )
    )

    # medication_exclusions: medications matched via ndc, rxnorm, or source code
    med_excl_ndc = (
        medications.alias("m")
        .join(
            exclusion_codes.alias("ec"),
            F.col("m.ndc_code") == F.col("ec.code"),
            "inner",
        )
        .filter(F.col("ec.code_system") == "ndc")
        .select(
            F.col("m.person_id"),
            F.col("m.dispensing_date"),
            F.col("ec.concept_name"),
        )
    )

    med_excl_rxnorm = (
        medications.alias("m")
        .join(
            exclusion_codes.alias("ec"),
            F.col("m.rxnorm_code") == F.col("ec.code"),
            "inner",
        )
        .filter(F.col("ec.code_system") == "rxnorm")
        .select(
            F.col("m.person_id"),
            F.col("m.dispensing_date"),
            F.col("ec.concept_name"),
        )
    )

    med_excl_source = (
        medications.alias("m")
        .join(
            exclusion_codes.alias("ec"),
            (F.col("m.source_code") == F.col("ec.code"))
            & (F.col("m.source_code_type") == F.col("ec.code_system")),
            "inner",
        )
        .select(
            F.col("m.person_id"),
            F.col("m.dispensing_date"),
            F.col("ec.concept_name"),
        )
    )

    medication_exclusions = (
        med_excl_ndc
        .unionByName(med_excl_rxnorm)
        .unionByName(med_excl_source)
    )

    # pharmacy_claim_exclusions: pharmacy claims matched via ndc
    pharmacy_claim_exclusions = (
        pharmacy_claim.alias("pc")
        .join(
            exclusion_codes.alias("ec"),
            F.col("pc.ndc_code") == F.col("ec.code"),
            "inner",
        )
        .filter(F.col("ec.code_system") == "ndc")
        .select(
            F.col("pc.person_id"),
            F.col("pc.dispensing_date"),
            F.col("pc.ndc_code"),
            F.col("pc.paid_date"),
            F.col("ec.concept_name"),
        )
    )

    # frailty_with_dementia: frailty patients joined with dementia medication evidence
    frailty_via_pharmacy = (
        patients_with_frailty.alias("pf")
        .join(
            pharmacy_claim_exclusions.alias("pce"),
            F.col("pf.person_id") == F.col("pce.person_id"),
            "inner",
        )
        .select(
            F.col("pf.person_id"),
            F.col("pf.exclusion_date"),
            F.concat(
                F.col("pf.exclusion_reason"),
                F.lit(" with "),
                F.col("pce.concept_name"),
            ).alias("exclusion_reason"),
            F.col("pce.dispensing_date"),
            F.col("pce.paid_date"),
        )
    )

    frailty_via_medication = (
        patients_with_frailty.alias("pf")
        .join(
            medication_exclusions.alias("me"),
            F.col("pf.person_id") == F.col("me.person_id"),
            "inner",
        )
        .select(
            F.col("pf.person_id"),
            F.col("me.dispensing_date").alias("exclusion_date"),
            F.concat(
                F.col("pf.exclusion_reason"),
                F.lit(" with "),
                F.col("me.concept_name"),
            ).alias("exclusion_reason"),
            F.col("me.dispensing_date"),
            F.lit(None).cast("date").alias("paid_date"),
        )
    )

    frailty_with_dementia = frailty_via_pharmacy.unionByName(frailty_via_medication)

    result = frailty_with_dementia.select(
        F.col("person_id"),
        F.col("exclusion_date"),
        F.col("exclusion_reason"),
        F.lit("dementia").alias("exclusion_type"),
        F.col("dispensing_date"),
        F.col("paid_date"),
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
