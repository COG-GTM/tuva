import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    procedure_ccs = spark.table("readmissions__procedure_ccs")
    always_planned_ccs_procedure = spark.table("readmissions__always_planned_ccs_procedure_category")
    encounter_with_ccs = spark.table("readmissions__encounter_with_ccs")
    always_planned_ccs_diagnosis = spark.table("readmissions__always_planned_ccs_diagnosis_category")
    potentially_planned_ccs_procedure = spark.table("readmissions__potentially_planned_ccs_procedure_category")
    potentially_planned_icd_10_pcs = spark.table("readmissions__potentially_planned_icd_10_pcs")
    acute_diagnosis_icd = spark.table("readmissions__acute_diagnosis_icd_10_cm")
    acute_diagnosis_ccs = spark.table("readmissions__acute_diagnosis_ccs")

    always_planned_px = (
        procedure_ccs.alias("pccs")
        .join(
            always_planned_ccs_procedure.alias("apc"),
            F.col("pccs.ccs_procedure_category") == F.col("apc.ccs_procedure_category"),
            "inner",
        )
        .select(F.col("pccs.encounter_id"))
        .distinct()
    )

    always_planned_dx = (
        encounter_with_ccs.alias("dccs")
        .join(
            always_planned_ccs_diagnosis.alias("apd"),
            F.col("dccs.ccs_diagnosis_category") == F.col("apd.ccs_diagnosis_category"),
            "inner",
        )
        .select(F.col("dccs.encounter_id"))
        .distinct()
    )

    pot_planned_px_ccs = (
        procedure_ccs.alias("pccs2")
        .join(
            potentially_planned_ccs_procedure.alias("pcs"),
            F.col("pccs2.ccs_procedure_category") == F.col("pcs.ccs_procedure_category"),
            "inner",
        )
        .select(F.col("pccs2.encounter_id"))
        .distinct()
    )

    pot_planned_px_icd = (
        procedure_ccs.alias("pcs2")
        .join(
            potentially_planned_icd_10_pcs.alias("pps"),
            F.col("pcs2.procedure_code") == F.col("pps.icd_10_pcs"),
            "inner",
        )
        .select(F.col("pcs2.encounter_id"))
        .distinct()
    )

    acute_encounters = (
        encounter_with_ccs.alias("dccs2")
        .join(
            acute_diagnosis_icd.alias("adi"),
            F.col("dccs2.primary_diagnosis_code") == F.col("adi.icd_10_cm"),
            "left",
        )
        .join(
            acute_diagnosis_ccs.alias("adc"),
            F.col("dccs2.ccs_diagnosis_category") == F.col("adc.ccs_diagnosis_category"),
            "left",
        )
        .filter(F.col("adi.icd_10_cm").isNotNull() | F.col("adc.ccs_diagnosis_category").isNotNull())
        .select(F.col("dccs2.encounter_id"))
        .distinct()
    )

    all_potentially_planned = pot_planned_px_ccs.unionByName(pot_planned_px_icd)

    actually_planned = (
        all_potentially_planned.alias("ppp")
        .join(
            acute_encounters.alias("ae"),
            F.col("ppp.encounter_id") == F.col("ae.encounter_id"),
            "left",
        )
        .filter(F.col("ae.encounter_id").isNull())
        .select(F.col("ppp.encounter_id"))
        .distinct()
    )

    result = (
        always_planned_px
        .withColumn("tuva_last_run", F.current_timestamp())
        .unionByName(always_planned_dx.withColumn("tuva_last_run", F.current_timestamp()))
        .unionByName(actually_planned.withColumn("tuva_last_run", F.current_timestamp()))
        .distinct()
    )

    return result
