import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    encounter = spark.table("readmissions__encounter")
    procedure_ccs = spark.table("readmissions__procedure_ccs")
    surgery_gyn_cohort = spark.table("readmissions__surgery_gynecology_cohort")
    specialty_cohort = spark.table("readmissions__specialty_cohort")
    encounter_with_ccs = spark.table("readmissions__encounter_with_ccs")

    cohort_ranks_data = [
        ("Surgery/Gynecology", 1),
        ("Cardiorespiratory", 2),
        ("Cardiovascular", 3),
        ("Neurology", 4),
        ("Medicine", 5),
    ]
    cohort_ranks = spark.createDataFrame(cohort_ranks_data, ["cohort", "c_rank"])

    proc_cohorts = (
        procedure_ccs.alias("procs")
        .join(
            surgery_gyn_cohort.alias("sgc"),
            F.col("procs.procedure_code") == F.col("sgc.icd_10_pcs"),
            "left",
        )
        .join(
            specialty_cohort.filter(F.col("specialty_cohort") == "Surgery/Gynecology").alias("sgsc"),
            F.col("procs.ccs_procedure_category") == F.col("sgsc.ccs"),
            "left",
        )
        .filter(F.col("sgc.icd_10_pcs").isNotNull() | F.col("sgsc.ccs").isNotNull())
        .select(F.col("procs.encounter_id"), F.lit(1).alias("c_rank"))
    )

    diag_cohorts = (
        encounter_with_ccs.alias("diag")
        .join(
            specialty_cohort.filter(F.col("procedure_or_diagnosis") == "Diagnosis").alias("sc"),
            F.col("diag.ccs_diagnosis_category") == F.col("sc.ccs"),
            "inner",
        )
        .join(
            cohort_ranks.alias("cr"),
            F.col("sc.specialty_cohort") == F.col("cr.cohort"),
            "inner",
        )
        .select(F.col("diag.encounter_id"), F.col("cr.c_rank"))
    )

    all_encounter_cohorts = proc_cohorts.unionByName(diag_cohorts)

    main_encounter_cohort = (
        all_encounter_cohorts
        .groupBy("encounter_id")
        .agg(F.min("c_rank").alias("main_c_rank"))
    )

    result = (
        encounter.alias("enc")
        .join(
            main_encounter_cohort.alias("mec"),
            F.col("enc.encounter_id") == F.col("mec.encounter_id"),
            "left",
        )
        .join(
            cohort_ranks.alias("cr2"),
            F.col("mec.main_c_rank") == F.col("cr2.c_rank"),
            "left",
        )
        .select(
            F.col("enc.encounter_id"),
            F.coalesce(F.col("cr2.cohort"), F.lit("Medicine")).alias("specialty_cohort"),
            F.current_timestamp().alias("tuva_last_run"),
        )
    )

    return result
