import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    encounter = spark.table("readmissions__encounter")
    index_admission = spark.table("readmissions__index_admission")
    planned_encounter = spark.table("readmissions__planned_encounter")
    specialty_cohort = spark.table("readmissions__encounter_specialty_cohort")
    data_quality = spark.table("readmissions__encounter_data_quality")

    aa = encounter.alias("aa")
    bb = index_admission.alias("bb")
    cc = planned_encounter.alias("cc")
    dd = specialty_cohort.alias("dd")
    ee = data_quality.alias("ee")

    joined = (
        aa
        .join(bb, F.col("aa.encounter_id") == F.col("bb.encounter_id"), "left")
        .join(cc, F.col("aa.encounter_id") == F.col("cc.encounter_id"), "left")
        .join(dd, F.col("aa.encounter_id") == F.col("dd.encounter_id"), "left")
        .join(ee, F.col("aa.encounter_id") == F.col("ee.encounter_id"), "left")
    )

    los_expr = F.datediff(F.col("aa.discharge_date"), F.col("aa.admit_date"))

    result = joined.select(
        F.col("aa.encounter_id"),
        F.col("aa.person_id"),
        F.col("aa.admit_date"),
        F.col("aa.discharge_date"),
        F.col("aa.discharge_disposition_code"),
        F.col("aa.facility_npi"),
        F.col("aa.drg_code_type"),
        F.col("aa.drg_code"),
        F.col("aa.paid_amount"),
        F.when(los_expr == 0, 1).otherwise(los_expr).alias("length_of_stay"),
        F.when(F.col("bb.encounter_id").isNotNull(), 1).otherwise(0).alias("index_admission_flag"),
        F.when(F.col("cc.encounter_id").isNotNull(), 1).otherwise(0).alias("planned_flag"),
        F.col("dd.specialty_cohort"),
        F.when(F.col("aa.discharge_disposition_code") == "20", 1).otherwise(0).alias("died_flag"),
        F.col("ee.diagnosis_ccs"),
        F.col("ee.disqualified_encounter_flag"),
        F.col("ee.missing_admit_date_flag"),
        F.col("ee.missing_discharge_date_flag"),
        F.col("ee.admit_after_discharge_flag"),
        F.col("ee.missing_discharge_disposition_code_flag"),
        F.col("ee.invalid_discharge_disposition_code_flag"),
        F.col("ee.missing_primary_diagnosis_flag"),
        F.col("ee.invalid_primary_diagnosis_code_flag"),
        F.col("ee.no_diagnosis_ccs_flag"),
        F.col("ee.overlaps_with_another_encounter_flag"),
        F.col("ee.missing_drg_flag"),
        F.col("ee.invalid_drg_flag"),
        F.col("aa.data_source"),
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
