import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    encounter = spark.table("readmissions__encounter")
    icd_10_cm = spark.table("terminology__icd_10_cm")
    icd_10_cm_to_ccs = spark.table("readmissions__icd_10_cm_to_ccs")

    aa = encounter.alias("aa")
    bb = icd_10_cm.alias("bb")
    cc = icd_10_cm_to_ccs.alias("cc")

    result = (
        aa
        .join(bb, F.col("aa.primary_diagnosis_code") == F.col("bb.icd_10_cm"), "left")
        .join(cc, F.col("aa.primary_diagnosis_code") == F.col("cc.icd_10_cm"), "left")
        .select(
            F.col("aa.encounter_id"),
            F.col("aa.person_id"),
            F.col("aa.admit_date"),
            F.col("aa.discharge_date"),
            F.col("aa.discharge_disposition_code"),
            F.col("aa.facility_npi"),
            F.col("aa.drg_code_type"),
            F.col("aa.drg_code"),
            F.col("aa.paid_amount"),
            F.col("aa.primary_diagnosis_code"),
            F.when(F.col("bb.icd_10_cm").isNotNull(), 1).otherwise(0).alias("valid_primary_diagnosis_code_flag"),
            F.col("cc.ccs_diagnosis_category"),
            F.current_timestamp().alias("tuva_last_run"),
        )
    )

    return result
