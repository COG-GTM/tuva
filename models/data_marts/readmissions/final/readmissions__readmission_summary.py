import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    encounter_augmented = spark.table("readmissions__encounter_augmented")

    filtered = encounter_augmented.filter(F.col("disqualified_encounter_flag") == 0)

    seq_window = Window.partitionBy("person_id").orderBy("admit_date", "discharge_date")
    encounter_sequence = filtered.withColumn("encounter_seq", F.row_number().over(seq_window))

    aa = encounter_sequence.alias("aa")
    bb = encounter_sequence.alias("bb")

    days_to_readmit_expr = F.datediff(F.col("bb.admit_date"), F.col("aa.discharge_date"))

    readmission_calc = (
        aa.join(
            bb,
            (F.col("aa.person_id") == F.col("bb.person_id"))
            & (F.col("aa.encounter_seq") + 1 == F.col("bb.encounter_seq")),
            "left",
        )
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
            F.col("aa.length_of_stay"),
            F.col("aa.index_admission_flag"),
            F.col("aa.planned_flag"),
            F.col("aa.specialty_cohort"),
            F.col("aa.died_flag"),
            F.col("aa.diagnosis_ccs"),
            F.when(F.col("bb.encounter_id").isNotNull(), 1).otherwise(0).alias("had_readmission_flag"),
            days_to_readmit_expr.alias("days_to_readmit"),
            F.when(days_to_readmit_expr <= 30, 1).otherwise(0).alias("readmit_30_flag"),
            F.when(
                (days_to_readmit_expr <= 30) & (F.col("bb.planned_flag") == 0),
                1,
            ).otherwise(0).alias("unplanned_readmit_30_flag"),
            F.col("bb.encounter_id").alias("readmission_encounter_id"),
            F.col("bb.admit_date").alias("readmission_admit_date"),
            F.col("bb.discharge_date").alias("readmission_discharge_date"),
            F.col("bb.discharge_disposition_code").alias("readmission_discharge_disposition_code"),
            F.col("bb.facility_npi").alias("readmission_facility"),
            F.col("bb.drg_code_type").alias("readmission_drg_code_type"),
            F.col("bb.drg_code").alias("readmission_drg"),
            F.col("bb.length_of_stay").alias("readmission_length_of_stay"),
            F.col("bb.index_admission_flag").alias("readmission_index_admission_flag"),
            F.col("bb.planned_flag").alias("readmission_planned_flag"),
            F.col("bb.specialty_cohort").alias("readmission_specialty_cohort"),
            F.col("bb.died_flag").alias("readmission_died_flag"),
            F.col("bb.diagnosis_ccs").alias("readmission_diagnosis_ccs"),
            F.current_timestamp().alias("tuva_last_run"),
        )
    )

    return readmission_calc
