import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    encounter = spark.table("readmissions__encounter")
    encounter_overlap = spark.table("readmissions__encounter_overlap")

    non_best = (
        encounter_overlap
        .filter(F.col("is_best_encounter") == 0)
        .select("encounter_id")
    )

    encounter_info = (
        encounter.alias("enc")
        .filter(
            F.col("admit_date").isNotNull()
            & F.col("discharge_date").isNotNull()
            & (F.col("admit_date") <= F.col("discharge_date"))
        )
        .join(
            non_best.alias("overlap"),
            F.col("enc.encounter_id") == F.col("overlap.encounter_id"),
            "left_anti",
        )
        .select("encounter_id", "person_id", "admit_date", "discharge_date")
    )

    seq_window = Window.partitionBy("person_id").orderBy("admit_date", "discharge_date")
    encounter_sequence = encounter_info.withColumn("encounter_seq", F.row_number().over(seq_window))

    aa = encounter_sequence.alias("aa")
    bb = encounter_sequence.alias("bb")

    days_to_readmit_expr = F.datediff(F.col("aa.discharge_date"), F.col("bb.admit_date"))

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
            F.when(F.col("bb.encounter_id").isNotNull(), 1).otherwise(0).alias("had_readmission_flag"),
            days_to_readmit_expr.alias("days_to_readmit"),
            F.when(days_to_readmit_expr <= 30, 1).otherwise(0).alias("readmit_30_flag"),
        )
    )

    result = readmission_calc.withColumn("tuva_last_run", F.current_timestamp())
    return result
