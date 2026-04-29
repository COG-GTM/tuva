import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    encounter_with_ccs = spark.table("readmissions__encounter_with_ccs")
    discharge_disposition = spark.table("terminology__discharge_disposition")
    ms_drg = spark.table("terminology__ms_drg")
    apr_drg = spark.table("terminology__apr_drg")
    encounter_overlap = spark.table("readmissions__encounter_overlap")

    best_encounter = (
        encounter_overlap
        .filter(F.col("is_best_encounter") == 0)
        .select("encounter_id")
        .distinct()
    )

    aa = encounter_with_ccs.alias("aa")
    bb = discharge_disposition.alias("bb")
    cc = ms_drg.alias("cc")
    dd = apr_drg.alias("dd")
    be = best_encounter.alias("be")

    joined = (
        aa
        .join(bb, F.col("aa.discharge_disposition_code") == F.col("bb.discharge_disposition_code"), "left")
        .join(
            cc,
            (F.col("aa.drg_code") == F.col("cc.ms_drg_code")) & (F.col("aa.drg_code_type") == F.lit("ms-drg")),
            "left",
        )
        .join(
            dd,
            (F.col("aa.drg_code") == F.col("dd.apr_drg_code")) & (F.col("aa.drg_code_type") == F.lit("apr-drg")),
            "left",
        )
        .join(be, F.col("aa.encounter_id") == F.col("be.encounter_id"), "left")
    )

    encounter_data_quality_issues = joined.select(
        F.col("aa.encounter_id"),
        F.when(F.col("aa.admit_date").isNull(), 1).otherwise(0).alias("missing_admit_date_flag"),
        F.when(F.col("aa.discharge_date").isNull(), 1).otherwise(0).alias("missing_discharge_date_flag"),
        F.when(F.col("aa.admit_date") > F.col("aa.discharge_date"), 1).otherwise(0).alias("admit_after_discharge_flag"),
        F.when(F.col("aa.discharge_disposition_code").isNull(), 1).otherwise(0).alias("missing_discharge_disposition_code_flag"),
        F.when(
            F.col("aa.discharge_disposition_code").isNotNull() & F.col("bb.discharge_disposition_code").isNull(),
            1,
        ).otherwise(0).alias("invalid_discharge_disposition_code_flag"),
        F.when(F.col("aa.primary_diagnosis_code").isNull(), 1).otherwise(0).alias("missing_primary_diagnosis_flag"),
        F.when(F.col("aa.valid_primary_diagnosis_code_flag") == 0, 1).otherwise(0).alias("invalid_primary_diagnosis_code_flag"),
        F.when(F.col("aa.ccs_diagnosis_category").isNull(), 1).otherwise(0).alias("no_diagnosis_ccs_flag"),
        F.col("aa.ccs_diagnosis_category").alias("diagnosis_ccs"),
        F.when(F.col("be.encounter_id").isNotNull(), 1).otherwise(0).alias("overlaps_with_another_encounter_flag"),
        F.when(F.col("aa.drg_code").isNull(), 1).otherwise(0).alias("missing_drg_flag"),
        F.when(
            F.coalesce(F.col("cc.ms_drg_code"), F.col("dd.apr_drg_code")).isNull(),
            1,
        ).otherwise(0).alias("invalid_drg_flag"),
    )

    all_data_quality_flags = encounter_data_quality_issues.select(
        "encounter_id",
        "diagnosis_ccs",
        F.when(
            (F.col("missing_admit_date_flag") == 1)
            | (F.col("missing_discharge_date_flag") == 1)
            | (F.col("admit_after_discharge_flag") == 1)
            | (F.col("missing_discharge_disposition_code_flag") == 1)
            | (F.col("invalid_discharge_disposition_code_flag") == 1)
            | (F.col("missing_primary_diagnosis_flag") == 1)
            | (F.col("invalid_primary_diagnosis_code_flag") == 1)
            | (F.col("no_diagnosis_ccs_flag") == 1)
            | (F.col("overlaps_with_another_encounter_flag") == 1)
            | (F.col("missing_drg_flag") == 1)
            | (F.col("invalid_drg_flag") == 1),
            1,
        ).otherwise(0).alias("disqualified_encounter_flag"),
        "missing_admit_date_flag",
        "missing_discharge_date_flag",
        "admit_after_discharge_flag",
        "missing_discharge_disposition_code_flag",
        "invalid_discharge_disposition_code_flag",
        "missing_primary_diagnosis_flag",
        "invalid_primary_diagnosis_code_flag",
        "no_diagnosis_ccs_flag",
        "overlaps_with_another_encounter_flag",
        "missing_drg_flag",
        "invalid_drg_flag",
    )

    result = all_data_quality_flags.withColumn("tuva_last_run", F.current_timestamp())
    return result
