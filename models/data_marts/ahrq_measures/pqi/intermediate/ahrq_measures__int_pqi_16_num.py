import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    condition = spark.table("ahrq_measures__stg_pqi_condition")
    procedure = spark.table("ahrq_measures__stg_pqi_procedure")
    pqi_vs = spark.table("pqi__value_sets")
    e = spark.table("ahrq_measures__stg_pqi_inpatient_encounter")
    denom = spark.table("ahrq_measures__int_pqi_16_denom")
    excl = spark.table("ahrq_measures__int_pqi_16_exclusions")

    diagnosis = (
        condition.alias("c")
        .join(
            pqi_vs.alias("pqi"),
            (F.col("c.normalized_code") == F.col("pqi.code"))
            & (F.col("c.normalized_code_type") == F.lit("icd-10-cm"))
            & (F.col("pqi.value_set_name") == F.lit("diabetes_diagnosis_codes"))
            & (F.col("pqi.pqi_number") == F.lit("16")),
            "inner",
        )
        .where(F.col("c.encounter_id").isNotNull())
        .select(F.col("c.encounter_id"), F.col("c.data_source"))
        .distinct()
    )

    procedures = (
        procedure.alias("p")
        .join(
            diagnosis.alias("d"),
            (F.col("p.encounter_id") == F.col("d.encounter_id"))
            & (F.col("d.data_source") == F.col("p.data_source")),
            "inner",
        )
        .join(
            pqi_vs.alias("pqi"),
            (F.col("p.normalized_code") == F.col("pqi.code"))
            & (F.col("p.normalized_code_type") == F.lit("icd-10-pcs"))
            & (F.col("pqi.value_set_name") == F.lit("lower-extremity_amputation_procedure_codes"))
            & (F.col("pqi.pqi_number") == F.lit("16")),
            "inner",
        )
        .select(F.col("p.encounter_id"), F.col("p.data_source"))
        .distinct()
    )

    result = (
        e.alias("e")
        .join(
            denom.alias("denom"),
            (F.col("e.person_id") == F.col("denom.person_id"))
            & (F.col("e.data_source") == F.col("denom.data_source"))
            & (F.col("e.year_number") == F.col("denom.year_number")),
            "inner",
        )
        .join(
            procedures.alias("p"),
            (F.col("e.encounter_id") == F.col("p.encounter_id"))
            & (F.col("e.data_source") == F.col("p.data_source")),
            "inner",
        )
        .join(
            excl.alias("shared"),
            (F.col("e.encounter_id") == F.col("shared.encounter_id"))
            & (F.col("e.data_source") == F.col("shared.data_source")),
            "left",
        )
        .where(F.col("shared.encounter_id").isNull())
        .select(
            F.col("e.data_source"),
            F.col("e.person_id"),
            F.col("e.year_number"),
            F.col("e.encounter_id"),
            F.current_timestamp().alias("tuva_last_run"),
        )
    )

    return result
