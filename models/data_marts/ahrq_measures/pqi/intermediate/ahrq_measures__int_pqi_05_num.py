import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    e = spark.table("ahrq_measures__stg_pqi_inpatient_encounter")
    denom = spark.table("ahrq_measures__int_pqi_05_denom")
    pqi_vs = spark.table("pqi__value_sets")
    excl = spark.table("ahrq_measures__int_pqi_05_exclusions")

    copd = pqi_vs.where(
        (F.col("value_set_name") == F.lit("chronic_obstructive_pulmonary_disorder"))
        & (F.col("pqi_number") == F.lit("05"))
    ).select(F.col("code").alias("copd_code"))

    asthma = pqi_vs.where(
        (F.col("value_set_name") == F.lit("asthma"))
        & (F.col("pqi_number") == F.lit("05"))
    ).select(F.col("code").alias("asthma_code"))

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
            copd.alias("copd"),
            F.col("e.primary_diagnosis_code") == F.col("copd.copd_code"),
            "left",
        )
        .join(
            asthma.alias("asthma"),
            F.col("e.primary_diagnosis_code") == F.col("asthma.asthma_code"),
            "left",
        )
        .join(
            excl.alias("shared"),
            (F.col("e.encounter_id") == F.col("shared.encounter_id"))
            & (F.col("e.data_source") == F.col("shared.data_source")),
            "left",
        )
        .where(
            (F.col("shared.encounter_id").isNull())
            & (F.col("asthma.asthma_code").isNotNull() | F.col("copd.copd_code").isNotNull())
        )
        .select(
            F.col("e.data_source"),
            F.col("e.person_id"),
            F.col("e.year_number"),
            F.col("e.encounter_id"),
            F.current_timestamp().alias("tuva_last_run"),
        )
    )

    return result
