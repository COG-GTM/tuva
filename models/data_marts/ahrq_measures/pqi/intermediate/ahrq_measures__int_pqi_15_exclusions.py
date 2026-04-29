import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    condition = spark.table("ahrq_measures__stg_pqi_condition")
    pqi_vs = spark.table("pqi__value_sets")
    shared_excl = spark.table("ahrq_measures__int_pqi_shared_exclusion_union")

    resp_an = (
        condition.alias("c")
        .join(
            pqi_vs.alias("pqi"),
            (F.col("c.normalized_code") == F.col("pqi.code"))
            & (F.col("c.normalized_code_type") == F.lit("icd-10-cm"))
            & (F.col("pqi.value_set_name") == F.lit("cystic_fibrosis_and_anomalies_of_the_respiratory_system_diagnosis_codes"))
            & (F.col("pqi.pqi_number") == F.lit("15")),
            "inner",
        )
        .where(F.col("c.encounter_id").isNotNull())
        .select(
            F.col("c.encounter_id"),
            F.col("c.data_source"),
            F.lit("cystic fibrosis").alias("exclusion_reason"),
        )
        .distinct()
    )

    shared_part = shared_excl.select("encounter_id", "data_source", "exclusion_reason")

    union_cte = shared_part.unionByName(resp_an)

    w = Window.partitionBy("encounter_id", "data_source").orderBy("exclusion_reason")

    result = union_cte.select(
        F.col("encounter_id"),
        F.col("data_source"),
        F.col("exclusion_reason"),
        F.row_number().over(w).alias("exclusion_number"),
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
