import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    encounter = spark.table("readmissions__encounter")

    encounter_enhanced = (
        encounter
        .withColumn(
            "actual_length_of_stay",
            F.coalesce(
                F.when(
                    F.datediff(F.col("admit_date"), F.col("discharge_date")) >= 1,
                    F.datediff(F.col("admit_date"), F.col("discharge_date")),
                ),
                F.lit(1),
            ),
        )
        .withColumn(
            "source_type_priority",
            F.when(
                F.upper(F.trim(F.coalesce(F.col("encounter_source_type"), F.lit("")))) == "CLAIM",
                1,
            ).otherwise(2),
        )
        .withColumn(
            "completeness_score",
            (
                F.when(
                    F.col("discharge_disposition_code").isNotNull()
                    & (F.trim(F.col("discharge_disposition_code")) != F.lit(""))
                    & (F.trim(F.col("discharge_disposition_code")) != F.lit("00")),
                    1,
                ).otherwise(0)
                + F.when(
                    F.col("drg_code_type").isNotNull()
                    & (F.trim(F.col("drg_code_type")) != F.lit("")),
                    1,
                ).otherwise(0)
                + F.when(
                    F.col("drg_code").isNotNull()
                    & (F.trim(F.col("drg_code")) != F.lit(""))
                    & (~F.trim(F.col("drg_code")).isin("998", "999")),
                    1,
                ).otherwise(0)
                + F.when(F.col("paid_amount").isNotNull(), 1).otherwise(0)
                + F.when(
                    F.col("primary_diagnosis_code").isNotNull()
                    & (F.trim(F.col("primary_diagnosis_code")) != F.lit("")),
                    1,
                ).otherwise(0)
            ),
        )
    )

    e1 = encounter_enhanced.alias("e1")
    e2 = encounter_enhanced.alias("e2")

    overlapping_encounters = (
        e1.join(
            e2,
            (F.col("e1.person_id") == F.col("e2.person_id"))
            & (F.col("e1.encounter_id") != F.col("e2.encounter_id"))
            & (F.col("e1.admit_date") <= F.col("e2.discharge_date"))
            & (F.col("e1.discharge_date") >= F.col("e2.admit_date")),
            "inner",
        )
        .select(
            F.col("e1.encounter_id"),
            F.col("e1.person_id"),
            F.col("e1.admit_date"),
            F.col("e1.discharge_date"),
        )
        .distinct()
    )

    oe1 = overlapping_encounters.alias("oe1")
    oe2 = overlapping_encounters.alias("oe2")

    overlap_pairs = (
        oe1.join(
            oe2,
            (F.col("oe1.person_id") == F.col("oe2.person_id"))
            & (F.col("oe1.admit_date") <= F.col("oe2.discharge_date"))
            & (F.col("oe1.discharge_date") >= F.col("oe2.admit_date")),
            "inner",
        )
        .select(
            F.col("oe1.encounter_id").alias("encounter_id"),
            F.col("oe1.person_id").alias("person_id"),
            F.col("oe2.encounter_id").alias("overlap_encounter_id"),
        )
        .distinct()
    )

    min_window = Window.partitionBy("person_id", "encounter_id")
    overlap_groups = (
        overlap_pairs
        .withColumn("overlap_group_id", F.min("overlap_encounter_id").over(min_window))
        .select("encounter_id", "person_id", "overlap_group_id")
        .distinct()
    )

    e = encounter_enhanced.alias("e")
    og = overlap_groups.alias("og")

    encounter_with_groups = (
        e.join(
            og,
            (F.col("e.encounter_id") == F.col("og.encounter_id"))
            & (F.col("e.person_id") == F.col("og.person_id")),
            "left",
        )
        .select(
            F.col("e.*"),
            F.coalesce(F.col("og.overlap_group_id"), F.col("e.encounter_id")).alias("overlap_group_id"),
            F.when(F.col("og.encounter_id").isNotNull(), 1).otherwise(0).alias("has_overlaps"),
        )
    )

    rank_window = Window.partitionBy("person_id", "overlap_group_id").orderBy(
        F.col("source_type_priority").asc(),
        F.col("actual_length_of_stay").desc(),
        F.col("completeness_score").desc(),
        F.col("encounter_id").asc(),
    )

    encounter_rankings = encounter_with_groups.withColumn(
        "encounter_rank_in_group", F.row_number().over(rank_window)
    )

    result = encounter_rankings.select(
        "encounter_id",
        "person_id",
        "admit_date",
        "discharge_date",
        "actual_length_of_stay",
        "source_type_priority",
        "completeness_score",
        "overlap_group_id",
        "has_overlaps",
        "encounter_rank_in_group",
        F.when(F.col("encounter_rank_in_group") == 1, 1).otherwise(0).alias("is_best_encounter"),
        F.when(
            (F.col("encounter_rank_in_group") == 1) & (F.col("has_overlaps") == 1),
            F.lit("Selected as best among overlapping encounters"),
        )
        .when(
            (F.col("encounter_rank_in_group") == 1) & (F.col("has_overlaps") == 0),
            F.lit("No overlapping encounters"),
        )
        .otherwise(F.lit("Not selected - better encounter exists"))
        .alias("selection_reason"),
    )

    return result
