import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    eligibility = spark.table("fhir_preprocessing__stg_core__eligibility")

    base = eligibility.select(
        F.col("person_id").alias("patient_internal_id"),
        F.md5(F.coalesce(F.col("eligibility_id").cast("string"), F.lit(""))).alias("resource_internal_id"),
        F.col("payer").alias("organization_name"),
        F.col("plan").alias("coverage_plan"),
        F.col("payer_type"),
        F.col("enrollment_start_date").alias("coverage_period_start"),
        F.col("enrollment_end_date").alias("coverage_period_end"),
        F.coalesce(F.col("subscriber_relation"), F.lit("self")).alias("coverage_relationship"),
        F.lit("active").alias("coverage_status"),
        F.coalesce(F.col("subscriber_id"), F.col("member_id")).alias("coverage_subscriber_id"),
        F.col("data_source"),
    )

    add_product = base.withColumn(
        "coverage_type_product",
        F.when(F.lower(F.col("payer_type")).like("%commercial%"), F.lit("PPO"))
         .when(F.lower(F.col("payer_type")).like("%self%"), F.lit("PPO"))
         .when(F.lower(F.col("payer_type")).like("%medicare%"), F.lit("MCR"))
         .when(F.lower(F.col("payer_type")).like("%medicaid%"), F.lit("MCD"))
         .when(F.lower(F.col("coverage_plan")).like("%pos&"), F.lit("POS"))
         .when(F.lower(F.col("coverage_plan")).like("%cep%"), F.lit("CEP"))
         .when(F.lower(F.col("coverage_plan")).like("%hmo%"), F.lit("HMO"))
         .when(F.lower(F.col("coverage_plan")).like("%MP%"), F.lit("MP"))
         .when(F.lower(F.col("coverage_plan")).like("%MC%"), F.lit("MC"))
         .when(F.lower(F.col("coverage_plan")).like("%SN1%"), F.lit("SN1"))
         .when(F.lower(F.col("coverage_plan")).like("%SN2%"), F.lit("SN2"))
         .when(F.lower(F.col("coverage_plan")).like("%SN3%"), F.lit("SN3"))
         .when(F.lower(F.col("coverage_plan")).like("%MCS%"), F.lit("MCS"))
         .when(F.lower(F.col("coverage_plan")).like("%MMP%"), F.lit("MMP"))
         .when(F.lower(F.col("coverage_plan")).like("%MDE%"), F.lit("MDE"))
         .when(F.lower(F.col("coverage_plan")).like("%MD%"), F.lit("MD"))
         .when(F.lower(F.col("coverage_plan")).like("%MLI%"), F.lit("MLI"))
         .when(F.lower(F.col("coverage_plan")).like("%MRB%"), F.lit("MRB"))
         .when(F.lower(F.col("coverage_plan")).like("%MMO%"), F.lit("MMO"))
         .when(F.lower(F.col("coverage_plan")).like("%MOS%"), F.lit("MOS"))
         .when(F.lower(F.col("coverage_plan")).like("%MPO%"), F.lit("MPO"))
         .when(F.lower(F.col("coverage_plan")).like("%MEP%"), F.lit("MEP"))
    )

    return add_product.select(
        "patient_internal_id",
        "resource_internal_id",
        "organization_name",
        "coverage_plan",
        "coverage_period_start",
        "coverage_period_end",
        "coverage_relationship",
        "coverage_status",
        "coverage_subscriber_id",
        "data_source",
        "coverage_type_product",
    )
