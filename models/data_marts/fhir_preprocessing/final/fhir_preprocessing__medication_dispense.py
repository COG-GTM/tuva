import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    med = spark.table("fhir_preprocessing__stg_core__medication")

    return med.select(
        F.col("person_id").cast("string").alias("patient_internal_id"),
        F.col("medication_id").cast("string").alias("resource_internal_id"),
        F.lit("completed").alias("medication_dispense_status"),
        F.when(F.col("ndc_code").isNotNull(), F.lit("NDC"))
         .when(F.col("rxnorm_code").isNotNull(), F.lit("RXNORM"))
         .otherwise(F.col("source_code_type").cast("string"))
         .alias("medication_code_system"),
        F.coalesce(
            F.col("ndc_code").cast("string"),
            F.col("rxnorm_code").cast("string"),
        ).alias("medication_code"),
        F.coalesce(
            F.col("ndc_description").cast("string"),
            F.col("rxnorm_description").cast("string"),
        ).alias("medication_code_display"),
        F.col("days_supply").cast("decimal(28,6)").alias("medication_dispense_days_supply_value"),
        F.col("dispensing_date").cast("date").alias("medication_dispense_when_handed_over"),
        F.col("data_source").cast("string").alias("data_source"),
    )
