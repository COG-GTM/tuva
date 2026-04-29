import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    ccsr_dx_vertical_pivot = spark.table("ccsr__dx_vertical_pivot")
    condition = spark.table("ccsr__stg_core__condition")
    dxccsr_body_systems = spark.table("ccsr__dxccsr_v2023_1_body_systems")

    result = (
        condition
        .join(
            ccsr_dx_vertical_pivot,
            condition["normalized_code"] == ccsr_dx_vertical_pivot["code"],
            "left_outer",
        )
        .join(
            dxccsr_body_systems,
            ccsr_dx_vertical_pivot["ccsr_parent_category"] == dxccsr_body_systems["ccsr_parent_category"],
            "left_outer",
        )
        .select(
            condition["encounter_id"],
            condition["claim_id"],
            condition["person_id"],
            condition["normalized_code"],
            ccsr_dx_vertical_pivot["code_description"],
            condition["condition_rank"],
            ccsr_dx_vertical_pivot["ccsr_parent_category"],
            dxccsr_body_systems["body_system"],
            dxccsr_body_systems["parent_category_description"],
            ccsr_dx_vertical_pivot["ccsr_category"],
            ccsr_dx_vertical_pivot["ccsr_category_description"],
            ccsr_dx_vertical_pivot["ccsr_category_rank"],
            ccsr_dx_vertical_pivot["is_ip_default_category"],
            ccsr_dx_vertical_pivot["is_op_default_category"],
            condition["data_source"],
            F.lit("v2023.1").alias("dxccsr_version"),
            F.current_timestamp().alias("tuva_last_run"),
        )
    )

    return result
