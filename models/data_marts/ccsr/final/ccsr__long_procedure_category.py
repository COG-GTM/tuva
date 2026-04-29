import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    procedures = spark.table("ccsr__stg_core__procedure")
    procedure_category_map = spark.table("ccsr__procedure_category_map")

    result = (
        procedures
        .join(
            procedure_category_map,
            procedures["normalized_code"] == procedure_category_map["code"],
            "inner",
        )
        .select(
            procedures["encounter_id"],
            procedures["claim_id"],
            procedures["person_id"],
            procedures["normalized_code"],
            procedure_category_map["code_description"],
            procedure_category_map["ccsr_parent_category"],
            procedure_category_map["ccsr_category"],
            procedure_category_map["ccsr_category_description"],
            procedure_category_map["clinical_domain"],
            procedure_category_map["procedure_section"],
            procedure_category_map["operation"],
            procedure_category_map["approach"],
            procedure_category_map["device"],
            procedure_category_map["qualifier"],
            procedures["data_source"],
            F.lit("v2023.1").alias("prccsr_version"),
            F.current_timestamp().alias("tuva_last_run"),
        )
        .distinct()
    )

    return result
