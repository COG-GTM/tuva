import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    medical_claim = spark.table("fhir_preprocessing__stg_core__medical_claim")

    modifier_dfs = []
    for mod_col in ["hcpcs_modifier_1", "hcpcs_modifier_2", "hcpcs_modifier_3",
                     "hcpcs_modifier_4", "hcpcs_modifier_5"]:
        mod_df = (
            medical_claim.filter(F.col(mod_col).isNotNull())
            .select(
                F.col("claim_id"),
                F.col("claim_line_number"),
                F.lit("CPT").alias("eob_item_modifier_system"),
                F.col(mod_col).alias("eob_item_modifier_code"),
            )
        )
        modifier_dfs.append(mod_df)

    from functools import reduce
    unioned = reduce(lambda a, b: a.unionByName(b), modifier_dfs)

    return create_json_object(
        df=unioned,
        group_cols=["claim_id", "claim_line_number"],
        obj_col="eob_item_modifier_list",
        obj_fields=["eob_item_modifier_system", "eob_item_modifier_code"],
    )
