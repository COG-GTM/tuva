import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    codes = spark.table("ccsr__dxccsr_v2023_1_cleaned_map").select(
        F.col("icd_10_cm_code").alias("code"),
        F.col("icd_10_cm_code_description").alias("code_description"),
        F.col("ccsr_category_1"),
        F.col("ccsr_category_1_description"),
        F.col("ccsr_category_2"),
        F.col("ccsr_category_2_description"),
        F.col("ccsr_category_3"),
        F.col("ccsr_category_3_description"),
        F.col("ccsr_category_4"),
        F.col("ccsr_category_4_description"),
        F.col("ccsr_category_5"),
        F.col("ccsr_category_5_description"),
        F.col("ccsr_category_6"),
        F.col("ccsr_category_6_description"),
        F.col("default_ccsr_category_ip"),
        F.col("default_ccsr_category_op"),
    )

    dfs = []
    for i in range(1, 7):
        cat_col = f"ccsr_category_{i}"
        desc_col = f"ccsr_category_{i}_description"
        df_i = codes.select(
            F.col("code"),
            F.col("code_description"),
            F.substring(F.col(cat_col), 1, 3).alias("ccsr_parent_category"),
            F.col(cat_col).alias("ccsr_category"),
            F.col(desc_col).alias("ccsr_category_description"),
            F.lit(i).alias("ccsr_category_rank"),
            F.when(F.col(cat_col) == F.col("default_ccsr_category_ip"), 1)
             .otherwise(0)
             .alias("is_ip_default_category"),
            F.when(F.col(cat_col) == F.col("default_ccsr_category_op"), 1)
             .otherwise(0)
             .alias("is_op_default_category"),
        )
        dfs.append(df_i)

    from functools import reduce
    long_union = reduce(DataFrame.unionAll, dfs)

    result = long_union.filter(F.col("ccsr_category").isNotNull()).distinct()

    result = result.withColumn(
        "tuva_last_run",
        F.current_timestamp()
    )

    return result
