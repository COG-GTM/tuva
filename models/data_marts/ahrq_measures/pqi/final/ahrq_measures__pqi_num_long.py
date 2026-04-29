import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    pqi_numbers = [
        ("ahrq_measures__int_pqi_01_num", "01"),
        ("ahrq_measures__int_pqi_03_num", "03"),
        ("ahrq_measures__int_pqi_05_num", "05"),
        ("ahrq_measures__int_pqi_07_num", "07"),
        ("ahrq_measures__int_pqi_08_num", "08"),
        ("ahrq_measures__int_pqi_11_num", "11"),
        ("ahrq_measures__int_pqi_12_num", "12"),
        ("ahrq_measures__int_pqi_14_num", "14"),
        ("ahrq_measures__int_pqi_15_num", "15"),
        ("ahrq_measures__int_pqi_16_num", "16"),
    ]

    dfs = []
    for table_name, pqi_num in pqi_numbers:
        df = (
            spark.table(table_name)
            .select(
                F.col("data_source"),
                F.col("person_id"),
                F.col("year_number"),
                F.col("encounter_id"),
                F.lit(pqi_num).alias("pqi_number"),
                F.current_timestamp().alias("tuva_last_run"),
            )
        )
        dfs.append(df)

    result = dfs[0]
    for df in dfs[1:]:
        result = result.unionByName(df)

    return result
