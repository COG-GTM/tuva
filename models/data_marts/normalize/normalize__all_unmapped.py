import os
import sys
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark, clinical_enabled=True):
    """Normalize: aggregate all unmapped codes across domains.

    Args:
        spark: SparkSession
        clinical_enabled: When True, includes appointment, lab_result,
            medication, and observation staging tables in the union.
    """
    parts = []

    if clinical_enabled:
        parts.append(spark.table("normalize__stg_unmapped_appointment"))
        parts.append(spark.table("normalize__stg_unmapped_lab_result"))
        parts.append(spark.table("normalize__stg_unmapped_medication"))
        parts.append(spark.table("normalize__stg_unmapped_observation"))

    parts.append(spark.table("normalize__stg_unmapped_condition"))
    parts.append(spark.table("normalize__stg_unmapped_procedure"))

    agg_cte = reduce(DataFrame.unionByName, parts)

    result = (
        agg_cte.alias("i")
        .groupBy(
            F.col("i.source_code_type"),
            F.col("i.source_code"),
            F.col("i.source_description"),
        )
        .agg(
            F.sum("item_count").alias("item_count"),
            F.concat_ws(
                ", ",
                F.sort_array(F.collect_set(F.col("domain"))),
            ).alias("domains"),
            F.concat_ws(
                ", ",
                F.sort_array(F.collect_set(F.col("data_source"))),
            ).alias("data_sources"),
        )
        .select(
            "source_code_type",
            "source_code",
            "source_description",
            "item_count",
            "domains",
            "data_sources",
            F.lit(None).cast("string").alias("normalized_code_type"),
            F.lit(None).cast("string").alias("normalized_code"),
            F.lit(None).cast("string").alias("normalized_description"),
            F.lit(None).cast("string").alias("not_mapped"),
            F.lit(None).cast("string").alias("added_by"),
            F.lit(None).cast("string").alias("added_date"),
            F.lit(None).cast("string").alias("reviewed_by"),
            F.lit(None).cast("string").alias("reviewed_date"),
            F.lit(None).cast("string").alias("notes"),
        )
    )

    return result
