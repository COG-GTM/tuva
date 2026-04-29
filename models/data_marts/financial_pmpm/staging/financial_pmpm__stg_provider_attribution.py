import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    provider_attribution_enabled = False

    if provider_attribution_enabled:
        src = spark.table("input_layer__provider_attribution")

        return src.select(
            F.col("person_id").cast("string").alias("person_id"),
            F.col("member_id").cast("string").alias("member_id"),
            F.col("year_month").cast("string").alias("year_month"),
            F.col("payer").cast("string").alias("payer"),
            F.col("plan"),
            F.col("data_source").cast("string").alias("data_source"),
            F.col("payer_attributed_provider").cast("string").alias("payer_attributed_provider"),
            F.col("payer_attributed_provider_practice").cast("string").alias("payer_attributed_provider_practice"),
            F.col("payer_attributed_provider_organization").cast("string").alias("payer_attributed_provider_organization"),
            F.col("payer_attributed_provider_lob").cast("string").alias("payer_attributed_provider_lob"),
            F.col("custom_attributed_provider").cast("string").alias("custom_attributed_provider"),
            F.col("custom_attributed_provider_practice").cast("string").alias("custom_attributed_provider_practice"),
            F.col("custom_attributed_provider_organization").cast("string").alias("custom_attributed_provider_organization"),
            F.col("custom_attributed_provider_lob").cast("string").alias("custom_attributed_provider_lob"),
            F.current_timestamp().alias("tuva_last_run"),
        )
    else:
        schema = "person_id string, member_id string, year_month string, payer string, " \
                 "plan string, data_source string, payer_attributed_provider string, " \
                 "payer_attributed_provider_practice string, payer_attributed_provider_organization string, " \
                 "payer_attributed_provider_lob string, custom_attributed_provider string, " \
                 "custom_attributed_provider_practice string, custom_attributed_provider_organization string, " \
                 "custom_attributed_provider_lob string, tuva_last_run string"
        return spark.createDataFrame([], schema)
