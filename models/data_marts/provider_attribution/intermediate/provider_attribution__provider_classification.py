import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    provider = spark.table("provider_attribution__stg_terminology__provider")
    taxonomy_crosswalk = spark.table("terminology__medicare_provider_and_supplier_taxonomy_crosswalk")
    assignment_codes = spark.table("cms_provider_attribution__provider_specialty_assignment_codes")

    # base: Map NPI to specialty description and entity type
    base = provider.select(
        F.col("npi").cast("string").alias("provider_id"),
        F.col("primary_taxonomy_code"),
        F.col("primary_specialty_description").alias("prov_specialty"),
        F.lower(F.trim(F.col("entity_type_description"))).alias("entity_type")
    )

    # mapped: Join taxonomy crosswalk and assignment codes to determine provider bucket
    mapped = (
        base.alias("b")
        .join(
            taxonomy_crosswalk.alias("x"),
            F.trim(F.col("b.primary_taxonomy_code").cast("string"))
            == F.trim(F.col("x.provider_taxonomy_code").cast("string")),
            "inner"
        )
        .join(
            assignment_codes.alias("a"),
            F.lpad(F.trim(F.col("x.medicare_specialty_code")), 2, "0")
            == F.lpad(F.trim(F.col("a.specialty_code")), 2, "0"),
            "inner"
        )
        .where(F.col("b.entity_type") == "individual")
        .select(
            F.col("b.provider_id"),
            F.col("b.prov_specialty"),
            F.when(
                (F.lower(F.col("a.primary_care_physician_step1")) == "yes") & (F.col("a.physician") == 1),
                F.lit("pcp")
            ).when(
                (F.lower(F.col("a.specialist_physician_step_2")) == "yes") & (F.col("a.physician") == 1),
                F.lit("specialist")
            ).when(
                F.col("a.physician") == 0,
                F.lit("npp")
            ).otherwise(F.lit("unknown")).alias("provider_bucket")
        )
    )

    # rnk: Prioritize provider bucket and pick the top-ranked row per provider
    prioritized = mapped.withColumn(
        "bucket_priority",
        F.when(F.col("provider_bucket") == "pcp", 1)
        .when(F.col("provider_bucket") == "npp", 2)
        .when(F.col("provider_bucket") == "specialist", 3)
        .otherwise(4)
    )

    w = Window.partitionBy("provider_id").orderBy("bucket_priority")

    rnk = prioritized.withColumn("bucket_rank", F.row_number().over(w))

    result = (
        rnk.where(F.col("bucket_rank") == 1)
        .select("provider_id", "prov_specialty", "provider_bucket")
    )

    return result
