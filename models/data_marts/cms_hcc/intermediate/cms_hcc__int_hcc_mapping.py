import os
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))


def run(spark: SparkSession) -> DataFrame:
    conditions = (
        spark.table("cms_hcc__int_eligible_conditions")
        .select("person_id", "payer", "condition_code", "payment_year",
                "collection_start_date", "collection_end_date")
    )

    # seed_hcc_mapping with next-year extension
    icd_base = spark.table("cms_hcc__icd_10_cm_mappings")
    max_py = icd_base.agg(F.max("payment_year").alias("max_py")).collect()[0]["max_py"]

    seed_current = icd_base.select(
        "payment_year", "diagnosis_code", "cms_hcc_v24", "cms_hcc_v24_flag",
        "cms_hcc_v28", "cms_hcc_v28_flag",
    )
    seed_next = (
        icd_base
        .where(F.col("payment_year") == max_py)
        .select(
            (F.col("payment_year") + 1).alias("payment_year"),
            "diagnosis_code", "cms_hcc_v24", "cms_hcc_v24_flag",
            "cms_hcc_v28", "cms_hcc_v28_flag",
        )
    )
    seed_hcc_mapping = seed_current.unionByName(seed_next)

    c = conditions.alias("c")
    s = seed_hcc_mapping.alias("s")

    # v24_mapped
    v24_mapped = (
        c.join(
            s,
            (F.col("c.condition_code") == F.col("s.diagnosis_code"))
            & (F.col("c.payment_year") == F.col("s.payment_year")),
            "inner",
        )
        .where(F.col("s.cms_hcc_v24_flag") == "Yes")
        .select(
            F.col("c.person_id"), F.col("c.payer"), F.col("c.condition_code"),
            F.col("c.payment_year"), F.col("c.collection_start_date"),
            F.col("c.collection_end_date"),
            F.lit("CMS-HCC-V24").alias("model_version"),
            F.col("s.cms_hcc_v24").cast("string").alias("hcc_code"),
        )
        .distinct()
    )

    # v28_mapped
    v28_mapped = (
        c.join(
            s,
            (F.col("c.condition_code") == F.col("s.diagnosis_code"))
            & (F.col("c.payment_year") == F.col("s.payment_year")),
            "inner",
        )
        .where(F.col("s.cms_hcc_v28_flag") == "Yes")
        .select(
            F.col("c.person_id"), F.col("c.payer"), F.col("c.condition_code"),
            F.col("c.payment_year"), F.col("c.collection_start_date"),
            F.col("c.collection_end_date"),
            F.lit("CMS-HCC-V28").alias("model_version"),
            F.col("s.cms_hcc_v28").cast("string").alias("hcc_code"),
        )
        .distinct()
    )

    # V28 Heart Interaction Patch
    v28_heart_sibling = (
        v28_mapped
        .where(F.col("hcc_code").isin("221", "222", "224", "225", "226"))
        .select("person_id", "payer", "payment_year", "collection_end_date")
        .distinct()
    )

    v28m = v28_mapped.alias("v28m")
    vhs = v28_heart_sibling.alias("vhs")

    v28_heart_patch = (
        v28m.join(
            vhs,
            (F.col("v28m.person_id") == F.col("vhs.person_id"))
            & (F.col("v28m.payer") == F.col("vhs.payer"))
            & (F.col("v28m.payment_year") == F.col("vhs.payment_year"))
            & (F.col("v28m.collection_end_date") == F.col("vhs.collection_end_date")),
            "left",
        )
        .where(
            ~((F.col("v28m.hcc_code") == "223") & F.col("vhs.person_id").isNull())
        )
        .select(
            F.col("v28m.person_id"), F.col("v28m.payer"), F.col("v28m.condition_code"),
            F.col("v28m.payment_year"), F.col("v28m.collection_start_date"),
            F.col("v28m.collection_end_date"), F.col("v28m.model_version"),
            F.col("v28m.hcc_code"),
        )
    )

    # union
    unioned = v24_mapped.unionByName(v28_heart_patch)

    # add_data_types
    add_data_types = unioned.select(
        F.col("person_id").cast("string"),
        F.col("payer").cast("string"),
        F.col("condition_code").cast("string"),
        F.col("hcc_code").cast("string"),
        F.col("model_version").cast("string"),
        F.col("payment_year").cast("int"),
        F.col("collection_start_date").cast("date"),
        F.col("collection_end_date").cast("date"),
    )

    result = add_data_types.select(
        "person_id", "payer", "condition_code", "hcc_code", "model_version",
        "payment_year", "collection_start_date", "collection_end_date",
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
