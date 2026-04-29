import os
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))


def run(spark: SparkSession) -> DataFrame:
    hcc_mapping = (
        spark.table("cms_hcc__int_hcc_mapping")
        .select("person_id", "payer", "hcc_code", "model_version",
                "payment_year", "collection_start_date", "collection_end_date")
        .distinct()
    )

    seed_hcc_hierarchy = (
        spark.table("cms_hcc__disease_hierarchy")
        .select("model_version", "hcc_code", "description", "hccs_to_exclude")
    )

    hm = hcc_mapping.alias("hm")
    htop = seed_hcc_hierarchy.alias("htop")
    hexc = seed_hcc_hierarchy.alias("hexc")

    # hccs_without_hierarchy
    hccs_without_hierarchy = (
        hm.join(
            htop,
            (F.col("hm.hcc_code") == F.col("htop.hcc_code"))
            & (F.col("hm.model_version") == F.col("htop.model_version")),
            "left",
        )
        .join(
            hexc,
            (F.col("hm.hcc_code") == F.col("hexc.hccs_to_exclude"))
            & (F.col("hm.model_version") == F.col("hexc.model_version")),
            "left",
        )
        .where(
            F.col("htop.hcc_code").isNull()
            & F.col("hexc.hccs_to_exclude").isNull()
        )
        .select(
            F.col("hm.person_id"), F.col("hm.payer"), F.col("hm.model_version"),
            F.col("hm.payment_year"), F.col("hm.collection_start_date"),
            F.col("hm.collection_end_date"), F.col("hm.hcc_code"),
        )
        .distinct()
    )

    # hccs_with_hierarchy
    sh = seed_hcc_hierarchy.alias("sh")
    hccs_with_hierarchy = (
        hm.join(
            sh,
            (F.col("hm.hcc_code") == F.col("sh.hccs_to_exclude"))
            & (F.col("hm.model_version") == F.col("sh.model_version")),
            "inner",
        )
        .select(
            F.col("hm.person_id"), F.col("hm.payer"), F.col("hm.model_version"),
            F.col("hm.payment_year"), F.col("hm.collection_start_date"),
            F.col("hm.collection_end_date"), F.col("hm.hcc_code"),
            F.col("sh.hcc_code").alias("top_level_hcc"),
        )
    )

    # hierarchy_applied
    hwh = hccs_with_hierarchy.alias("hwh")
    hm2 = hcc_mapping.alias("hm2")

    hierarchy_applied = (
        hwh.join(
            hm2,
            (F.col("hm2.person_id") == F.col("hwh.person_id"))
            & (F.col("hm2.payer") == F.col("hwh.payer"))
            & (F.col("hm2.hcc_code") == F.col("hwh.top_level_hcc"))
            & (F.col("hm2.model_version") == F.col("hwh.model_version"))
            & (F.col("hm2.payment_year") == F.col("hwh.payment_year"))
            & (F.col("hm2.collection_end_date") == F.col("hwh.collection_end_date")),
            "left",
        )
        .groupBy(
            F.col("hwh.person_id"), F.col("hwh.payer"), F.col("hwh.model_version"),
            F.col("hwh.payment_year"), F.col("hwh.collection_start_date"),
            F.col("hwh.collection_end_date"), F.col("hwh.hcc_code"),
        )
        .agg(F.min(F.col("hm2.hcc_code").cast("int")).alias("top_level_hcc_num"))
    )

    # lower_level_inclusions
    lower_level_inclusions = (
        hierarchy_applied
        .withColumn(
            "hcc_code",
            F.when(F.col("top_level_hcc_num").isNotNull(),
                   F.col("top_level_hcc_num").cast("string"))
            .otherwise(F.col("hcc_code")),
        )
        .select("person_id", "payer", "model_version", "payment_year",
                "collection_start_date", "collection_end_date", "hcc_code")
        .distinct()
    )

    # top_level_inclusions
    hm3 = hcc_mapping.alias("hm3")
    sh2 = seed_hcc_hierarchy.alias("sh2")
    lli = lower_level_inclusions.alias("lli")
    ha = hierarchy_applied.alias("ha")

    top_level_inclusions = (
        hm3.join(
            sh2,
            (F.col("hm3.hcc_code") == F.col("sh2.hcc_code"))
            & (F.col("hm3.model_version") == F.col("sh2.model_version")),
            "inner",
        )
        .join(
            lli,
            (F.col("hm3.person_id") == F.col("lli.person_id"))
            & (F.col("hm3.payer") == F.col("lli.payer"))
            & (F.col("hm3.hcc_code") == F.col("lli.hcc_code"))
            & (F.col("hm3.model_version") == F.col("lli.model_version"))
            & (F.col("hm3.payment_year") == F.col("lli.payment_year"))
            & (F.col("hm3.collection_end_date") == F.col("lli.collection_end_date")),
            "left",
        )
        .join(
            ha,
            (F.col("hm3.person_id") == F.col("ha.person_id"))
            & (F.col("hm3.payer") == F.col("ha.payer"))
            & (F.col("hm3.hcc_code") == F.col("ha.hcc_code"))
            & (F.col("hm3.model_version") == F.col("ha.model_version"))
            & (F.col("hm3.payment_year") == F.col("ha.payment_year"))
            & (F.col("hm3.collection_end_date") == F.col("ha.collection_end_date")),
            "left",
        )
        .where(
            F.col("lli.hcc_code").isNull()
            & F.col("ha.top_level_hcc_num").isNull()
        )
        .select(
            F.col("hm3.person_id"), F.col("hm3.payer"), F.col("hm3.model_version"),
            F.col("hm3.payment_year"), F.col("hm3.collection_start_date"),
            F.col("hm3.collection_end_date"), F.col("hm3.hcc_code"),
        )
        .distinct()
    )

    # union all three
    unioned = (
        hccs_without_hierarchy
        .unionByName(lower_level_inclusions)
        .unionByName(top_level_inclusions)
    )

    # add_data_types
    add_data_types = unioned.select(
        F.col("person_id").cast("string"),
        F.col("payer").cast("string"),
        F.col("model_version").cast("string"),
        F.col("payment_year").cast("int"),
        F.col("collection_start_date").cast("date"),
        F.col("collection_end_date").cast("date"),
        F.col("hcc_code").cast("string"),
    )

    result = add_data_types.select(
        "person_id", "payer", "model_version", "payment_year",
        "collection_start_date", "collection_end_date", "hcc_code",
        F.current_timestamp().alias("tuva_last_run"),
    )

    return result
