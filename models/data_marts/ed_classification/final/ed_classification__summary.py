import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import (
    safe_cast_date,
    safe_cast_timestamp,
    safe_cast_int,
    year_month,
    apply_regex,
    substring_col,
    create_json_object,
    union_relations,
)


def run(spark: SparkSession) -> DataFrame:
    class_df = spark.table("ed_classification__int_filter_encounter_with_classification")
    cat = spark.table("ed_classification__categories")
    fac_prov = spark.table("terminology__provider")
    pat = spark.table("ed_classification__stg_patient")

    joined = (
        class_df.alias("class")
        .join(
            cat.alias("cat"),
            F.col("class.classification") == F.col("cat.classification"),
            "inner",
        )
        .join(
            fac_prov.alias("fac_prov"),
            F.col("class.facility_npi") == F.col("fac_prov.npi"),
            "left_outer",
        )
        .join(
            pat.alias("pat"),
            F.col("class.person_id") == F.col("pat.person_id"),
            "left_outer",
        )
    )

    # year_month: concat(year, right('00' || month, 2))  →  YYYYMM
    year_part = F.year(F.col("class.encounter_end_date")).cast("string")
    month_part = F.lpad(F.month(F.col("class.encounter_end_date")).cast("string"), 2, "0")
    year_month_col = F.concat(year_part, month_part)

    # patient_age: floor(datediff_hours(birth_date, encounter_end_date) / 8766.0)
    age_col = F.floor(
        (
            F.unix_timestamp(F.col("class.encounter_end_date").cast("timestamp"))
            - F.unix_timestamp(F.col("pat.birth_date").cast("timestamp"))
        )
        / 3600.0
        / 8766.0
    )

    result = joined.select(
        F.col("class.encounter_id"),
        F.col("cat.classification_name").alias("ed_classification_description"),
        F.col("cat.classification_order").alias("ed_classification_order"),
        F.col("class.person_id"),
        F.col("class.encounter_end_date"),
        year_month_col.alias("year_month"),
        F.col("class.primary_diagnosis_code"),
        F.col("class.primary_diagnosis_description"),
        F.col("class.paid_amount"),
        F.col("class.allowed_amount"),
        F.col("class.charge_amount"),
        F.col("class.facility_npi"),
        F.col("fac_prov.provider_organization_name").alias("facility_name"),
        F.col("fac_prov.practice_state").alias("facility_state"),
        F.col("fac_prov.practice_city").alias("facility_city"),
        F.col("fac_prov.practice_zip_code").alias("facility_zip_code"),
        F.col("pat.sex").alias("patient_sex"),
        age_col.alias("patient_age"),
        F.col("pat.zip_code").alias("patient_zip_code"),
        F.col("pat.latitude").alias("patient_latitude"),
        F.col("pat.longitude").alias("patient_longitude"),
        F.col("pat.race").alias("patient_race"),
        F.col("pat.data_source"),
    )

    return result
