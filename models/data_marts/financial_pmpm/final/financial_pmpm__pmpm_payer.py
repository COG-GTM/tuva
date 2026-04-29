import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    a = spark.table("financial_pmpm__pmpm_prep")

    paid_cols = [
        "total_paid", "medical_paid", "inpatient_paid", "outpatient_paid",
        "office_based_paid", "ancillary_paid", "other_paid", "pharmacy_paid",
        "acute_inpatient_paid", "ambulance_paid", "ambulatory_surgery_center_paid",
        "dialysis_paid", "durable_medical_equipment_paid", "emergency_department_paid",
        "home_health_paid", "inpatient_hospice_paid", "inpatient_psychiatric_paid",
        "inpatient_rehabilitation_paid", "lab_paid", "observation_paid",
        "office_based_other_paid", "office_based_pt_ot_st_paid",
        "office_based_radiology_paid", "office_based_surgery_paid",
        "office_based_visit_paid", "outpatient_hospital_or_clinic_paid",
        "outpatient_pt_ot_st_paid", "outpatient_psychiatric_paid",
        "outpatient_radiology_paid", "outpatient_rehabilitation_paid",
        "outpatient_surgery_paid", "skilled_nursing_paid",
        "telehealth_visit_paid", "urgent_care_paid",
    ]

    allowed_cols = [
        "total_allowed", "medical_allowed", "inpatient_allowed", "outpatient_allowed",
        "office_based_allowed", "ancillary_allowed", "other_allowed", "pharmacy_allowed",
        "acute_inpatient_allowed", "ambulance_allowed", "ambulatory_surgery_center_allowed",
        "dialysis_allowed", "durable_medical_equipment_allowed", "emergency_department_allowed",
        "home_health_allowed", "inpatient_hospice_allowed", "inpatient_psychiatric_allowed",
        "inpatient_rehabilitation_allowed", "lab_allowed", "observation_allowed",
        "office_based_other_allowed", "office_based_pt_ot_st_allowed",
        "office_based_radiology_allowed", "office_based_surgery_allowed",
        "office_based_visit_allowed", "outpatient_hospital_or_clinic_allowed",
        "outpatient_pt_ot_st_allowed", "outpatient_psychiatric_allowed",
        "outpatient_radiology_allowed", "outpatient_rehabilitation_allowed",
        "outpatient_surgery_allowed", "skilled_nursing_allowed",
        "telehealth_visit_allowed", "urgent_care_allowed",
    ]

    agg_exprs = [F.count(F.lit(1)).alias("member_months")]
    for col_name in paid_cols + allowed_cols:
        agg_exprs.append(
            (F.sum(F.col(col_name)) / F.count(F.lit(1))).alias(col_name)
        )

    result = a.groupBy(
        "year_month",
        "payer",
        "data_source",
    ).agg(*agg_exprs)

    result = result.withColumn("tuva_last_run", F.current_timestamp())

    return result
