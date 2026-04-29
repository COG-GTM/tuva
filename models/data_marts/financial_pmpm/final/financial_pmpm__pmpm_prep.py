import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    a = spark.table("core__member_months")
    b = spark.table("financial_pmpm__service_category_1_paid_pivot")
    c = spark.table("financial_pmpm__service_category_2_paid_pivot")
    d = spark.table("financial_pmpm__service_category_1_allowed_pivot")
    e = spark.table("financial_pmpm__service_category_2_allowed_pivot")

    join_keys = ["person_id", "member_id", "year_month", "payer", "plan", "data_source"]

    join_cond_b = [a[k] == b[k] for k in join_keys]
    join_cond_c = [a[k] == c[k] for k in join_keys]
    join_cond_d = [a[k] == d[k] for k in join_keys]
    join_cond_e = [a[k] == e[k] for k in join_keys]

    combined = (
        a
        .join(b, join_cond_b, "left")
        .join(c, join_cond_c, "left")
        .join(d, join_cond_d, "left")
        .join(e, join_cond_e, "left")
    )

    combine = combined.select(
        a["person_id"],
        a["member_id"],
        a["year_month"],
        a["payer"],
        a["plan"],
        a["data_source"],
        a["payer_attributed_provider"],
        a["payer_attributed_provider_practice"],
        a["payer_attributed_provider_organization"],
        a["payer_attributed_provider_lob"],
        a["custom_attributed_provider"],
        a["custom_attributed_provider_practice"],
        a["custom_attributed_provider_organization"],
        a["custom_attributed_provider_lob"],

        # service cat 1 paid
        F.coalesce(b["inpatient_paid"], F.lit(0)).alias("inpatient_paid"),
        F.coalesce(b["outpatient_paid"], F.lit(0)).alias("outpatient_paid"),
        F.coalesce(b["office_based_paid"], F.lit(0)).alias("office_based_paid"),
        F.coalesce(b["ancillary_paid"], F.lit(0)).alias("ancillary_paid"),
        F.coalesce(b["other_paid"], F.lit(0)).alias("other_paid"),
        F.coalesce(b["pharmacy_paid"], F.lit(0)).alias("pharmacy_paid"),

        # service cat 2 paid
        F.coalesce(c["acute_inpatient_paid"], F.lit(0)).alias("acute_inpatient_paid"),
        F.coalesce(c["ambulance_paid"], F.lit(0)).alias("ambulance_paid"),
        F.coalesce(c["ambulatory_surgery_center_paid"], F.lit(0)).alias("ambulatory_surgery_center_paid"),
        F.coalesce(c["dialysis_paid"], F.lit(0)).alias("dialysis_paid"),
        F.coalesce(c["durable_medical_equipment_paid"], F.lit(0)).alias("durable_medical_equipment_paid"),
        F.coalesce(c["emergency_department_paid"], F.lit(0)).alias("emergency_department_paid"),
        F.coalesce(c["home_health_paid"], F.lit(0)).alias("home_health_paid"),
        F.coalesce(c["inpatient_hospice_paid"], F.lit(0)).alias("inpatient_hospice_paid"),
        F.coalesce(c["inpatient_psychiatric_paid"], F.lit(0)).alias("inpatient_psychiatric_paid"),
        F.coalesce(c["inpatient_rehabilitation_paid"], F.lit(0)).alias("inpatient_rehabilitation_paid"),
        F.coalesce(c["lab_paid"], F.lit(0)).alias("lab_paid"),
        F.coalesce(c["observation_paid"], F.lit(0)).alias("observation_paid"),
        F.coalesce(c["office_based_other_paid"], F.lit(0)).alias("office_based_other_paid"),
        F.coalesce(c["office_based_ptotst_paid"], F.lit(0)).alias("office_based_pt_ot_st_paid"),
        F.coalesce(c["office_based_radiology_paid"], F.lit(0)).alias("office_based_radiology_paid"),
        F.coalesce(c["office_based_surgery_paid"], F.lit(0)).alias("office_based_surgery_paid"),
        F.coalesce(c["office_based_visit_paid"], F.lit(0)).alias("office_based_visit_paid"),
        F.coalesce(c["other_paid"], F.lit(0)).alias("other_paid_2"),
        F.coalesce(c["outpatient_hospice_paid"], F.lit(0)).alias("outpatient_hospice_paid"),
        F.coalesce(c["outpatient_hospital_or_clinic_paid"], F.lit(0)).alias("outpatient_hospital_or_clinic_paid"),
        F.coalesce(c["outpatient_ptotst_paid"], F.lit(0)).alias("outpatient_pt_ot_st_paid"),
        F.coalesce(c["outpatient_psychiatric_paid"], F.lit(0)).alias("outpatient_psychiatric_paid"),
        F.coalesce(c["outpatient_radiology_paid"], F.lit(0)).alias("outpatient_radiology_paid"),
        F.coalesce(c["outpatient_rehabilitation_paid"], F.lit(0)).alias("outpatient_rehabilitation_paid"),
        F.coalesce(c["outpatient_surgery_paid"], F.lit(0)).alias("outpatient_surgery_paid"),
        F.coalesce(c["pharmacy_paid"], F.lit(0)).alias("pharmacy_paid_2"),
        F.coalesce(c["skilled_nursing_paid"], F.lit(0)).alias("skilled_nursing_paid"),
        F.coalesce(c["telehealth_visit_paid"], F.lit(0)).alias("telehealth_visit_paid"),
        F.coalesce(c["urgent_care_paid"], F.lit(0)).alias("urgent_care_paid"),

        # service cat 1 allowed
        F.coalesce(d["inpatient_allowed"], F.lit(0)).alias("inpatient_allowed"),
        F.coalesce(d["outpatient_allowed"], F.lit(0)).alias("outpatient_allowed"),
        F.coalesce(d["office_based_allowed"], F.lit(0)).alias("office_based_allowed"),
        F.coalesce(d["ancillary_allowed"], F.lit(0)).alias("ancillary_allowed"),
        F.coalesce(d["other_allowed"], F.lit(0)).alias("other_allowed"),
        F.coalesce(d["pharmacy_allowed"], F.lit(0)).alias("pharmacy_allowed"),

        # service cat 2 allowed
        F.coalesce(e["acute_inpatient_allowed"], F.lit(0)).alias("acute_inpatient_allowed"),
        F.coalesce(e["ambulance_allowed"], F.lit(0)).alias("ambulance_allowed"),
        F.coalesce(e["ambulatory_surgery_center_allowed"], F.lit(0)).alias("ambulatory_surgery_center_allowed"),
        F.coalesce(e["dialysis_allowed"], F.lit(0)).alias("dialysis_allowed"),
        F.coalesce(e["durable_medical_equipment_allowed"], F.lit(0)).alias("durable_medical_equipment_allowed"),
        F.coalesce(e["emergency_department_allowed"], F.lit(0)).alias("emergency_department_allowed"),
        F.coalesce(e["home_health_allowed"], F.lit(0)).alias("home_health_allowed"),
        F.coalesce(e["inpatient_hospice_allowed"], F.lit(0)).alias("inpatient_hospice_allowed"),
        F.coalesce(e["inpatient_psychiatric_allowed"], F.lit(0)).alias("inpatient_psychiatric_allowed"),
        F.coalesce(e["inpatient_rehabilitation_allowed"], F.lit(0)).alias("inpatient_rehabilitation_allowed"),
        F.coalesce(e["lab_allowed"], F.lit(0)).alias("lab_allowed"),
        F.coalesce(e["observation_allowed"], F.lit(0)).alias("observation_allowed"),
        F.coalesce(e["office_based_other_allowed"], F.lit(0)).alias("office_based_other_allowed"),
        F.coalesce(e["office_based_ptotst_allowed"], F.lit(0)).alias("office_based_pt_ot_st_allowed"),
        F.coalesce(e["office_based_radiology_allowed"], F.lit(0)).alias("office_based_radiology_allowed"),
        F.coalesce(e["office_based_surgery_allowed"], F.lit(0)).alias("office_based_surgery_allowed"),
        F.coalesce(e["office_based_visit_allowed"], F.lit(0)).alias("office_based_visit_allowed"),
        F.coalesce(e["other_allowed"], F.lit(0)).alias("other_allowed_2"),
        F.coalesce(e["outpatient_hospice_allowed"], F.lit(0)).alias("outpatient_hospice_allowed"),
        F.coalesce(e["outpatient_hospital_or_clinic_allowed"], F.lit(0)).alias("outpatient_hospital_or_clinic_allowed"),
        F.coalesce(e["outpatient_ptotst_allowed"], F.lit(0)).alias("outpatient_pt_ot_st_allowed"),
        F.coalesce(e["outpatient_psychiatric_allowed"], F.lit(0)).alias("outpatient_psychiatric_allowed"),
        F.coalesce(e["outpatient_radiology_allowed"], F.lit(0)).alias("outpatient_radiology_allowed"),
        F.coalesce(e["outpatient_rehabilitation_allowed"], F.lit(0)).alias("outpatient_rehabilitation_allowed"),
        F.coalesce(e["outpatient_surgery_allowed"], F.lit(0)).alias("outpatient_surgery_allowed"),
        F.coalesce(e["pharmacy_allowed"], F.lit(0)).alias("pharmacy_allowed_2"),
        F.coalesce(e["skilled_nursing_allowed"], F.lit(0)).alias("skilled_nursing_allowed"),
        F.coalesce(e["telehealth_visit_allowed"], F.lit(0)).alias("telehealth_visit_allowed"),
        F.coalesce(e["urgent_care_allowed"], F.lit(0)).alias("urgent_care_allowed"),
    )

    result = combine.withColumn(
        "total_paid",
        F.col("inpatient_paid") + F.col("outpatient_paid") + F.col("office_based_paid")
        + F.col("ancillary_paid") + F.col("other_paid") + F.col("pharmacy_paid"),
    ).withColumn(
        "medical_paid",
        F.col("inpatient_paid") + F.col("outpatient_paid") + F.col("office_based_paid")
        + F.col("ancillary_paid") + F.col("other_paid"),
    ).withColumn(
        "total_allowed",
        F.col("inpatient_allowed") + F.col("outpatient_allowed") + F.col("office_based_allowed")
        + F.col("ancillary_allowed") + F.col("other_allowed") + F.col("pharmacy_allowed"),
    ).withColumn(
        "medical_allowed",
        F.col("inpatient_allowed") + F.col("outpatient_allowed") + F.col("office_based_allowed")
        + F.col("ancillary_allowed") + F.col("other_allowed"),
    ).withColumn(
        "tuva_last_run", F.current_timestamp(),
    )

    return result
