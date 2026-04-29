import os
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date


def run(spark: SparkSession) -> DataFrame:
    elig = spark.table("cms_hcc__stg_core__eligibility")
    dates = spark.table("cms_hcc__int_monthly_collection_dates")
    patient_tbl = spark.table("cms_hcc__stg_core__patient")

    # stg_eligibility: join eligibility with collection dates
    stg_eligibility = (
        elig.alias("elig")
        .join(
            dates.alias("dates"),
            (F.col("elig.enrollment_start_date") <= F.concat(F.col("dates.payment_year").cast("string"), F.lit("-12-31")).cast("date"))
            & (F.col("elig.enrollment_end_date") >= F.col("dates.collection_start_date")),
            "inner",
        )
        .select(
            F.col("elig.person_id"),
            F.col("elig.payer"),
            F.col("elig.enrollment_start_date"),
            F.col("elig.enrollment_end_date"),
            F.col("elig.original_reason_entitlement_code"),
            F.col("elig.dual_status_code"),
            F.col("elig.medicare_status_code"),
            F.col("elig.enrollment_status"),
            F.col("elig.medicaid_indicator"),
            F.col("elig.long_term_institutional_flag"),
            F.col("elig.institutional_snp_flag"),
            F.col("dates.collection_year"),
            F.col("dates.payment_year"),
            F.col("dates.collection_start_date"),
            F.col("dates.collection_end_date"),
            F.concat(F.col("dates.payment_year").cast("string"), F.lit("-12-31")).alias("payment_year_end_date"),
            F.row_number().over(
                Window.partitionBy(F.col("elig.person_id"), F.col("dates.collection_end_date"))
                .orderBy(F.col("elig.enrollment_end_date").desc())
            ).alias("row_num"),
        )
    )

    # payment_year_age_dates
    payment_year_age_dates = (
        dates.select("payment_year")
        .distinct()
        .withColumn("payment_year_age_date", F.concat(F.col("payment_year").cast("string"), F.lit("-02-01")).cast("date"))
    )

    # stg_patient
    stg_patient = (
        patient_tbl.alias("patient")
        .crossJoin(payment_year_age_dates.alias("dates"))
        .where(F.col("patient.birth_date").isNotNull())
        .select(
            F.col("patient.person_id"),
            F.col("patient.sex"),
            F.col("patient.birth_date"),
            F.col("dates.payment_year"),
            F.floor(F.datediff(F.col("dates.payment_year_age_date"), F.col("patient.birth_date")) / 365.25).alias("payment_year_age"),
            F.col("patient.death_date"),
        )
    )

    # cap_collection_start_end_dates
    cap_dates = (
        stg_eligibility.select(
            "person_id", "payer", "enrollment_start_date", "enrollment_end_date",
            "payment_year", "collection_start_date", "collection_end_date", "payment_year_end_date",
        )
        .withColumn(
            "proxy_enrollment_start_date",
            F.when(F.col("enrollment_start_date") < safe_cast_date(F.col("collection_start_date")),
                   safe_cast_date(F.col("collection_start_date")))
            .otherwise(F.col("enrollment_start_date")),
        )
        .withColumn(
            "proxy_enrollment_end_date",
            F.when(F.col("enrollment_end_date") > safe_cast_date(F.col("payment_year_end_date")),
                   safe_cast_date(F.col("payment_year_end_date")))
            .otherwise(F.col("enrollment_end_date")),
        )
    )

    # calculate_prior_coverage
    calculate_prior_coverage = (
        cap_dates
        .groupBy("person_id", "payer", "payment_year", "collection_end_date")
        .agg(
            F.sum(F.months_between(F.col("proxy_enrollment_end_date"), F.col("proxy_enrollment_start_date")).cast("int") + 1).alias("coverage_months"),
            F.min(F.months_between(F.col("collection_end_date"), F.col("collection_start_date")).cast("int") + 1).alias("collection_months"),
        )
    )

    # add_enrollment
    add_enrollment = (
        calculate_prior_coverage
        .withColumn(
            "enrollment_status",
            F.when(F.col("coverage_months") < F.col("collection_months"), F.lit("New"))
            .otherwise(F.lit("Continuing")),
        )
        .select("person_id", "payer", "payment_year", "collection_end_date", "enrollment_status")
    )

    # latest_eligibility
    se = stg_eligibility.alias("se")
    ae = add_enrollment.alias("ae")
    sp = stg_patient.alias("sp")

    latest_eligibility = (
        se
        .join(
            ae,
            (F.col("se.person_id") == F.col("ae.person_id"))
            & (F.col("se.payer") == F.col("ae.payer"))
            & (F.col("se.payment_year") == F.col("ae.payment_year"))
            & (F.col("se.collection_end_date") == F.col("ae.collection_end_date")),
            "left",
        )
        .join(
            sp,
            (F.col("se.person_id") == F.col("sp.person_id"))
            & (F.col("se.payment_year") == F.col("sp.payment_year")),
            "left",
        )
        .where(
            (F.col("se.row_num") == 1)
            & (F.col("sp.payment_year_age").isNotNull())
        )
        .select(
            F.col("se.person_id"),
            F.col("se.payer"),
            F.col("se.payment_year"),
            F.col("se.collection_start_date"),
            F.col("se.collection_end_date"),
            F.col("sp.sex").alias("gender"),
            F.col("sp.payment_year_age"),
            F.col("se.original_reason_entitlement_code"),
            F.col("se.dual_status_code"),
            F.col("se.medicare_status_code"),
            F.col("se.medicaid_indicator"),
            F.col("se.long_term_institutional_flag"),
            F.col("se.institutional_snp_flag"),
            F.when(F.col("se.enrollment_status").isNotNull(), F.col("se.enrollment_status"))
            .when(F.col("ae.enrollment_status").isNull(), F.lit("New"))
            .otherwise(F.col("ae.enrollment_status"))
            .alias("enrollment_status"),
            F.when(
                F.col("ae.enrollment_status").isNull() & F.col("se.enrollment_status").isNull(),
                F.lit(True),
            ).otherwise(F.lit(False)).alias("enrollment_status_default"),
        )
    )

    # add_age_group
    es = F.col("enrollment_status")
    age = F.col("payment_year_age")
    orec_code = F.col("original_reason_entitlement_code")

    age_group_expr = (
        # New Enrollee age groups
        F.when((es == "New") & age.between(0, 34), F.lit("0-34"))
        .when((es == "New") & age.between(35, 44), F.lit("35-44"))
        .when((es == "New") & age.between(45, 54), F.lit("45-54"))
        .when((es == "New") & age.between(55, 59), F.lit("55-59"))
        .when((es == "New") & age.between(60, 63), F.lit("60-64"))
        .when((es == "New") & (orec_code != "0") & (age == 64), F.lit("60-64"))
        .when((es == "New") & (age == 64), F.lit("65"))
        .when((es == "New") & (age == 65), F.lit("65"))
        .when((es == "New") & (age == 66), F.lit("66"))
        .when((es == "New") & (age == 67), F.lit("67"))
        .when((es == "New") & (age == 68), F.lit("68"))
        .when((es == "New") & (age == 69), F.lit("69"))
        .when((es == "New") & age.between(70, 74), F.lit("70-74"))
        .when((es == "New") & age.between(75, 79), F.lit("75-79"))
        .when((es == "New") & age.between(80, 84), F.lit("80-84"))
        .when((es == "New") & age.between(85, 89), F.lit("85-89"))
        .when((es == "New") & age.between(90, 94), F.lit("90-94"))
        .when((es == "New") & (age >= 95), F.lit(">=95"))
        # Continuing/other age groups
        .when(age.between(0, 34), F.lit("0-34"))
        .when(age.between(35, 44), F.lit("35-44"))
        .when(age.between(45, 54), F.lit("45-54"))
        .when(age.between(55, 59), F.lit("55-59"))
        .when(age.between(60, 64), F.lit("60-64"))
        .when(age.between(65, 69), F.lit("65-69"))
        .when(age.between(70, 74), F.lit("70-74"))
        .when(age.between(75, 79), F.lit("75-79"))
        .when(age.between(80, 84), F.lit("80-84"))
        .when(age.between(85, 89), F.lit("85-89"))
        .when(age.between(90, 94), F.lit("90-94"))
        .when(age >= 95, F.lit(">=95"))
    )

    add_age_group = latest_eligibility.withColumn("age_group", age_group_expr)

    # add_status_logic
    gender_col = F.col("gender")
    med_ind = F.col("medicaid_indicator")
    dual = F.col("dual_status_code")
    orec = F.col("original_reason_entitlement_code")
    msc = F.col("medicare_status_code")
    lti = F.col("long_term_institutional_flag")

    add_status_logic = (
        add_age_group
        .withColumn(
            "gender",
            F.when(gender_col == "female", F.lit("Female"))
            .when(gender_col == "male", F.lit("Male"))
        )
        .withColumn(
            "medicaid_status",
            F.when(med_ind == 1, F.lit("Yes"))
            .when(dual.isin("01", "02", "03", "04", "05", "06", "08"), F.lit("Yes"))
            .otherwise(F.lit("No")),
        )
        .withColumn(
            "dual_status",
            F.when(dual.isin("02", "04", "08"), F.lit("Full"))
            .when(dual.isin("01", "03", "05", "06"), F.lit("Partial"))
            .otherwise(F.lit("Non")),
        )
        .withColumn(
            "orec",
            F.when(orec == "0", F.lit("Aged"))
            .when(orec.isin("1", "2", "3") & (age >= 65), F.lit("Aged"))
            .when(orec.isin("1", "2", "3"), F.lit("Disabled"))
            .when(orec.isNull() & msc.isin("10", "11", "31"), F.lit("Aged"))
            .when(orec.isNull() & msc.isin("20", "21") & (age >= 65), F.lit("Aged"))
            .when(orec.isNull() & msc.isin("20", "21"), F.lit("Disabled"))
            .when(F.coalesce(orec, msc).isNull(), F.lit("Aged")),
        )
        .withColumn(
            "originally_disabled_flag",
            F.when((orec == "1") & (age >= 65), F.lit("Yes"))
            .when(orec.isNull() & msc.isin("20", "21") & (age >= 65), F.lit("Yes"))
            .otherwise(F.lit("No")),
        )
        .withColumn(
            "institutional_status",
            F.when(lti == 1, F.lit("Yes"))
            .when(F.col("enrollment_status") == "Institutional", F.lit("Yes"))
            .otherwise(F.lit("No")),
        )
        .withColumn(
            "medicaid_dual_status_default",
            F.when(med_ind.isNull() & dual.isNull(), F.lit(True))
            .otherwise(F.lit(False)),
        )
        .withColumn(
            "orec_default",
            F.when(orec.isin("2"), F.lit(True))
            .when(orec.isNull() & msc.isin("31"), F.lit(True))
            .when(F.coalesce(orec, msc).isNull(), F.lit(True))
            .otherwise(F.lit(False)),
        )
        .withColumn(
            "institutional_status_default",
            F.when(lti.isNull() & (F.col("enrollment_status") != "Institutional"), F.lit(True))
            .otherwise(F.lit(False)),
        )
    )

    # add_data_types
    add_data_types = add_status_logic.select(
        F.col("person_id").cast("string"),
        F.col("payer").cast("string"),
        F.col("enrollment_status").cast("string"),
        F.col("gender").cast("string"),
        F.col("age_group").cast("string"),
        F.col("medicaid_status").cast("string"),
        F.col("dual_status").cast("string"),
        F.col("orec").cast("string"),
        F.col("institutional_status").cast("string"),
        F.col("originally_disabled_flag").cast("string"),
        F.col("institutional_snp_flag").cast("int"),
        F.col("enrollment_status_default").cast("boolean"),
        F.col("medicaid_dual_status_default").cast("boolean"),
        F.col("orec_default").cast("boolean"),
        F.col("institutional_status_default").cast("boolean"),
        F.col("payment_year").cast("int"),
        F.col("collection_start_date").cast("date"),
        F.col("collection_end_date").cast("date"),
    )

    result = add_data_types.withColumn("tuva_last_run", F.current_timestamp())

    return result
