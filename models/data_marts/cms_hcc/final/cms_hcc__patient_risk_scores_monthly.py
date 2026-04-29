import os
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))


def run(spark: SparkSession) -> DataFrame:
    by_factor = spark.table("cms_hcc__patient_risk_scores_monthly_by_factor_type")

    result = (
        by_factor
        .groupBy(
            "person_id", "payer", "risk_model_code", "enrollment_status",
            "enrollment_status_default", "orec_default",
            "payment_year", "collection_start_date", "collection_end_date",
        )
        .agg(
            F.sum("v24_risk_score").alias("v24_risk_score"),
            F.sum("v28_risk_score").alias("v28_risk_score"),
            F.sum("blended_risk_score").alias("blended_risk_score"),
            F.sum("normalized_risk_score").alias("normalized_risk_score"),
            F.sum("payment_risk_score").alias("payment_risk_score"),
            F.sum("payment_risk_score_weighted_by_months").alias("payment_risk_score_weighted_by_months"),
            F.max("member_months").alias("member_months"),
            F.max("tuva_last_run").alias("tuva_last_run"),
        )
    )

    return result
