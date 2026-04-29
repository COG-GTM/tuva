import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:
    medical_claim = spark.table("fhir_preprocessing__stg_core__medical_claim")
    claim_procedure = spark.table("fhir_preprocessing__stg_core__procedure")

    add_sequence = (
        claim_procedure.select(
            F.col("claim_id"),
            F.col("procedure_id"),
            F.row_number().over(
                Window.partitionBy("claim_id").orderBy("procedure_id")
            ).alias("eob_procedure_sequence"),
        )
    )

    mc_header = medical_claim.filter(F.col("claim_line_number") == 1)

    staging = (
        mc_header.alias("mc")
        .join(claim_procedure.alias("cp"), F.col("mc.claim_id") == F.col("cp.claim_id"), how="inner")
        .join(
            add_sequence.alias("seq"),
            (F.col("cp.claim_id") == F.col("seq.claim_id"))
            & (F.col("cp.procedure_id") == F.col("seq.procedure_id")),
            how="inner",
        )
        .filter(F.lower(F.col("cp.normalized_code_type")).isin("icd-9-pcs", "icd-10-pcs"))
        .select(
            F.col("mc.claim_id").alias("claim_id"),
            F.col("seq.eob_procedure_sequence"),
            F.when(
                F.lower(F.col("cp.normalized_code_type")) == "icd-10-pcs",
                F.lit("ICD10PCS"),
            ).otherwise(F.lit("ICD9")).alias("eob_procedure_system"),
            F.col("cp.normalized_code").alias("eob_procedure_code"),
            F.regexp_replace(F.col("cp.normalized_description"), ",", "").alias("eob_procedure_display"),
            F.when(F.col("seq.eob_procedure_sequence") == 1, F.lit("principal"))
             .otherwise(F.lit("other"))
             .alias("eob_procedure_type_code"),
        )
    )

    return create_json_object(
        df=staging,
        group_cols="claim_id",
        obj_col="eob_procedure_list",
        obj_fields=[
            "eob_procedure_sequence",
            "eob_procedure_system",
            "eob_procedure_code",
            "eob_procedure_display",
            "eob_procedure_type_code",
        ],
    )
