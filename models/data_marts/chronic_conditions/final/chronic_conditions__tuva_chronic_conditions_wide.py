import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'pyspark', 'utils'))
from pyspark_helpers import safe_cast_date, safe_cast_timestamp, safe_cast_int, year_month, apply_regex, substring_col, create_json_object, union_relations


def run(spark: SparkSession) -> DataFrame:

    conditions_long = spark.table("chronic_conditions__tuva_chronic_conditions_long")

    patients = conditions_long.select("person_id").distinct()

    condition_map = {
        "obesity": "Obesity",
        "osteoarthritis": "Osteoarthritis",
        "copd": "Chronic Obstructive Pulmonary Disease",
        "anxiety_disorders": "Anxiety Disorders",
        "ckd": "Chronic Kidney Disease",
        "t2d": "Type 2 Diabetes Mellitus",
        "cll": "Chronic Lymphocytic Leukemia",
        "dysplipidemias": "Dyslipidemias",
        "hypertension": "Hypertension",
        "atherosclerosis": "Atherosclerosis",
        "dementia": "Dementia",
        "rheumatoid_arthritis": "Rheumatoid Arthritis",
        "celiac": "Celiac Disease",
        "hip_fracture": "Hip Fracture",
        "immunodeficiencies_and_white_blood_cell_disorders": "Specified Immunodeficiencies and White Blood Cell Disorders  (HCC v28 concept #115)",
        "asthma": "Asthma",
        "t1d": "Type 1 Diabetes Mellitus",
        "ulcerative_colitis": "Ulcerative colitis",
        "chrohns": "Crohns Disease",
        "holicobacter": "Helicobacter pylori Infection",
        "bipolar": "Bipolar Affective Disorder",
        "heart_failure": "Heart Failure",
        "tabacco": "Tobacco Use",
        "lyme": "Lyme Disease",
        "breast_cancer": "Breast Cancer",
        "osteoporosis": "Osteoporosis",
        "pulmonary_embolism": "Pulmonary Embolism, Thrombotic or Unspecified",
        "schizophrenia": "Schizophrenia",
        "atrial_fibrillation": "Atrial Fibrillation",
        "colorectal_cancer": "Colorectal Cancer",
        "depression": "Major Depressive Disorder",
        "deep_vein_thrombosis": "Deep Vein Thrombosis of Extremities or Central Veins",
        "alzheimer": "Alzheimer Disease",
        "stroke": "Stroke",
        "myocardial_infraction": "Myocardial Infarction",
        "opiod_use_disorder": "Opioid Use Disorder",
        "lung_cancer": "Lung cancer, primary or unspecified",
        "herpes": "Herpes Simplex Infection",
        "rickettsiosis": "Rickettsiosis",
        "ms": "Multiple Sclerosis",
        "alchohol": "Alcohol Use Disorder",
        "adhd": "Attention Deficit-Hyperactivity Disorder",
        "hiv": "HIV/AIDS  (HCC v28 concept #1)",
        "ptsd": "Post-Traumatic Stress Disorder",
        "lupus": "Systemic Lupus Erythematosus",
    }

    # Build a set of person_ids per condition for efficient lookup
    condition_person_ids = {}
    for col_name, condition_value in condition_map.items():
        condition_person_ids[col_name] = (
            conditions_long
            .filter(F.col("condition") == condition_value)
            .select("person_id")
            .distinct()
        )

    result = patients

    for col_name, condition_df in condition_person_ids.items():
        flagged = condition_df.withColumn(col_name, F.lit(1))
        result = (
            result.join(flagged, on="person_id", how="left")
        )
        result = result.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)))

    result = result.withColumn("tuva_last_run", F.current_timestamp())

    return result
