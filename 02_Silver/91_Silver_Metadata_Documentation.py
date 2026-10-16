# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Metadata & Clinical Documentation
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC This notebook applies table and column documentation to the
# MAGIC `cdr_mimiciv.silver_v2` schema.
# MAGIC
# MAGIC Source clinical descriptions are derived from the official MIMIC-IV
# MAGIC HOSP and ICU documentation.
# MAGIC
# MAGIC For columns inherited directly from Bronze, the original MIMIC
# MAGIC documentation is preserved.
# MAGIC
# MAGIC Silver-specific derived columns such as data-quality indicators and
# MAGIC processing timestamps are documented separately.
# MAGIC
# MAGIC This notebook contains no clinical transformations and does not
# MAGIC modify row-level data.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "cdr_mimiciv"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver_v2"

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
SILVER = f"{CATALOG}.{SILVER_SCHEMA}"

# COMMAND ----------

# helper function
# This safely applies comments
def sql_escape(text):
    if text is None:
        return None
    return text.replace("'", "''")


def set_table_comment(table_name, comment):
    spark.sql(f"""
        COMMENT ON TABLE {SILVER}.{table_name}
        IS '{sql_escape(comment)}'
    """)


def set_column_comment(table_name, column_name, comment):
    spark.sql(f"""
        ALTER TABLE {SILVER}.{table_name}
        ALTER COLUMN `{column_name}`
        COMMENT '{sql_escape(comment)}'
    """)