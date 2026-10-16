# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Engineering Standards
# MAGIC
# MAGIC ## Purpose
# MAGIC Create reusable, clinically interpretable and validated MIMIC-IV entities.
# MAGIC
# MAGIC ## Processing principles
# MAGIC 1. Preserve source clinical meaning.
# MAGIC 2. Preserve missing values unless a deterministic correction exists.
# MAGIC 3. Do not silently remove unusual clinical records.
# MAGIC 4. Standardise reusable datatypes, whitespace and units.
# MAGIC 5. Validate subject_id, hadm_id, stay_id and domain-specific keys.
# MAGIC 6. Enrich coded values using authoritative MIMIC dictionaries.
# MAGIC 7. Flag questionable values rather than guessing corrections.
# MAGIC 8. Preserve event timestamps.
# MAGIC 9. Keep project-specific cohorts and analytical features out of Silver.
# MAGIC 10. Record and validate every transformation.

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS cdr_mimiciv.silver_v2
""")