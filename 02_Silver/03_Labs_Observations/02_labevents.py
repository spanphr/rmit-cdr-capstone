# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC SELECT COUNT(*) AS total_records
# MAGIC FROM cdr_mimiciv.bronze.labevents;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM cdr_mimiciv.Bronze.labevents
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM cdr_mimiciv.bronze.d_labitems
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select value, count(*) as c
# MAGIC FROM cdr_mimiciv.Bronze.labevents
# MAGIC WHERE valuenum IS NULL
# MAGIC group by value
# MAGIC ORDER BY c DESC;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT
# MAGIC     l.labevent_id,
# MAGIC     l.itemid,
# MAGIC     d.label AS lab_test,
# MAGIC     d.fluid,
# MAGIC     l.value,
# MAGIC     l.valuenum,
# MAGIC     l.valueuom,
# MAGIC     l.ref_range_lower,
# MAGIC     l.ref_range_upper,
# MAGIC     l.flag
# MAGIC FROM cdr_mimiciv.bronze.labevents l
# MAGIC LEFT JOIN cdr_mimiciv.bronze.d_labitems d
# MAGIC     ON l.itemid = d.itemid
# MAGIC LIMIT 30;
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT
# MAGIC     l.labevent_id,
# MAGIC     l.itemid,
# MAGIC     d.label AS lab_test,
# MAGIC     l.value,
# MAGIC     l.valuenum,
# MAGIC     l.valueuom,
# MAGIC     l.ref_range_lower,
# MAGIC     l.ref_range_upper,
# MAGIC     l.flag
# MAGIC FROM cdr_mimiciv.bronze.labevents l
# MAGIC LEFT JOIN cdr_mimiciv.bronze.d_labitems d
# MAGIC     ON l.itemid = d.itemid
# MAGIC WHERE l.valuenum = 6.1
# MAGIC   AND l.ref_range_upper = 4
# MAGIC   AND LOWER(TRIM(l.valueuom)) = 'ng/ml'
# MAGIC LIMIT 50;
# MAGIC