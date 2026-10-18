# Databricks notebook source
# DBTITLE 1,dim_lab_item
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.dim_lab_item
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     xxhash64(itemid) AS lab_item_key,
# MAGIC     itemid,
# MAGIC     label AS lab_test_name,
# MAGIC     fluid,
# MAGIC     category
# MAGIC FROM cdr_mimiciv.silver.d_labitems
# MAGIC WHERE itemid IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,dim_icu_item
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.dim_icu_item
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     xxhash64(itemid) AS icu_item_key,
# MAGIC     itemid,
# MAGIC     label AS item_name,
# MAGIC     abbreviation,
# MAGIC     linksto,
# MAGIC     category,
# MAGIC     unitname,
# MAGIC     param_type
# MAGIC FROM cdr_mimiciv.silver.d_items
# MAGIC WHERE itemid IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,dim_careunit
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW cdr_mimiciv.silver.v_careunit AS
# MAGIC SELECT DISTINCT first_careunit AS careunit_name
# MAGIC FROM cdr_mimiciv.silver.icustays
# MAGIC WHERE first_careunit IS NOT NULL
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT last_careunit AS careunit_name
# MAGIC FROM cdr_mimiciv.silver.icustays
# MAGIC WHERE last_careunit IS NOT NULL
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT careunit AS careunit_name
# MAGIC FROM cdr_mimiciv.silver.transfers
# MAGIC WHERE careunit IS NOT NULL;
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.dim_careunit
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     xxhash64(careunit_name) AS careunit_key,
# MAGIC     careunit_name
# MAGIC FROM cdr_mimiciv.silver.v_careunit;

# COMMAND ----------

# DBTITLE 1,dim_procedure
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.dim_procedure
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     xxhash64(icd_code, icd_version) AS procedure_key,
# MAGIC     icd_code,
# MAGIC     icd_version,
# MAGIC     long_title AS procedure_title
# MAGIC FROM cdr_mimiciv.silver.d_icd_procedures
# MAGIC WHERE icd_code IS NOT NULL
# MAGIC   AND icd_version IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,dim_provider
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW cdr_mimiciv.silver.v_provider AS
# MAGIC SELECT
# MAGIC     provider_id
# MAGIC FROM cdr_mimiciv.silver.provider
# MAGIC WHERE provider_id IS NOT NULL;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.dim_provider
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     xxhash64(provider_id) AS provider_key,
# MAGIC     provider_id
# MAGIC FROM cdr_mimiciv.silver.v_provider;

# COMMAND ----------

# DBTITLE 1,dim_medication
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.dim_medication
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     xxhash64(
# MAGIC         drug_type,
# MAGIC         drug,
# MAGIC         formulary_drug_cd,
# MAGIC         gsn,
# MAGIC         ndc
# MAGIC     ) AS medication_key,
# MAGIC
# MAGIC     drug_type,
# MAGIC     drug,
# MAGIC     formulary_drug_cd,
# MAGIC     gsn,
# MAGIC     ndc
# MAGIC
# MAGIC FROM (
# MAGIC     SELECT DISTINCT
# MAGIC         drug_type,
# MAGIC         drug,
# MAGIC         formulary_drug_cd,
# MAGIC         gsn,
# MAGIC         ndc
# MAGIC     FROM cdr_mimiciv.silver.prescriptions
# MAGIC     WHERE drug IS NOT NULL
# MAGIC );

# COMMAND ----------

# DBTITLE 1,dim_patient
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW cdr_mimiciv.silver.v_patient AS
# MAGIC SELECT
# MAGIC     subject_id,
# MAGIC     gender,
# MAGIC     anchor_age,
# MAGIC     anchor_year,
# MAGIC     anchor_year_group
# MAGIC FROM cdr_mimiciv.silver.patients;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.dim_patient
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     xxhash64(subject_id) AS patient_key,
# MAGIC     subject_id,
# MAGIC     gender,
# MAGIC     anchor_age,
# MAGIC     anchor_year,
# MAGIC     anchor_year_group,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN anchor_age = 91 THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS age_deidentified_flag
# MAGIC
# MAGIC FROM cdr_mimiciv.silver.v_patient
# MAGIC WHERE subject_id IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,dim_date
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.dim_date
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC WITH date_range AS (
# MAGIC     SELECT sequence(
# MAGIC         DATE '2100-01-01',
# MAGIC         DATE '2210-12-31',
# MAGIC         INTERVAL 1 DAY
# MAGIC     ) AS dates
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
# MAGIC     d AS full_date,
# MAGIC     year(d) AS year,
# MAGIC     quarter(d) AS quarter,
# MAGIC     month(d) AS month,
# MAGIC     date_format(d, 'MMMM') AS month_name,
# MAGIC     day(d) AS day_of_month,
# MAGIC     weekofyear(d) AS week_of_year,
# MAGIC     dayofweek(d) AS day_of_week,
# MAGIC     date_format(d, 'EEEE') AS day_name
# MAGIC FROM date_range
# MAGIC LATERAL VIEW explode(dates) t AS d;

# COMMAND ----------

# DBTITLE 1,dim_location
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW cdr_mimiciv.silver.v_location AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     admission_location AS location_name,
# MAGIC     'ADMISSION' AS location_type
# MAGIC FROM cdr_mimiciv.silver.admissions
# MAGIC WHERE admission_location IS NOT NULL
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     discharge_location AS location_name,
# MAGIC     'DISCHARGE' AS location_type
# MAGIC FROM cdr_mimiciv.silver.admissions
# MAGIC WHERE discharge_location IS NOT NULL;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.dim_location
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     xxhash64(location_type, location_name) AS location_key,
# MAGIC     location_type,
# MAGIC     location_name
# MAGIC FROM cdr_mimiciv.silver.v_location;

# COMMAND ----------

# DBTITLE 1,dim_admission_type
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW cdr_mimiciv.silver.v_admission_type AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     admission_type
# MAGIC FROM cdr_mimiciv.silver.admissions
# MAGIC WHERE admission_type IS NOT NULL;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.dim_admission_type
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     xxhash64(admission_type) AS admission_type_key,
# MAGIC     admission_type
# MAGIC FROM cdr_mimiciv.silver.v_admission_type;

# COMMAND ----------

# DBTITLE 1,dim_diagnosis
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.dim_diagnosis
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     xxhash64(icd_code, icd_version) AS diagnosis_key,
# MAGIC     icd_code,
# MAGIC     icd_version,
# MAGIC     long_title AS diagnosis_title
# MAGIC FROM cdr_mimiciv.silver.d_icd_diagnoses
# MAGIC WHERE icd_code IS NOT NULL
# MAGIC   AND icd_version IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,dim_microbiology_test
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW cdr_mimiciv.silver.v_microbiology_test AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     spec_itemid,
# MAGIC     spec_type_desc,
# MAGIC     test_itemid,
# MAGIC     test_name
# MAGIC FROM cdr_mimiciv.silver.microbiologyevents
# MAGIC WHERE test_itemid IS NOT NULL;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.dim_microbiology_test
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     xxhash64(spec_itemid, test_itemid) AS microbiology_test_key,
# MAGIC     spec_itemid,
# MAGIC     spec_type_desc,
# MAGIC     test_itemid,
# MAGIC     test_name
# MAGIC FROM cdr_mimiciv.silver.v_microbiology_test;

# COMMAND ----------

# DBTITLE 1,dim_service
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW cdr_mimiciv.silver.v_service AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     curr_service AS service
# MAGIC FROM cdr_mimiciv.silver.services
# MAGIC WHERE curr_service IS NOT NULL
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     prev_service AS service
# MAGIC FROM cdr_mimiciv.silver.services
# MAGIC WHERE prev_service IS NOT NULL;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.dim_service
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     xxhash64(service) AS service_key,
# MAGIC     service
# MAGIC FROM cdr_mimiciv.silver.v_service;