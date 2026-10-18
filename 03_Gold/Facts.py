# Databricks notebook source
# DBTITLE 1,fact_icu_stay
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.fact_icu_stay
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     i.stay_id AS icu_stay_fact_key,
# MAGIC     
# MAGIC     i.stay_id,
# MAGIC     i.hadm_id,
# MAGIC     
# MAGIC     p.patient_key,
# MAGIC     fc.careunit_key AS first_careunit_key,
# MAGIC     lc.careunit_key AS last_careunit_key,
# MAGIC
# MAGIC     CAST(date_format(i.intime, 'yyyyMMdd') AS INT) AS in_date_key,
# MAGIC     CAST(date_format(i.outtime, 'yyyyMMdd') AS INT) AS out_date_key,
# MAGIC
# MAGIC     i.intime,
# MAGIC     i.outtime,
# MAGIC     i.los AS los_days
# MAGIC
# MAGIC FROM cdr_mimiciv.silver.icustays i
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_patient p
# MAGIC     ON i.subject_id = p.subject_id
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_careunit fc
# MAGIC     ON i.first_careunit = fc.careunit_name
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_careunit lc
# MAGIC     ON i.last_careunit = lc.careunit_name
# MAGIC
# MAGIC WHERE i.stay_id IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,fact_icu_observation
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.fact_icu_observation
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     xxhash64(
# MAGIC         c.stay_id,
# MAGIC         c.itemid,
# MAGIC         c.charttime
# MAGIC     ) AS observation_fact_key,
# MAGIC
# MAGIC     c.stay_id,
# MAGIC     c.hadm_id,
# MAGIC     
# MAGIC     p.patient_key,
# MAGIC     di.icu_item_key,
# MAGIC
# MAGIC     CAST(date_format(c.charttime, 'yyyyMMdd') AS INT) AS chart_date_key,
# MAGIC
# MAGIC     c.charttime,
# MAGIC     c.storetime,
# MAGIC
# MAGIC     c.valuenum,
# MAGIC     c.value,
# MAGIC     c.valueuom,
# MAGIC     c.warning
# MAGIC
# MAGIC FROM cdr_mimiciv.silver.chartevents c
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_patient p
# MAGIC     ON c.subject_id = p.subject_id
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_icu_item di
# MAGIC     ON c.itemid = di.itemid
# MAGIC
# MAGIC WHERE c.stay_id IS NOT NULL
# MAGIC   AND c.itemid IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,fact_lab
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.fact_lab
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     l.labevent_id AS lab_fact_key,
# MAGIC
# MAGIC     l.hadm_id,
# MAGIC
# MAGIC     p.patient_key,
# MAGIC     li.lab_item_key,
# MAGIC
# MAGIC     CAST(date_format(l.charttime, 'yyyyMMdd') AS INT) AS lab_date_key,
# MAGIC
# MAGIC     l.charttime,
# MAGIC     l.storetime,
# MAGIC
# MAGIC     l.valuenum,
# MAGIC     l.value,
# MAGIC     l.valueuom,
# MAGIC
# MAGIC     l.ref_range_lower,
# MAGIC     l.ref_range_upper,
# MAGIC
# MAGIC     l.flag,
# MAGIC     l.priority
# MAGIC
# MAGIC FROM cdr_mimiciv.silver.labevents l
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_patient p
# MAGIC     ON l.subject_id = p.subject_id
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_lab_item li
# MAGIC     ON l.itemid = li.itemid
# MAGIC
# MAGIC WHERE l.labevent_id IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,fact_procedure
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.fact_procedure
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     xxhash64(
# MAGIC         p.hadm_id,
# MAGIC         p.seq_num,
# MAGIC         p.icd_code,
# MAGIC         p.icd_version
# MAGIC     ) AS procedure_fact_key,
# MAGIC
# MAGIC     p.hadm_id,
# MAGIC     dp.patient_key,
# MAGIC     pr.procedure_key,
# MAGIC
# MAGIC     p.seq_num,
# MAGIC
# MAGIC     CAST(date_format(p.chartdate, 'yyyyMMdd') AS INT) AS procedure_date_key,
# MAGIC
# MAGIC     p.chartdate
# MAGIC
# MAGIC FROM cdr_mimiciv.silver.procedures_icd p
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_patient dp
# MAGIC     ON p.subject_id = dp.subject_id
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_procedure pr
# MAGIC     ON p.icd_code = pr.icd_code
# MAGIC    AND p.icd_version = pr.icd_version
# MAGIC
# MAGIC WHERE p.hadm_id IS NOT NULL
# MAGIC   AND p.icd_code IS NOT NULL
# MAGIC   AND p.icd_version IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,fact_medication_administration
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.fact_medication_administration
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     xxhash64(e.emar_id, ed.emar_seq) AS med_admin_fact_key,
# MAGIC     
# MAGIC     e.hadm_id,
# MAGIC     p.patient_key,
# MAGIC     dm.medication_key,
# MAGIC     
# MAGIC     CAST(date_format(e.charttime, 'yyyyMMdd') AS INT) AS admin_date_key,
# MAGIC     
# MAGIC     e.charttime,
# MAGIC     e.scheduletime,
# MAGIC     
# MAGIC     e.event_txt AS administration_status,
# MAGIC     ed.administration_type,
# MAGIC     ed.barcode_type,
# MAGIC     
# MAGIC     ed.dose_due,
# MAGIC     ed.dose_due_unit,
# MAGIC     ed.dose_given,
# MAGIC     ed.dose_given_unit,
# MAGIC     ed.route,
# MAGIC     ed.site,
# MAGIC     
# MAGIC     ed.complete_dose_not_given
# MAGIC
# MAGIC FROM cdr_mimiciv.silver.emar e
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.silver.emar_detail ed
# MAGIC     ON e.emar_id = ed.emar_id
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_patient p
# MAGIC     ON e.subject_id = p.subject_id
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.silver.prescriptions rx
# MAGIC     ON e.pharmacy_id = rx.pharmacy_id
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_medication dm
# MAGIC     ON rx.drug_type <=> dm.drug_type
# MAGIC    AND rx.drug <=> dm.drug
# MAGIC    AND rx.formulary_drug_cd <=> dm.formulary_drug_cd
# MAGIC    AND rx.gsn <=> dm.gsn
# MAGIC    AND rx.ndc <=> dm.ndc
# MAGIC
# MAGIC WHERE e.emar_id IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,fact_admission
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.fact_admission
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     a.hadm_id,
# MAGIC     p.patient_key,
# MAGIC     at.admission_type_key,
# MAGIC     al.location_key AS admission_location_key,
# MAGIC     dl.location_key AS discharge_location_key,
# MAGIC
# MAGIC     CAST(date_format(a.admittime, 'yyyyMMdd') AS INT) AS admit_date_key,
# MAGIC     CAST(date_format(a.dischtime, 'yyyyMMdd') AS INT) AS discharge_date_key,
# MAGIC
# MAGIC     a.admittime,
# MAGIC     a.dischtime,
# MAGIC     a.deathtime,
# MAGIC
# MAGIC     a.hospital_expire_flag,
# MAGIC
# MAGIC     a.insurance,
# MAGIC     a.language,
# MAGIC     a.marital_status,
# MAGIC     a.race,
# MAGIC
# MAGIC     a.los_hours,
# MAGIC     a.los_days
# MAGIC
# MAGIC FROM cdr_mimiciv.silver.admissions a
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_patient p
# MAGIC     ON a.subject_id = p.subject_id
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_admission_type at
# MAGIC     ON a.admission_type = at.admission_type
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_location al
# MAGIC     ON al.location_type = 'ADMISSION'
# MAGIC    AND a.admission_location = al.location_name
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_location dl
# MAGIC     ON dl.location_type = 'DISCHARGE'
# MAGIC    AND a.discharge_location = dl.location_name
# MAGIC
# MAGIC WHERE a.hadm_id IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,fact_diagnosis
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.fact_diagnosis
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     xxhash64(
# MAGIC         d.hadm_id,
# MAGIC         d.seq_num,
# MAGIC         d.icd_code,
# MAGIC         d.icd_version
# MAGIC     ) AS diagnosis_fact_key,
# MAGIC
# MAGIC     d.hadm_id,
# MAGIC     p.patient_key,
# MAGIC     dx.diagnosis_key,
# MAGIC
# MAGIC     d.seq_num,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN d.seq_num = 1 THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS primary_diagnosis_flag,
# MAGIC
# MAGIC     fa.admit_date_key,
# MAGIC     fa.discharge_date_key
# MAGIC
# MAGIC FROM cdr_mimiciv.silver.diagnoses_icd d
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_patient p
# MAGIC     ON d.subject_id = p.subject_id
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_diagnosis dx
# MAGIC     ON d.icd_code = dx.icd_code
# MAGIC    AND d.icd_version = dx.icd_version
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.fact_admission fa
# MAGIC     ON d.hadm_id = fa.hadm_id
# MAGIC
# MAGIC WHERE d.hadm_id IS NOT NULL
# MAGIC   AND d.icd_code IS NOT NULL
# MAGIC   AND d.icd_version IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,fact_transfer
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.fact_transfer
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     t.transfer_id,
# MAGIC
# MAGIC     p.patient_key,
# MAGIC     t.hadm_id,
# MAGIC     cu.careunit_key,
# MAGIC
# MAGIC     CAST(date_format(t.intime, 'yyyyMMdd') AS INT) AS in_date_key,
# MAGIC     CAST(date_format(t.outtime, 'yyyyMMdd') AS INT) AS out_date_key,
# MAGIC
# MAGIC     t.eventtype,
# MAGIC
# MAGIC     t.intime,
# MAGIC     t.outtime,
# MAGIC
# MAGIC     t.transfer_duration_hours,
# MAGIC     t.dq_invalid_transfer_time
# MAGIC
# MAGIC FROM cdr_mimiciv.silver.transfers t
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_patient p
# MAGIC     ON t.subject_id = p.subject_id
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_careunit cu
# MAGIC     ON t.careunit = cu.careunit_name
# MAGIC
# MAGIC WHERE t.transfer_id IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,fact_microbiology
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.fact_microbiology
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     m.microevent_id,
# MAGIC     m.micro_specimen_id,
# MAGIC
# MAGIC     p.patient_key,
# MAGIC     m.hadm_id,
# MAGIC     mt.microbiology_test_key,
# MAGIC     pr.provider_key,
# MAGIC
# MAGIC     CAST(date_format(m.chartdate, 'yyyyMMdd') AS INT) AS chart_date_key,
# MAGIC     CAST(date_format(m.storedate, 'yyyyMMdd') AS INT) AS store_date_key,
# MAGIC
# MAGIC     m.charttime,
# MAGIC     m.storetime,
# MAGIC
# MAGIC     m.test_seq,
# MAGIC     m.org_itemid,
# MAGIC     m.org_name,
# MAGIC     m.isolate_num,
# MAGIC     m.quantity
# MAGIC
# MAGIC FROM cdr_mimiciv.silver.microbiologyevents m
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_patient p
# MAGIC     ON m.subject_id = p.subject_id
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_provider pr
# MAGIC     ON m.order_provider_id = pr.provider_id
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_microbiology_test mt
# MAGIC     ON m.spec_itemid <=> mt.spec_itemid
# MAGIC    AND m.test_itemid = mt.test_itemid
# MAGIC
# MAGIC WHERE m.microevent_id IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,fact_service
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.fact_service
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     s.hadm_id,
# MAGIC     p.patient_key,
# MAGIC
# MAGIC     ps.service_key AS previous_service_key,
# MAGIC     cs.service_key AS current_service_key,
# MAGIC
# MAGIC     CAST(date_format(s.transfertime, 'yyyyMMdd') AS INT) AS transfer_date_key,
# MAGIC
# MAGIC     s.transfertime
# MAGIC
# MAGIC FROM cdr_mimiciv.silver.services s
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_patient p
# MAGIC     ON s.subject_id = p.subject_id
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_service ps
# MAGIC     ON s.prev_service = ps.service
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_service cs
# MAGIC     ON s.curr_service = cs.service
# MAGIC
# MAGIC WHERE s.hadm_id IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,fact_prescription
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cdr_mimiciv.gold.fact_prescription
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     pr.pharmacy_id,
# MAGIC     pr.poe_id,
# MAGIC     pr.poe_seq,
# MAGIC
# MAGIC     p.patient_key,
# MAGIC     pr.hadm_id,
# MAGIC     m.medication_key,
# MAGIC     prov.provider_key,
# MAGIC
# MAGIC     CAST(
# MAGIC         date_format(TRY_CAST(pr.starttime AS TIMESTAMP), 'yyyyMMdd')
# MAGIC         AS INT
# MAGIC     ) AS start_date_key,
# MAGIC
# MAGIC     CAST(
# MAGIC         date_format(TRY_CAST(pr.stoptime AS TIMESTAMP), 'yyyyMMdd')
# MAGIC         AS INT
# MAGIC     ) AS stop_date_key,
# MAGIC
# MAGIC     TRY_CAST(pr.starttime AS TIMESTAMP) AS starttime,
# MAGIC     TRY_CAST(pr.stoptime AS TIMESTAMP) AS stoptime,
# MAGIC
# MAGIC     pr.prod_strength,
# MAGIC     pr.form_rx,
# MAGIC     pr.dose_val_rx,
# MAGIC     pr.dose_unit_rx,
# MAGIC     pr.form_val_disp
# MAGIC
# MAGIC FROM cdr_mimiciv.silver.prescriptions pr
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_patient p
# MAGIC     ON pr.subject_id = CAST(p.subject_id AS STRING)
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_provider prov
# MAGIC     ON pr.order_provider_id = prov.provider_id
# MAGIC
# MAGIC LEFT JOIN cdr_mimiciv.gold.dim_medication m
# MAGIC     ON pr.drug_type <=> m.drug_type
# MAGIC    AND pr.drug <=> m.drug
# MAGIC    AND pr.formulary_drug_cd <=> m.formulary_drug_cd
# MAGIC    AND pr.gsn <=> m.gsn
# MAGIC    AND pr.ndc <=> m.ndc
# MAGIC
# MAGIC WHERE pr.hadm_id IS NOT NULL;