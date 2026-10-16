# Databricks notebook source
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- TABLE: admissions
# MAGIC -- Source: Official MIMIC-IV HOSP documentation
# MAGIC -- ============================================================
# MAGIC
# MAGIC COMMENT ON TABLE cdr_mimiciv.bronze.admissions IS
# MAGIC 'The admissions table gives information regarding a patient\'s admission to the hospital. Since each unique hospital visit for a patient is assigned a unique hadm_id, the admissions table can be considered as a definition table for hadm_id. Information available includes timing information for admission and discharge, demographic information, the source of the admission, and so on.';
# MAGIC
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.admissions
# MAGIC ALTER COLUMN subject_id COMMENT
# MAGIC 'It is possible for this table to have duplicate subject_id, indicating that a single patient had multiple admissions to the hospital. The ADMISSIONS table can be linked to the PATIENTS table using subject_id.';
# MAGIC
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.admissions
# MAGIC ALTER COLUMN hadm_id COMMENT
# MAGIC 'Each row of this table contains a unique hadm_id, which represents a single patient\'s admission to the hospital. hadm_id ranges from 2000000 - 2999999.';
# MAGIC
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.admissions
# MAGIC ALTER COLUMN admittime COMMENT
# MAGIC 'admittime provides the date and time the patient was admitted to the hospital, while dischtime provides the date and time the patient was discharged from the hospital.';
# MAGIC
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.admissions
# MAGIC ALTER COLUMN dischtime COMMENT
# MAGIC 'admittime provides the date and time the patient was admitted to the hospital, while dischtime provides the date and time the patient was discharged from the hospital.';
# MAGIC
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.admissions
# MAGIC ALTER COLUMN deathtime COMMENT
# MAGIC 'If applicable, deathtime provides the time of in-hospital death for the patient. Note that deathtime is only present if the patient died in-hospital, and is almost always the same as the patient\'s dischtime. However, there can be some discrepancies due to typographical errors.';
# MAGIC
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.admissions
# MAGIC ALTER COLUMN admission_type COMMENT
# MAGIC 'admission_type is useful for classifying the urgency of the admission. There are 9 possibilities: AMBULATORY OBSERVATION, DIRECT EMER., DIRECT OBSERVATION, ELECTIVE, EU OBSERVATION, EW EMER., OBSERVATION ADMIT, SURGICAL SAME DAY ADMISSION, URGENT.';
# MAGIC
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.admissions
# MAGIC ALTER COLUMN admit_provider_id COMMENT
# MAGIC 'admit_provider_id provides an anonymous identifier for the provider who admitted the patient. Provider identifiers follow a consistent pattern: the letter “P”, followed by either three numbers, followed by two letters or two numbers. For example, “P003AB”, “P00102”, “P1248B”, etc. Provider identifiers are randomly generated and do not have any inherent meaning aside from uniquely identifying the same provider across the database.';
# MAGIC
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.admissions
# MAGIC ALTER COLUMN admission_location COMMENT
# MAGIC 'admission_location provides information about the location of the patient prior to arriving at the hospital. Note that as the emergency room is technically a clinic, patients who are admitted via the emergency room usually have it as their admission location.';
# MAGIC
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.admissions
# MAGIC ALTER COLUMN discharge_location COMMENT
# MAGIC 'Similarly, discharge_location is the disposition of the patient after they are discharged from the hospital.';
# MAGIC
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.admissions
# MAGIC ALTER COLUMN insurance COMMENT
# MAGIC 'The insurance, language, marital_status, and ethnicity columns provide information about patient demographics for the given hospitalization. Note that as this data is documented for each hospital admission, they may change from stay to stay.';
# MAGIC
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.admissions
# MAGIC ALTER COLUMN language COMMENT
# MAGIC 'The insurance, language, marital_status, and ethnicity columns provide information about patient demographics for the given hospitalization. Note that as this data is documented for each hospital admission, they may change from stay to stay.';
# MAGIC
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.admissions
# MAGIC ALTER COLUMN marital_status COMMENT
# MAGIC 'The insurance, language, marital_status, and ethnicity columns provide information about patient demographics for the given hospitalization. Note that as this data is documented for each hospital admission, they may change from stay to stay.';
# MAGIC
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.admissions
# MAGIC ALTER COLUMN race COMMENT
# MAGIC 'The insurance, language, marital_status, and ethnicity columns provide information about patient demographics for the given hospitalization. Note that as this data is documented for each hospital admission, they may change from stay to stay.';
# MAGIC
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.admissions
# MAGIC ALTER COLUMN edregtime COMMENT
# MAGIC 'The date and time at which the patient was registered and discharged from the emergency department.';
# MAGIC
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.admissions
# MAGIC ALTER COLUMN edouttime COMMENT
# MAGIC 'The date and time at which the patient was registered and discharged from the emergency department.';
# MAGIC
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.admissions
# MAGIC ALTER COLUMN hospital_expire_flag COMMENT
# MAGIC 'This is a binary flag which indicates whether the patient died within the given hospitalization. 1 indicates death in the hospital, and 0 indicates survival to hospital discharge.';