# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- TABLE: outputevents
# MAGIC -- Source: Official MIMIC-IV ICU documentation
# MAGIC -- ============================================================
# MAGIC
# MAGIC COMMENT ON TABLE cdr_mimiciv.bronze.outputevents IS
# MAGIC 'Output data for patients.';
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.outputevents
# MAGIC ALTER COLUMN subject_id COMMENT
# MAGIC 'Identifiers which specify the patient: subject_id is unique to a patient, hadm_id is unique to a patient hospital stay and stay_id is unique to a patient ICU stay.';
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.outputevents
# MAGIC ALTER COLUMN hadm_id COMMENT
# MAGIC 'Identifiers which specify the patient: subject_id is unique to a patient, hadm_id is unique to a patient hospital stay and stay_id is unique to a patient ICU stay.';
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.outputevents
# MAGIC ALTER COLUMN stay_id COMMENT
# MAGIC 'Identifiers which specify the patient: subject_id is unique to a patient, hadm_id is unique to a patient hospital stay and stay_id is unique to a patient ICU stay.';
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.outputevents
# MAGIC ALTER COLUMN caregiver_id COMMENT
# MAGIC 'caregiver_id uniquely identifies a single caregiver who documented data in the ICU information system.';
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.outputevents
# MAGIC ALTER COLUMN charttime COMMENT
# MAGIC 'charttime is the time of an output event.';
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.outputevents
# MAGIC ALTER COLUMN storetime COMMENT
# MAGIC 'storetime records the time at which an observation was manually input or manually validated by a member of the clinical staff.';
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.outputevents
# MAGIC ALTER COLUMN itemid COMMENT
# MAGIC 'Identifier for a single measurement type in the database. Each row associated with one itemid (e.g. 212) corresponds to an instantiation of the same measurement (e.g. heart rate).';
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.outputevents
# MAGIC ALTER COLUMN value COMMENT
# MAGIC 'value and valueuom list the amount of a substance at the charttime (when the exact start time is unknown, but usually up to an hour before).';
# MAGIC
# MAGIC ALTER TABLE cdr_mimiciv.bronze.outputevents
# MAGIC ALTER COLUMN valueuom COMMENT
# MAGIC 'value and valueuom list the amount of a substance at the charttime (when the exact start time is unknown, but usually up to an hour before).';
# MAGIC