%sql
CREATE OR REPLACE TABLE cdr_mimiciv.gold.fact_procedure
USING DELTA
AS

SELECT
    xxhash64(
        p.hadm_id,
        p.seq_num,
        p.icd_code,
        p.icd_version
    ) AS procedure_fact_key,

    p.hadm_id,
    dp.patient_key,
    pr.procedure_key,

    p.seq_num,

    CAST(date_format(p.chartdate, 'yyyyMMdd') AS INT) AS procedure_date_key,

    p.chartdate

FROM cdr_mimiciv.silver.procedures_icd p

LEFT JOIN cdr_mimiciv.gold.dim_patient dp
    ON p.subject_id = dp.subject_id

LEFT JOIN cdr_mimiciv.gold.dim_procedure pr
    ON p.icd_code = pr.icd_code
   AND p.icd_version = pr.icd_version

WHERE p.hadm_id IS NOT NULL
  AND p.icd_code IS NOT NULL
  AND p.icd_version IS NOT NULL;
