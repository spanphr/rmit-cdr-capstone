CREATE OR REPLACE TABLE cdr_mimiciv.gold.dim_procedure
USING DELTA
AS

SELECT
    xxhash64(icd_code, icd_version) AS procedure_key,
    icd_code,
    icd_version,
    long_title AS procedure_title
FROM cdr_mimiciv.silver.d_icd_procedures
WHERE icd_code IS NOT NULL
  AND icd_version IS NOT NULL;
