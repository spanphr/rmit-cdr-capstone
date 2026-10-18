CREATE OR REPLACE VIEW cdr_mimiciv.silver.v_medication AS
SELECT DISTINCT
    drug,
    ndc,
    drug_type,
    formulary_drug_cd
FROM cdr_mimiciv.silver.prescriptions
WHERE drug IS NOT NULL;

CREATE OR REPLACE TABLE cdr_mimiciv.gold.dim_medication
USING DELTA
AS

SELECT
    xxhash64(drug, ndc) AS medication_key,
    drug,
    ndc,
    drug_type,
    formulary_drug_cd
FROM cdr_mimiciv.silver.v_medication;
