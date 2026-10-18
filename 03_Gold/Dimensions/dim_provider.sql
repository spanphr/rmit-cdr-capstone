CREATE OR REPLACE VIEW cdr_mimiciv.silver.v_provider AS
SELECT
    provider_id
FROM cdr_mimiciv.silver.provider
WHERE provider_id IS NOT NULL;

CREATE OR REPLACE TABLE cdr_mimiciv.gold.dim_provider
USING DELTA
AS

SELECT
    xxhash64(provider_id) AS provider_key,
    provider_id
FROM cdr_mimiciv.silver.v_provider;
