CREATE OR REPLACE TABLE cdr_mimiciv.gold.dim_lab_item
USING DELTA
AS

SELECT
    xxhash64(itemid) AS lab_item_key,
    itemid,
    label AS lab_test_name,
    fluid,
    category
FROM cdr_mimiciv.silver.d_labitems
WHERE itemid IS NOT NULL;
