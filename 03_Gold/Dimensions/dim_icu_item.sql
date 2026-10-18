CREATE OR REPLACE TABLE cdr_mimiciv.gold.dim_icu_item
USING DELTA
AS

SELECT
    xxhash64(itemid) AS icu_item_key,
    itemid,
    label AS item_name,
    abbreviation,
    linksto,
    category,
    unitname,
    param_type
FROM cdr_mimiciv.silver.d_items
WHERE itemid IS NOT NULL;
