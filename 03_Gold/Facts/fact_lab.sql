CREATE OR REPLACE TABLE cdr_mimiciv.gold.fact_lab
USING DELTA
AS

SELECT
    l.labevent_id AS lab_fact_key,

    l.hadm_id,

    p.patient_key,
    li.lab_item_key,

    CAST(date_format(l.charttime, 'yyyyMMdd') AS INT) AS lab_date_key,

    l.charttime,
    l.storetime,

    l.valuenum,
    l.value,
    l.valueuom,

    l.ref_range_lower,
    l.ref_range_upper,

    l.flag,
    l.priority

FROM cdr_mimiciv.silver.labevents l

LEFT JOIN cdr_mimiciv.gold.dim_patient p
    ON l.subject_id = p.subject_id

LEFT JOIN cdr_mimiciv.gold.dim_lab_item li
    ON l.itemid = li.itemid

WHERE l.labevent_id IS NOT NULL;
