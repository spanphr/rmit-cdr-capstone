# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Apply tags (job task 1 of 2)
# MAGIC
# MAGIC Job version of `04_Governance/tags/*` (README: "Default + Override" taxonomy). One task applies all three levels:
# MAGIC
# MAGIC | Level | Tags | Scope |
# MAGIC |---|---|---|
# MAGIC | schema | `layer` | bronze, silver, gold |
# MAGIC | table | `layer`, `mimic_module` (+ `type`, `domain` in Gold) | every table in the three layers; Gold uses the config below (27 tables, POE included) |
# MAGIC | column | `hipaa_identifiability`, `hl7_confidentiality` | every Gold column: default `de_identified` / `N_Normal`, overrides below |
# MAGIC
# MAGIC Idempotent: `SET TAGS` overwrites the same key with the same value. Tags set by teammates under other keys
# MAGIC (`pii_level`, `access_tier`, `data_domain`) are left alone. Views are tagged `type=view`.
# MAGIC
# MAGIC Widgets: `mode` = `dry_run` | `apply`; `layers` = comma list of schemas to tag (default `bronze,silver,gold`).

# COMMAND ----------

dbutils.widgets.dropdown("mode", "dry_run", ["dry_run", "apply"])
dbutils.widgets.text("catalog", "cdr_mimiciv")
dbutils.widgets.text("layers", "bronze,silver,gold")

MODE = dbutils.widgets.get("mode")
CATALOG = dbutils.widgets.get("catalog")
LAYERS = [l.strip() for l in dbutils.widgets.get("layers").split(",") if l.strip()]
assert CATALOG == "cdr_mimiciv"
assert set(LAYERS) <= {"bronze", "silver", "gold"}, LAYERS
print(f"mode={MODE} layers={LAYERS}")

# COMMAND ----------

# Gold table-level config: the 24 tables from tags/Gold_Table_level.py plus the 3 POE tables from the Gold design (§3.3, §6)
GOLD_TABLE_TAGS = {
    "dim_patient":                   {"type": "dimension", "domain": "demographics",   "mimic_module": "hosp"},
    "dim_location":                  {"type": "dimension", "domain": "encounter",      "mimic_module": "hosp"},
    "dim_admission_type":            {"type": "dimension", "domain": "encounter",      "mimic_module": "hosp"},
    "dim_provider":                  {"type": "dimension", "domain": "encounter",      "mimic_module": "hosp"},
    "dim_service":                   {"type": "dimension", "domain": "encounter",      "mimic_module": "hosp"},
    "dim_date":                      {"type": "dimension", "domain": "encounter",      "mimic_module": "hosp"},
    "fact_admission":                {"type": "fact",      "domain": "encounter",      "mimic_module": "hosp"},
    "fact_transfer":                 {"type": "fact",      "domain": "encounter",      "mimic_module": "hosp"},
    "fact_service":                  {"type": "fact",      "domain": "encounter",      "mimic_module": "hosp"},
    "dim_careunit":                  {"type": "dimension", "domain": "critical_care",  "mimic_module": "icu"},
    "dim_icu_item":                  {"type": "dimension", "domain": "critical_care",  "mimic_module": "icu"},
    "fact_icu_stay":                 {"type": "fact",      "domain": "critical_care",  "mimic_module": "icu"},
    "fact_icu_observation":          {"type": "fact",      "domain": "critical_care",  "mimic_module": "icu"},
    "dim_medication":                {"type": "dimension", "domain": "pharmacy",       "mimic_module": "hosp"},
    "fact_prescription":             {"type": "fact",      "domain": "pharmacy",       "mimic_module": "hosp"},
    "fact_medication_administration":{"type": "fact",      "domain": "pharmacy",       "mimic_module": "hosp"},
    "dim_lab_item":                  {"type": "dimension", "domain": "diagnostics",    "mimic_module": "hosp"},
    "dim_microbiology_test":         {"type": "dimension", "domain": "diagnostics",    "mimic_module": "hosp"},
    "fact_lab":                      {"type": "fact",      "domain": "diagnostics",    "mimic_module": "hosp"},
    "fact_microbiology":             {"type": "fact",      "domain": "diagnostics",    "mimic_module": "hosp"},
    "dim_diagnosis":                 {"type": "dimension", "domain": "clinical_coding","mimic_module": "hosp"},
    "dim_procedure":                 {"type": "dimension", "domain": "clinical_coding","mimic_module": "hosp"},
    "fact_diagnosis":                {"type": "fact",      "domain": "clinical_coding","mimic_module": "hosp"},
    "fact_procedure":                {"type": "fact",      "domain": "clinical_coding","mimic_module": "hosp"},
    # POE (new in the Gold rewrite)
    "dim_order_type":                {"type": "dimension", "domain": "orders",         "mimic_module": "hosp"},
    "fact_order":                    {"type": "fact",      "domain": "orders",         "mimic_module": "hosp"},
    "fact_order_detail":             {"type": "fact",      "domain": "orders",         "mimic_module": "hosp"},
}

# Bronze/Silver: mimic_module by source table name (MIMIC-IV docs: icu module tables, everything else hosp)
ICU_TABLES = {"icustays", "chartevents", "d_items", "datetimeevents", "inputevents", "outputevents",
              "procedureevents", "ingredientevents", "caregiver"}
def mimic_module(table): return "icu" if table.lower() in ICU_TABLES else "hosp"

# Gold column-level overrides (tags/Gold_Column_level.py plus identifiers and POE free text)
Q, R, V = "phi_quasi_identifier", "R_Restricted", "V_Very_Restricted"
COLUMN_OVERRIDES = {
    "dim_patient": {
        "subject_id":        {"hipaa_identifiability": "phi_direct_identifier", "hl7_confidentiality": R},
        "gender":            {"hipaa_identifiability": Q, "hl7_confidentiality": R},
        "anchor_age":        {"hipaa_identifiability": Q, "hl7_confidentiality": R},
        "anchor_year":       {"hipaa_identifiability": Q, "hl7_confidentiality": R},
        "anchor_year_group": {"hipaa_identifiability": Q, "hl7_confidentiality": R},
    },
    "dim_provider": {
        "provider_id":       {"hipaa_identifiability": "phi_direct_identifier", "hl7_confidentiality": R},
    },
    "fact_admission": {
        "insurance":         {"hipaa_identifiability": Q, "hl7_confidentiality": R},
        "language":          {"hipaa_identifiability": Q, "hl7_confidentiality": R},
        "marital_status":    {"hipaa_identifiability": Q, "hl7_confidentiality": R},
        "race":              {"hipaa_identifiability": Q, "hl7_confidentiality": R},
        "deathtime":         {"hipaa_identifiability": Q, "hl7_confidentiality": V},
    },
    "fact_diagnosis": {
        "primary_diagnosis_flag": {"hipaa_identifiability": "de_identified", "hl7_confidentiality": R},
    },
    "fact_order_detail": {
        "field_value":       {"hipaa_identifiability": Q, "hl7_confidentiality": R},   # free text
    },
}
COLUMN_DEFAULT = {"hipaa_identifiability": "de_identified", "hl7_confidentiality": "N_Normal"}

# COMMAND ----------

def tag_clause(tags): return ", ".join(f"'{k}'='{v}'" for k, v in tags.items())

statements = []   # (label, sql)
for layer in LAYERS:
    statements.append((f"schema {layer}", f"ALTER SCHEMA {CATALOG}.{layer} SET TAGS ('layer'='{layer}')"))

tables = spark.sql(f"""
  SELECT table_schema, table_name, table_type FROM {CATALOG}.information_schema.tables
  WHERE table_schema IN ({", ".join(f"'{l}'" for l in LAYERS)}) ORDER BY 1, 2""").collect()

for t in tables:
    is_view = t.table_type == "VIEW"
    fqn = f"{CATALOG}.{t.table_schema}.{t.table_name}"
    if t.table_schema == "gold":
        cfg = GOLD_TABLE_TAGS.get(t.table_name)
        if cfg is None:
            print(f"WARNING: gold.{t.table_name} is not in GOLD_TABLE_TAGS; tagging layer/mimic_module only")
            cfg = {"type": "view" if is_view else "unclassified", "mimic_module": "hosp"}
        tags = {"layer": "gold", **cfg}
    else:
        tags = {"layer": t.table_schema, "type": "view" if is_view else "table", "mimic_module": mimic_module(t.table_name)}
    obj = "VIEW" if is_view else "TABLE"
    statements.append((f"{obj.lower()} {fqn}", f"ALTER {obj} {fqn} SET TAGS ({tag_clause(tags)})"))

missing = sorted(set(GOLD_TABLE_TAGS) - {t.table_name for t in tables if t.table_schema == "gold"})
if "gold" in LAYERS and missing:
    print("NOTE: configured Gold tables not present yet (Gold_Build_All not run?):", missing)

# Column level (Gold only; views are skipped, tags on tables carry no further)
if "gold" in LAYERS:
    cols = spark.sql(f"""
      SELECT c.table_name, c.column_name FROM {CATALOG}.information_schema.columns c
      JOIN {CATALOG}.information_schema.tables t ON c.table_schema = t.table_schema AND c.table_name = t.table_name
      WHERE c.table_schema = 'gold' AND t.table_type <> 'VIEW' ORDER BY 1, c.ordinal_position""").collect()
    for c in cols:
        tags = COLUMN_OVERRIDES.get(c.table_name, {}).get(c.column_name, COLUMN_DEFAULT)
        statements.append((f"column gold.{c.table_name}.{c.column_name}",
                           f"ALTER TABLE {CATALOG}.gold.{c.table_name} ALTER COLUMN `{c.column_name}` SET TAGS ({tag_clause(tags)})"))
    # overrides that point at columns that do not exist are a config error worth seeing
    have = {(c.table_name, c.column_name) for c in cols}
    for tbl, cmap in COLUMN_OVERRIDES.items():
        for col in cmap:
            if (tbl, col) not in have:
                print(f"WARNING: override for gold.{tbl}.{col} but column not found")

print(f"{len(statements)} tag statements prepared")

# COMMAND ----------

ok, errors = 0, []
for label, sql in statements:
    if MODE == "dry_run":
        print(sql)
        continue
    try:
        spark.sql(sql); ok += 1
    except Exception as e:
        errors.append((label, str(e).splitlines()[0][:200]))
print(f"\nmode={MODE} applied={ok} errors={len(errors)}")
for label, err in errors:
    print("ERROR:", label, "->", err)

# COMMAND ----------

if MODE == "apply":
    display(spark.sql(f"""SELECT schema_name, tag_name, tag_value, COUNT(*) AS tables
                          FROM {CATALOG}.information_schema.table_tags
                          WHERE tag_name IN ('layer','type','domain','mimic_module') GROUP BY ALL ORDER BY 1,2,3"""))
    display(spark.sql(f"""SELECT tag_name, tag_value, COUNT(*) AS columns
                          FROM {CATALOG}.information_schema.column_tags
                          WHERE schema_name = 'gold' AND tag_name IN ('hipaa_identifiability','hl7_confidentiality')
                          GROUP BY ALL ORDER BY 1,2"""))
    assert not errors, f"{len(errors)} tag statements failed; see output"

dbutils.notebook.exit(f"01_apply_tags mode={MODE} statements={len(statements)} errors={len(errors)}")
