# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Verify tags (job task 2 of 2)
# MAGIC
# MAGIC Checks that `01_apply_tags` left the catalog in the expected state. Metadata only, no table data is read.
# MAGIC Fails the task when `fail_on_gap=true` and anything is missing, so the job run goes red.
# MAGIC
# MAGIC Checks:
# MAGIC 1. `layer` schema tag on bronze, silver, gold
# MAGIC 2. every table and view in the three layers has `layer`, `type` and `mimic_module`
# MAGIC 3. every Gold table has `domain`
# MAGIC 4. every Gold column (base tables) has `hipaa_identifiability` and `hl7_confidentiality`
# MAGIC 5. the override columns carry their override values (not the default)

# COMMAND ----------

dbutils.widgets.text("catalog", "cdr_mimiciv")
dbutils.widgets.dropdown("fail_on_gap", "true", ["true", "false"])
CAT = dbutils.widgets.get("catalog")
FAIL = dbutils.widgets.get("fail_on_gap") == "true"
assert CAT == "cdr_mimiciv"

gaps = []
def check(label, cond, detail=""):
    print(("PASS  " if cond else "GAP   ") + label + (f": {detail}" if detail else ""))
    if not cond:
        gaps.append(label)

# COMMAND ----------

# 1. schema tags
schema_tags = {(r.schema_name, r.tag_value) for r in spark.sql(
    f"SELECT schema_name, tag_value FROM {CAT}.information_schema.schema_tags WHERE tag_name = 'layer'").collect()}
for layer in ["bronze", "silver", "gold"]:
    check(f"schema tag layer={layer}", (layer, layer) in schema_tags)

# COMMAND ----------

# 2 and 3. table tags
for tag in ["layer", "type", "mimic_module"]:
    missing = spark.sql(f"""
      SELECT t.table_schema, t.table_name FROM {CAT}.information_schema.tables t
      LEFT JOIN {CAT}.information_schema.table_tags g
        ON g.schema_name = t.table_schema AND g.table_name = t.table_name AND g.tag_name = '{tag}'
      WHERE t.table_schema IN ('bronze', 'silver', 'gold') AND g.tag_value IS NULL ORDER BY 1, 2""").collect()
    check(f"every bronze/silver/gold table has '{tag}'", not missing,
          ", ".join(f"{r.table_schema}.{r.table_name}" for r in missing[:10]) + (" ..." if len(missing) > 10 else ""))

missing_domain = spark.sql(f"""
  SELECT t.table_name FROM {CAT}.information_schema.tables t
  LEFT JOIN {CAT}.information_schema.table_tags g
    ON g.schema_name = t.table_schema AND g.table_name = t.table_name AND g.tag_name = 'domain'
  WHERE t.table_schema = 'gold' AND g.tag_value IS NULL ORDER BY 1""").collect()
check("every gold table has 'domain'", not missing_domain, ", ".join(r.table_name for r in missing_domain))

wrong_layer = spark.sql(f"""
  SELECT schema_name, table_name, tag_value FROM {CAT}.information_schema.table_tags
  WHERE tag_name = 'layer' AND tag_value <> schema_name""").collect()
check("layer tag value equals schema name", not wrong_layer,
      ", ".join(f"{r.schema_name}.{r.table_name}={r.tag_value}" for r in wrong_layer[:10]))

# COMMAND ----------

# 4. gold column tags
for tag in ["hipaa_identifiability", "hl7_confidentiality"]:
    missing = spark.sql(f"""
      SELECT c.table_name, c.column_name FROM {CAT}.information_schema.columns c
      JOIN {CAT}.information_schema.tables t
        ON t.table_schema = c.table_schema AND t.table_name = c.table_name AND t.table_type <> 'VIEW'
      LEFT JOIN {CAT}.information_schema.column_tags g
        ON g.schema_name = c.table_schema AND g.table_name = c.table_name
       AND g.column_name = c.column_name AND g.tag_name = '{tag}'
      WHERE c.table_schema = 'gold' AND g.tag_value IS NULL""").collect()
    check(f"every gold column has '{tag}'", not missing, f"{len(missing)} missing" if missing else "")

# COMMAND ----------

# 5. overrides (keep in step with COLUMN_OVERRIDES in 01_apply_tags)
EXPECTED = {
    ("dim_patient", "subject_id", "hipaa_identifiability"): "phi_direct_identifier",
    ("dim_patient", "gender", "hl7_confidentiality"): "R_Restricted",
    ("dim_provider", "provider_id", "hipaa_identifiability"): "phi_direct_identifier",
    ("fact_admission", "race", "hipaa_identifiability"): "phi_quasi_identifier",
    ("fact_admission", "deathtime", "hl7_confidentiality"): "V_Very_Restricted",
    ("fact_order_detail", "field_value", "hl7_confidentiality"): "R_Restricted",
}
have = {(r.table_name, r.column_name, r.tag_name): r.tag_value for r in spark.sql(
    f"SELECT table_name, column_name, tag_name, tag_value FROM {CAT}.information_schema.column_tags WHERE schema_name = 'gold'").collect()}
built = {(r.table_name, r.column_name) for r in spark.sql(
    f"SELECT table_name, column_name FROM {CAT}.information_schema.columns WHERE table_schema = 'gold'").collect()}
for (tbl, col, tag), val in EXPECTED.items():
    if (tbl, col) not in built:
        print(f"SKIP  gold.{tbl}.{col} not built yet")
        continue
    check(f"gold.{tbl}.{col} {tag}={val}", have.get((tbl, col, tag)) == val, have.get((tbl, col, tag), "none"))

# COMMAND ----------

# Summary tables (evidence for the report)
display(spark.sql(f"""SELECT schema_name, tag_name, tag_value, COUNT(*) AS tables
                      FROM {CAT}.information_schema.table_tags
                      WHERE tag_name IN ('layer', 'type', 'domain', 'mimic_module') GROUP BY ALL ORDER BY 1, 2, 3"""))
display(spark.sql(f"""SELECT tag_name, tag_value, COUNT(*) AS columns
                      FROM {CAT}.information_schema.column_tags
                      WHERE schema_name = 'gold' AND tag_name IN ('hipaa_identifiability', 'hl7_confidentiality')
                      GROUP BY ALL ORDER BY 1, 2"""))

print(f"\n{len(gaps)} gaps")
for g in gaps:
    print("  -", g)
if FAIL and gaps:
    raise Exception(f"Tag verification found {len(gaps)} gaps: {gaps}")
dbutils.notebook.exit(f"02_verify_tags gaps={len(gaps)}")
