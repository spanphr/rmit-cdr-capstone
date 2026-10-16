# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Applies column masks to cdr_mimiciv from the team's rbac.data_classification.
# Import via Workspace > Create > Import. Run all cells with MODE = "dry_run" first,
# read the printed statements, then set MODE = "apply".
# MODE = "rollback" removes the masks it would have applied.
# Prerequisite: the mask functions from Phase 4 of Governance_Implementation_Runbook.sql
# exist, and you are a member of cdr_admins (the next cell checks).

# COMMAND ----------

MODE = "apply"          # "dry_run" | "apply" | "rollback"
CATALOG = "cdr_mimiciv"

# COMMAND ----------

# Safety check: stop if you are not in a privileged group, so you do not lock yourself out.
me_admin = spark.sql("SELECT is_account_group_member('cdr_admins')").first()[0]
assert me_admin, "You are not in cdr_admins. Add yourself to the group before running this notebook."
print("Membership check passed.")

# COMMAND ----------

TYPE_MAP = {"INT": "int", "INTEGER": "int", "BIGINT": "bigint", "LONG": "bigint",
            "STRING": "string", "DATE": "date", "TIMESTAMP": "timestamp"}

def mask_fn(handling, dtype):
    prefix = "mask_cres" if handling == "Controlled Research" else "mask_ctrl"
    return f"{CATALOG}.rbac.{prefix}_{TYPE_MAP[dtype.upper()]}"

# Real base tables in Silver (views are skipped; masks on a table carry into its views)
silver_cols = spark.sql(f"""
  SELECT c.table_name, c.column_name, c.data_type
  FROM {CATALOG}.information_schema.columns c
  JOIN {CATALOG}.information_schema.tables t
    ON c.table_schema = t.table_schema AND c.table_name = t.table_name
  WHERE c.table_schema = 'silver' AND t.table_type <> 'VIEW'
""").collect()
silver_index = {(r.table_name, r.column_name): r.data_type for r in silver_cols}

# The team's classification: Controlled and Controlled Research columns
cls = spark.sql(f"""
  SELECT table_name, column_name, proposed_handling
  FROM {CATALOG}.rbac.data_classification
  WHERE proposed_handling IN ('Controlled', 'Controlled Research')
""").collect()

targets = {}   # (schema, table, column) -> (handling, dtype, source)
for r in cls:
    key = (r.table_name, r.column_name)
    if key in silver_index:
        targets[("silver", r.table_name, r.column_name)] = (r.proposed_handling, silver_index[key], "classified")

# Identifier columns the classification does not cover: treated as Controlled and flagged
for (table, col), dtype in silver_index.items():
    if col in ("subject_id", "hadm_id", "stay_id") and ("silver", table, col) not in targets:
        targets[("silver", table, col)] = ("Controlled", dtype, "inferred (not in classification)")

# Gold: mask the raw identifiers that sit in dimensions. Facts join on surrogate keys
# (patient_key, provider_key), so these masks do not break joins.
# hadm_id is NOT masked in Gold yet: it is the only link between a patient's rows within
# one admission across the fact tables. Team decision needed (see GOLD_HADM_NOTE).
GOLD_MASKS = [
    ("dim_patient", "subject_id", "Controlled"),
    ("dim_provider", "provider_id", "Controlled"),
]
gold_cols = spark.sql(f"""
  SELECT table_name, column_name, data_type
  FROM {CATALOG}.information_schema.columns
  WHERE table_schema = 'gold'
""").collect()
gold_index = {(r.table_name, r.column_name): r.data_type for r in gold_cols}
for table, col, handling in GOLD_MASKS:
    if (table, col) in gold_index:
        targets[("gold", table, col)] = (handling, gold_index[(table, col)], "gold decision")
    else:
        print(f"WARNING: gold.{table}.{col} not found, skipped")

GOLD_HADM_NOTE = [t for (t, c) in gold_index if c == "hadm_id"]
print("Gold tables still exposing hadm_id (needs team decision):", sorted(GOLD_HADM_NOTE))

print(f"{len(targets)} columns to process")

# COMMAND ----------

done, skipped, errors = [], [], []
for (schema, table, col), (handling, dtype, source) in sorted(targets.items()):
    try:
        fn = mask_fn(handling, dtype)
    except KeyError:
        skipped.append((schema, table, col, dtype))
        continue
    if MODE == "rollback":
        stmt = f"ALTER TABLE {CATALOG}.{schema}.{table} ALTER COLUMN {col} DROP MASK"
    else:
        stmt = f"ALTER TABLE {CATALOG}.{schema}.{table} ALTER COLUMN {col} SET MASK {fn}"
    if MODE == "dry_run":
        print(f"[{source}] {stmt}")
        continue
    try:
        spark.sql(stmt)
        done.append(stmt)
    except Exception as e:
        errors.append((stmt, str(e)[:200]))

print(f"\nMODE={MODE}  applied={len(done)}  skipped(unsupported type)={len(skipped)}  errors={len(errors)}")
for s in skipped:
    print("SKIPPED:", s)
for stmt, err in errors:
    print("ERROR:", stmt, "->", err)

# COMMAND ----------

# Evidence: masks now registered (metadata only)
display(spark.sql(f"SELECT table_schema, table_name, column_name, mask_name FROM {CATALOG}.information_schema.column_masks ORDER BY 1, 2, 3"))

# COMMAND ----------

