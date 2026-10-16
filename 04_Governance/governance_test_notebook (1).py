# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Governance test: run this as each identity (you, cdr_researcher_sp, cdr_clinician_sp, cdr_outsider_sp).
# It prints COUNTS only. No patient rows are shown, so output is safe to screenshot for the report.
# Run it BEFORE applying masks (baseline) and AFTER, then compare.

# COMMAND ----------

CAT = "cdr_mimiciv"

me = spark.sql("SELECT current_user()").first()[0]
print("Running as:", me)
for g in ["cdr_admins", "cdr_data_engineers", "cdr_data_stewards",
          "cdr_hospital_researchers", "cdr_hospital_clinicians", "cdr_hospital_governance"]:
    print(f"  member of {g}:", spark.sql(f"SELECT is_account_group_member('{g}')").first()[0])

# COMMAND ----------

def run(label, sql):
    try:
        row = spark.sql(sql).first()
        print(f"OK      {label}: {row[0]}")
    except Exception as e:
        msg = str(e).split("\n")[0][:140]
        print(f"DENIED  {label}: {msg}")

# Gold: rows visible, and whether the IDs are hidden (NULL count should equal row count once masked)
run("gold.dim_patient rows",                  f"SELECT COUNT(*) FROM {CAT}.gold.dim_patient")
run("gold.dim_patient subject_id NULL count",  f"SELECT COUNT(*) FROM {CAT}.gold.dim_patient WHERE subject_id IS NULL")
run("gold.dim_provider provider_id NULL count", f"SELECT COUNT(*) FROM {CAT}.gold.dim_provider WHERE provider_id IS NULL")
run("gold.fact_admission rows",               f"SELECT COUNT(*) FROM {CAT}.gold.fact_admission")
run("gold.fact_admission hadm_id NULL count",  f"SELECT COUNT(*) FROM {CAT}.gold.fact_admission WHERE hadm_id IS NULL")

# Silver: access and ID masking
run("silver.patients rows",                   f"SELECT COUNT(*) FROM {CAT}.silver.patients")
run("silver.patients subject_id NULL count",   f"SELECT COUNT(*) FROM {CAT}.silver.patients WHERE subject_id IS NULL")
run("silver.patients dod NULL count",          f"SELECT COUNT(*) FROM {CAT}.silver.patients WHERE dod IS NULL")
run("silver.admissions race NULL count",       f"SELECT COUNT(*) FROM {CAT}.silver.admissions WHERE race IS NULL")

# Bronze: should be denied for everyone except admins, engineers, stewards, governance
try:
    first_bronze = spark.sql(f"SHOW TABLES IN {CAT}.bronze").first()
    if first_bronze is None:
        print("DENIED  bronze: no tables visible")
    else:
        run("bronze first table count", f"SELECT COUNT(*) FROM {CAT}.bronze.{first_bronze['tableName']}")
except Exception as e:
    print("DENIED  bronze:", str(e).split("\n")[0][:140])

# Governance tables
run("rbac.roles rows",                        f"SELECT COUNT(*) FROM {CAT}.rbac.roles")

# COMMAND ----------

# Expected pattern AFTER masks are applied
# admin / engineer / steward / governance : all counts normal, NULL counts 0
# researcher : Gold readable, subject_id and provider_id NULL = row count, silver.patients.dod and race visible,
#              silver IDs NULL (only after Silver read is granted)
# clinician  : Gold readable, subject_id NULL, silver dod/race NULL
# outsider   : DENIED everywhere except what account users can still read (until that grant is revoked)

# COMMAND ----------

# COMMAND ----------
# What does this identity actually SEE in Gold? Counts only, no rows.
# "values visible" = number of non-NULL values this identity gets back.
# 0 on a table with rows means the column is hidden (masked) for this identity.

SHOW_ROWS = False   # keep False for the meeting; see note below

cols = spark.sql(f"""
  SELECT table_name, column_name
  FROM {CAT}.information_schema.columns
  WHERE table_schema = 'gold'
  ORDER BY table_name, ordinal_position
""").collect()

by_table = {}
for r in cols:
    by_table.setdefault(r.table_name, []).append(r.column_name)

print(f"Gold tables visible to this identity: {len(by_table)}\n")

for table, columns in by_table.items():
    try:
        exprs = ", ".join(f"COUNT(`{c}`) AS `{c}`" for c in columns)
        row = spark.sql(f"SELECT COUNT(*) AS total_rows, {exprs} FROM {CAT}.gold.{table}").first()
        total = row["total_rows"]
        print(f"{table}  ({total:,} rows)")
        for c in columns:
            n = row[c]
            status = "HIDDEN" if (total > 0 and n == 0) else "visible"
            print(f"   {status:8} {c}: {n:,} values")
        print()
    except Exception as e:
        print(f"DENIED  {table}: {str(e).split(chr(10))[0][:100]}\n")

if SHOW_ROWS:
    display(spark.table(f"{CAT}.gold.dim_patient").limit(5))