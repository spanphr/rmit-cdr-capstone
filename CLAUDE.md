# RMIT Clinical Data Repository Capstone

You are assisting with an RMIT WIL capstone building a Clinical Data Repository using MIMIC-IV and Databricks.

## Workspace

Live Databricks workspace project:

/Shared/RMIT_CDR_Capstone

Local working copy:

~/RMIT_CDR_Claude

Databricks CLI profile:

RMIT CDR Capstone

## Critical rules

1. Inspect before editing.
2. Do not delete existing notebooks or teammate work.
3. Do not modify cdr_mimiciv.silver unless explicitly instructed.
4. New controlled Silver work targets cdr_mimiciv.silver_v2.
5. Bronze is source-aligned and must not be altered.
6. Profile clinical tables before deciding transformations.
7. Do not blindly drop duplicates, fill NULLs, correct timestamps, or delete anomalous clinical records.
8. Preserve questionable clinical records and use documented DQ flags where appropriate.
9. Official MIMIC documentation is authoritative for field/table semantics.
10. Reusable clinical conformance belongs in Silver.
11. Analytical or research-specific features belong in Gold.
12. Do not DROP tables.
13. Do not delete Databricks workspace objects.
14. Do not overwrite live Databricks notebooks without explicit user approval.
15. Do not run workspace import-dir or other write/deployment commands without explicit approval.
16. Read-only Databricks CLI inspection is allowed.
17. Before deployment, show the exact git diff and explain what will change.

## Existing architecture

01_Bronze
02_Silver
03_Gold
04_Governance
05_Demo_Genie_Dashboards

Silver domains include:

- 01_Identity_Encounter
- 02_Diagnoses_Procedures
- 03_Labs_Observations
- 04_Medication_Orders
- 05_People_Reference

The controlled Silver implementation is cdr_mimiciv.silver_v2.

Patients and admissions contain important examples of the intended engineering approach.

## Method

For each clinical table:

Understand semantics
→ establish grain
→ inspect schema
→ profile keys
→ profile missingness
→ inspect categorical values
→ validate timestamps
→ validate relationships
→ investigate duplicates
→ decide transformations
→ transform
→ validate
→ publish
→ document

Do not manufacture complexity simply for demonstration purposes.

## Databricks access

You may use read-only Databricks CLI commands to inspect the workspace and project.

Before any Databricks write, import, overwrite, table modification, or deployment, ask for explicit approval.
