# Gold Inventory

Tables currently present in `cdr_mimiciv.gold` (read-only inspection, 2026-10-19): 24 managed tables.

"Query object" = a Databricks SQL Query object whose body was readable and saved here as `.sql`.
"Notebook cell" = a titled cell in `03_Gold/Dimensions.py` or `03_Gold/Facts.py` (workspace notebooks at `/Shared/RMIT_CDR_Capstone/03_Gold/`).

| Table | Readable SQL Query object | Build cell in Gold notebook |
|---|---|---|
| `dim_admission_type` | No | Yes |
| `dim_careunit` | Blank | Yes |
| `dim_date` | No | Yes |
| `dim_diagnosis` | No | Yes |
| `dim_icu_item` | Yes - `Dimensions/dim_icu_item.sql` | Yes |
| `dim_lab_item` | Yes - `Dimensions/dim_lab_item.sql` | Yes |
| `dim_location` | No | Yes |
| `dim_medication` | Yes - `Dimensions/dim_medication.sql` | Yes |
| `dim_microbiology_test` | No | Yes |
| `dim_patient` | No | Yes |
| `dim_procedure` | Yes - `Dimensions/dim_procedure.sql` | Yes |
| `dim_provider` | Yes - `Dimensions/dim_provider.sql` | Yes |
| `dim_service` | No | Yes |
| `fact_admission` | No | Yes |
| `fact_diagnosis` | No | Yes |
| `fact_icu_observation` | Blank | Yes |
| `fact_icu_stay` | Blank | Yes |
| `fact_lab` | Yes - `Facts/fact_lab.sql` | Yes |
| `fact_medication_administration` | Blank | Yes |
| `fact_microbiology` | No | Yes |
| `fact_prescription` | No | Yes |
| `fact_procedure` | Yes - `Facts/fact_procedure.sql` | Yes |
| `fact_service` | No | Yes |
| `fact_transfer` | No | Yes |

## Notes

- `Dimensions` and `Facts` under `/Shared/RMIT_CDR_Capstone/03_Gold` are notebooks, not folders of Query objects.
- The SQL Query objects were found in a teammate's personal workspace folders (`/Workspace/Users/s4153365.../Dimensions` and `.../Facts (1)`), not under `/Shared`.
- Four Query objects exist with blank bodies: `dim_careunit`, `fact_icu_stay`, `fact_icu_observation`, `fact_medication_administration`. No SQL was invented for them.
- Saved `dim_provider.sql` and `dim_medication.sql` contain `CREATE OR REPLACE VIEW cdr_mimiciv.silver.v_provider` / `v_medication`, which would write to the legacy `silver` schema if run. Review before executing.
- `fact_procedure.sql` begins with a `%sql` notebook magic line, saved verbatim.
- No Gold SQL was executed as part of this sync.
