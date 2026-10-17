catalog = "cdr_mimiciv"
schema = "gold"

# 1. Define overrides ONLY for columns that are sensitive/restricted
sensitive_overrides = {
    "dim_patient": {
        "gender": {"hipaa_identifiability": "phi_quasi_identifier", "hl7_confidentiality": "R_Restricted"},
        "anchor_age": {"hipaa_identifiability": "phi_quasi_identifier", "hl7_confidentiality": "R_Restricted"},
        "anchor_year": {"hipaa_identifiability": "phi_quasi_identifier", "hl7_confidentiality": "R_Restricted"},
        "anchor_year_group": {"hipaa_identifiability": "phi_quasi_identifier", "hl7_confidentiality": "R_Restricted"}
    },
    "fact_admission": {
        "insurance": {"hipaa_identifiability": "phi_quasi_identifier", "hl7_confidentiality": "R_Restricted"},
        "language": {"hipaa_identifiability": "phi_quasi_identifier", "hl7_confidentiality": "R_Restricted"},
        "marital_status": {"hipaa_identifiability": "phi_quasi_identifier", "hl7_confidentiality": "R_Restricted"},
        "race": {"hipaa_identifiability": "phi_quasi_identifier", "hl7_confidentiality": "R_Restricted"},
        "deathtime": {"hipaa_identifiability": "phi_quasi_identifier", "hl7_confidentiality": "V_Very_Restricted"}
    },
    "fact_diagnosis": {
        "primary_diagnosis_flag": {"hipaa_identifiability": "de_identified", "hl7_confidentiality": "R_Restricted"}
    }
}

print(f"Starting column-level tagging for {catalog}.{schema}...")

# 2. Get all tables in the gold schema
tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")

# Exclude internal Delta Live Tables event logs from tagging
table_names = [row['tableName'] for row in tables_df.collect() if not row['tableName'].startswith("event_log")]

total_tagged = 0

for table in table_names:
    # 3. Dynamically fetch all columns for the current table
    columns_df = spark.sql(f"SHOW COLUMNS IN {catalog}.{schema}.{table}")
    column_names = [row['col_name'] for row in columns_df.collect()]
    
    for col in column_names:
        # 4. Check if this column needs a sensitive override
        if table in sensitive_overrides and col in sensitive_overrides[table]:
            tags = sensitive_overrides[table][col]
        else:
            # 5. Default baseline for all standard keys, IDs, dates, and clinical measurements
            tags = {
                "hipaa_identifiability": "de_identified",
                "hl7_confidentiality": "N_Normal"
            }
        
        # 6. Construct and execute the ALTER TABLE command
        tag_list = [f"'{k}'='{v}'" for k, v in tags.items()]
        tag_string = ", ".join(tag_list)
        
        sql_command = f"ALTER TABLE {catalog}.{schema}.{table} ALTER COLUMN {col} SET TAGS ({tag_string});"
        
        try:
            spark.sql(sql_command)
            print(f"Tagged {table}.{col}")
            total_tagged += 1
        except Exception as e:
            print(f"Failed to tag {table}.{col}: {str(e)}")

print(f"\nSuccess! Tagged {total_tagged} columns across {len(table_names)} tables in the Gold layer.")