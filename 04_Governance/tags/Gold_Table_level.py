# Define the tagging configuration for your Gold tables
# Format: "table_name": {"type": "...", "domain": "...", "mimic_module": "..."}
table_tags_config = {
    # Demographics
    "dim_patient": {"type": "dimension", "domain": "demographics", "mimic_module": "hosp"},
    
    # Encounter & Patient Flow
    "dim_location": {"type": "dimension", "domain": "encounter", "mimic_module": "hosp"},
    "dim_admission_type": {"type": "dimension", "domain": "encounter", "mimic_module": "hosp"},
    "dim_provider": {"type": "dimension", "domain": "encounter", "mimic_module": "hosp"},
    "dim_service": {"type": "dimension", "domain": "encounter", "mimic_module": "hosp"},
    "dim_date": {"type": "dimension", "domain": "encounter", "mimic_module": "hosp"},
    "fact_admission": {"type": "fact", "domain": "encounter", "mimic_module": "hosp"},
    "fact_transfer": {"type": "fact", "domain": "encounter", "mimic_module": "hosp"},
    "fact_service": {"type": "fact", "domain": "encounter", "mimic_module": "hosp"},
    
    # Critical Care (ICU)
    "dim_careunit": {"type": "dimension", "domain": "critical_care", "mimic_module": "icu"},
    "dim_icu_item": {"type": "dimension", "domain": "critical_care", "mimic_module": "icu"},
    "fact_icu_stay": {"type": "fact", "domain": "critical_care", "mimic_module": "icu"},
    "fact_icu_observation": {"type": "fact", "domain": "critical_care", "mimic_module": "icu"},
    
    # Pharmacy & Medication
    "dim_medication": {"type": "dimension", "domain": "pharmacy", "mimic_module": "hosp"},
    "fact_prescription": {"type": "fact", "domain": "pharmacy", "mimic_module": "hosp"},
    "fact_medication_administration": {"type": "fact", "domain": "pharmacy", "mimic_module": "hosp"},
    
    # Diagnostics & Labs
    "dim_lab_item": {"type": "dimension", "domain": "diagnostics", "mimic_module": "hosp"},
    "dim_microbiology_test": {"type": "dimension", "domain": "diagnostics", "mimic_module": "hosp"},
    "fact_lab": {"type": "fact", "domain": "diagnostics", "mimic_module": "hosp"},
    "fact_microbiology": {"type": "fact", "domain": "diagnostics", "mimic_module": "hosp"},
    
    # Clinical Coding
    "dim_diagnosis": {"type": "dimension", "domain": "clinical_coding", "mimic_module": "hosp"},
    "dim_procedure": {"type": "dimension", "domain": "clinical_coding", "mimic_module": "hosp"},
    "fact_diagnosis": {"type": "fact", "domain": "clinical_coding", "mimic_module": "hosp"},
    "fact_procedure": {"type": "fact", "domain": "clinical_coding", "mimic_module": "hosp"}
}

catalog = "cdr_mimiciv"
schema = "gold"

# Loop through the config and execute the ALTER TABLE commands
for table_name, tags in table_tags_config.items():
    
    # Format the tags string (e.g., "'layer'='gold', 'type'='fact', ...")
    tag_string = f"'layer'='gold'" # All tables in this script get the gold layer tag
    for tag_key, tag_value in tags.items():
        tag_string += f", '{tag_key}'='{tag_value}'"
        
    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    # Construct the SQL statement
    sql_command = f"ALTER TABLE {full_table_name} SET TAGS ({tag_string});"
    
    try:
        # Execute the SQL using Spark
        spark.sql(sql_command)
        print(f" Successfully tagged {full_table_name}")
    except Exception as e:
        print(f" Failed to tag {full_table_name}: {str(e)}")

print("\nTable tagging complete.")