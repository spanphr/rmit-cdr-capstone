# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS cdr_mimiciv;
# MAGIC CREATE SCHEMA IF NOT EXISTS cdr_mimiciv.bronze;
# MAGIC

# COMMAND ----------

import os

volume_path = "/Volumes/cdr_mimiciv/bronze/s3_mimic_iv/"
catalog = "cdr_mimiciv"
modules =["hosp","icu"]

def CreateTable(file_path,module,catalog):    
    table_name = file_name.split('.')[0]
    target_table = f"{catalog}.{'bronze'}.{table_name}"

    print(f"Reading: {file_name} -> Writing to: {target_table}")

    if not spark.catalog.tableExists(target_table):
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(file_path)
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
    else:
        print(f"Table {target_table} already exists, skipping.")

for mod in modules:
    folder_path = volume_path+mod
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv') or f.endswith('.csv.gz')]
    for file_name in csv_files:
        file_path = os.path.join(folder_path, file_name)
        CreateTable(file_path,mod,catalog)

print("Done")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_schema, table_name
# MAGIC FROM cdr_mimiciv.information_schema.tables
# MAGIC WHERE table_schema = 'bronze';