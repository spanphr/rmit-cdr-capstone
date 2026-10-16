# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Silver Layer — d_labitems
# MAGIC
# MAGIC ## Source
# MAGIC cdr_mimiciv.bronze.d_labitems
# MAGIC
# MAGIC ## Target
# MAGIC To be confirmed: cdr_mimiciv.silver_v2.d_labitems
# MAGIC
# MAGIC ## Purpose
# MAGIC Document and engineer the MIMIC-IV
# MAGIC laboratory item dictionary.
# MAGIC
# MAGIC ## Official source columns
# MAGIC - itemid
# MAGIC - label
# MAGIC - fluid
# MAGIC - category
# MAGIC
# MAGIC ## Engineering workflow
# MAGIC 1. Understand the source
# MAGIC 2. Profile data quality
# MAGIC 3. Define transformation rules
# MAGIC 4. Transform
# MAGIC 5. Validate
# MAGIC 6. Publish
# MAGIC
# MAGIC

# COMMAND ----------


from pyspark.sql import functions as F

# Project configuration
CATALOG = "cdr_mimiciv"
BRONZE = f"{CATALOG}.bronze"

# Load the Bronze source table
labitems_bronze = spark.table(
    f"{BRONZE}.d_labitems"
)

# Inspect its structure
print("=== BRONZE d_labitems ===")

print("Total rows:", labitems_bronze.count())
print("Total columns:", len(labitems_bronze.columns))
print("Column names:", labitems_bronze.columns)

# Inspect column datatypes
labitems_bronze.printSchema()

# Preview 10 records
display(labitems_bronze.limit(10))


# COMMAND ----------


# ==================================================
# STEP 3: BRONZE DATA QUALITY PROFILING
# Table: d_labitems
# ==================================================

# Check key uniqueness and missing values
profile = labitems_bronze.select(
    F.count("*").alias("total_rows"),

    F.countDistinct("itemid").alias("unique_itemids"),

    F.sum(F.when(F.col("itemid").isNull(), 1)
          .otherwise(0)).alias("null_itemids"),

    F.sum(F.when(F.col("label").isNull(), 1)
          .otherwise(0)).alias("null_labels"),

    F.sum(F.when(F.col("fluid").isNull(), 1)
          .otherwise(0)).alias("null_fluids"),

    F.sum(F.when(F.col("category").isNull(), 1)
          .otherwise(0)).alias("null_categories")
)

display(profile)


# COMMAND ----------


# ==================================================
# STEP 4: INVESTIGATE MISSING LABELS
# ==================================================

missing_labels = labitems_bronze.filter(
    F.col("label").isNull()
)

display(
    missing_labels.select(
        "itemid",
        "label",
        "fluid",
        "category"
    )
)


# COMMAND ----------


# STEP 5: TEXT QUALITY CHECK

text_columns = ["label", "fluid", "category"]

for c in text_columns:
    result = labitems_bronze.select(
        F.sum(
            F.when(F.trim(F.col(c)) == "", 1)
             .otherwise(0)
        ).alias("empty_strings"),

        F.sum(
            F.when(
                F.col(c) != F.trim(F.col(c)), 1
            ).otherwise(0)
        ).alias("whitespace_issues")
    )

    print(f"\nColumn: {c}")
    display(result)


# COMMAND ----------


# STEP 6: INSPECT TEXT QUALITY ISSUES

affected_labels = labitems_bronze.filter(
    F.col("label").isNotNull() &
    (
        (F.trim(F.col("label")) == "") |
        (F.col("label") != F.trim(F.col("label")))
    )
)

display(
    affected_labels.select(
        "itemid",
        F.concat(
            F.lit("["),
            F.col("label"),
            F.lit("]")
        ).alias("original_label"),
        F.length("label").alias("original_length"),
        F.trim("label").alias("trimmed_label"),
        "fluid",
        "category"
    )
)


# COMMAND ----------


# STEP 7: BUILD SILVER DATAFRAME

labitems_silver = (
    labitems_bronze
    .select(
        F.col("itemid"),

        F.when(
            F.trim(F.col("label")) == "",
            F.lit(None).cast("string")
        )
        .otherwise(F.trim(F.col("label")))
        .alias("label"),

        F.col("fluid"),
        F.col("category")
    )
)

# Preview transformed records
display(
    labitems_silver.filter(
        F.col("itemid").isin(51905, 52170)
    )
)


# COMMAND ----------


# STEP 8: VALIDATE SILVER TRANSFORMATION

validation = labitems_silver.select(
    F.count("*").alias("silver_rows"),

    F.countDistinct("itemid").alias("unique_itemids"),

    F.sum(
        F.when(F.col("itemid").isNull(), 1)
        .otherwise(0)
    ).alias("null_itemids"),

    F.sum(
        F.when(F.col("label").isNull(), 1)
        .otherwise(0)
    ).alias("null_labels"),

    F.sum(
        F.when(F.trim(F.col("label")) == "", 1)
        .otherwise(0)
    ).alias("empty_labels"),

    F.sum(
        F.when(
            F.col("label") != F.trim(F.col("label")), 1
        ).otherwise(0)
    ).alias("whitespace_issues")
)

display(validation)


# COMMAND ----------


# STEP 9: VERIFY SILVER_V2 DESTINATION

TARGET = "cdr_mimiciv.silver_v2.d_labitems"

# Check whether our target table already exists
print("Target already exists:",
      spark.catalog.tableExists(TARGET))

# Inspect teammate's Silver table structure
print("\nExisting silver_v2.patients schema:")
spark.table(
    "cdr_mimiciv.silver_v2.patients"
).printSchema()


# COMMAND ----------


# STEP 10: PUBLISH d_labitems TO SILVER_V2

TARGET = "cdr_mimiciv.silver_v2.d_labitems"

# Add Silver processing timestamp
labitems_final = labitems_silver.withColumn(
    "_silver_processed_at",
    F.current_timestamp()
)

# Create the table without overwriting existing work
if spark.catalog.tableExists(TARGET):
    print("Table already exists. No changes made.")
else:
    (
        labitems_final.write
        .format("delta")
        .mode("errorifexists")
        .saveAsTable(TARGET)
    )
    print(f"Successfully created: {TARGET}")

# Verify the published table
display(
    spark.table(TARGET).limit(10)
)


# COMMAND ----------


# STEP 11: FINAL PUBLISHED TABLE VALIDATION

published = spark.table(
    "cdr_mimiciv.silver_v2.d_labitems"
)

display(
    published.select(
        F.count("*").alias("total_rows"),
        F.countDistinct("itemid").alias("unique_itemids"),
        F.sum(
            F.when(F.col("label").isNull(), 1)
            .otherwise(0)
        ).alias("null_labels"),
        F.sum(
            F.when(F.trim(F.col("label")) == "", 1)
            .otherwise(0)
        ).alias("empty_labels")
    )
)
