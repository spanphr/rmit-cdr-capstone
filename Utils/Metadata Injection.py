# Databricks notebook source
from pyspark.sql import functions as F

# ============================================================
# Utility Functions to Add Metadata (Comments & Tags) to 
# Unity Catalog Objects: Catalogs, Schemas, Tables, and Columns
# ============================================================

def add_catalog_comment(catalog: str, comment: str):
    """Add a descriptive comment to a catalog."""
    spark.sql(f"COMMENT ON CATALOG `{catalog}` IS '{comment}'")
    print(f"✅ Comment set on catalog '{catalog}'")


def add_schema_comment(catalog: str, schema: str, comment: str):
    """Add a descriptive comment to a schema."""
    spark.sql(f"COMMENT ON SCHEMA `{catalog}`.`{schema}` IS '{comment}'")
    print(f"✅ Comment set on schema '{catalog}.{schema}'")


def add_table_comment(catalog: str, schema: str, table: str, comment: str):
    """Add a descriptive comment to a table (also works for views)."""
    spark.sql(f"COMMENT ON TABLE `{catalog}`.`{schema}`.`{table}` IS '{comment}'")
    print(f"✅ Comment set on table '{catalog}.{schema}.{table}'")


def add_column_comment(catalog: str, schema: str, table: str, column: str, comment: str):
    """Add a descriptive comment to a specific column."""
    spark.sql(
        f"ALTER TABLE `{catalog}`.`{schema}`.`{table}` "
        f"ALTER COLUMN `{column}` COMMENT '{comment}'"
    )
    print(f"✅ Comment set on column '{catalog}.{schema}.{table}.{column}'")


def add_column_comments_bulk(catalog: str, schema: str, table: str, column_comments: dict):
    """Add comments to multiple columns of a table at once.
    
    Args:
        column_comments: dict mapping column_name -> comment string
            e.g. {'subject_id': 'Unique patient identifier', 'hadm_id': 'Hospital admission ID'}
    """
    for col_name, comment in column_comments.items():
        add_column_comment(catalog, schema, table, col_name, comment)


# --- Tag Functions ---

def add_catalog_tag(catalog: str, key: str, value: str):
    """Set a key-value tag on a catalog."""
    spark.sql(f"SET TAG ON CATALOG `{catalog}` `{key}` = `{value}`")
    print(f"🏷️  Tag '{key}={value}' set on catalog '{catalog}'")


def add_schema_tag(catalog: str, schema: str, key: str, value: str):
    """Set a key-value tag on a schema."""
    spark.sql(f"SET TAG ON SCHEMA `{catalog}`.`{schema}` `{key}` = `{value}`")
    print(f"🏷️  Tag '{key}={value}' set on schema '{catalog}.{schema}'")


def add_table_tag(catalog: str, schema: str, table: str, key: str, value: str):
    """Set a key-value tag on a table."""
    spark.sql(f"SET TAG ON TABLE `{catalog}`.`{schema}`.`{table}` `{key}` = `{value}`")
    print(f"🏷️  Tag '{key}={value}' set on table '{catalog}.{schema}.{table}'")


def add_column_tag(catalog: str, schema: str, table: str, column: str, key: str, value: str):
    """Set a key-value tag on a specific column."""
    spark.sql(
        f"SET TAG ON COLUMN `{catalog}`.`{schema}`.`{table}`.`{column}` `{key}` = `{value}`"
    )
    print(f"🏷️  Tag '{key}={value}' set on column '{catalog}.{schema}.{table}.{column}'")


def add_table_tags_bulk(catalog: str, schema: str, table: str, tags: dict):
    """Set multiple key-value tags on a table at once.
    
    Args:
        tags: dict mapping tag_key -> tag_value
            e.g. {'quality_tier': 'silver', 'team': 'data-engineering'}
    """
    for key, value in tags.items():
        add_table_tag(catalog, schema, table, key, value)


def add_column_tags_bulk(catalog: str, schema: str, table: str, column: str, tags: dict):
    """Set multiple key-value tags on a single column.
    
    Args:
        tags: dict mapping tag_key -> tag_value
            e.g. {'pii': 'true', 'pii_type': 'email', 'sensitivity': 'high'}
    """
    for key, value in tags.items():
        add_column_tag(catalog, schema, table, column, key, value)


# --- Remove Metadata Functions ---

def remove_comment(object_type: str, full_name: str):
    """Remove a comment from any UC object by setting it to NULL.
    
    Args:
        object_type: One of 'CATALOG', 'SCHEMA', 'TABLE'
        full_name: Fully qualified name (e.g. 'catalog.schema.table')
    """
    spark.sql(f"COMMENT ON {object_type} {full_name} IS NULL")
    print(f"🗑️  Comment removed from {object_type.lower()} '{full_name}'")


def remove_table_tag(catalog: str, schema: str, table: str, key: str):
    """Remove a tag from a table."""
    spark.sql(f"UNSET TAG ON TABLE `{catalog}`.`{schema}`.`{table}` `{key}`")
    print(f"🗑️  Tag '{key}' removed from table '{catalog}.{schema}.{table}'")


def remove_column_tag(catalog: str, schema: str, table: str, column: str, key: str):
    """Remove a tag from a column."""
    spark.sql(
        f"UNSET TAG ON COLUMN `{catalog}`.`{schema}`.`{table}`.`{column}` `{key}`"
    )
    print(f"🗑️  Tag '{key}' removed from column '{catalog}.{schema}.{table}.{column}'")


# COMMAND ----------

import json

CATALOG_NAME = "cdr_mimiciv"  
SCHEMA_NAME = "bronze"       

MIMIC_HOSP_METADATA = '/Workspace/Shared/RMIT_CDR_Capstone/Utils/comments.json'

def apply_mimic_metadata(catalog: str, schema: str, json_filepath: str):
    """Iterates through the metadata dictionary and applies table and column comments."""
    print(f"Loading metadata from {json_filepath}...")
    
    # Load the JSON file
    with open(json_filepath, 'r') as f:
        metadata_dict = json.load(f)
        
    print(f"Starting metadata application for {catalog}.{schema}...\n")
    for table_name, table_data in metadata_dict.items():
        # 1. Escape single quotes for the table comment to avoid Spark SQL syntax errors
        raw_table_comment = table_data.get("table_comment", "")
        safe_table_comment = raw_table_comment.replace("'", "''")
        
        # Apply Table Comment
        if safe_table_comment:
            add_table_comment(catalog, schema, table_name, safe_table_comment)
            
        # 2. Escape single quotes for all column comments
        raw_columns = table_data.get("columns", {})
        safe_columns = {
            col_name: col_comment.replace("'", "''") 
            for col_name, col_comment in raw_columns.items()
        }
        
        # Apply Column Comments
        if safe_columns:
            add_column_comments_bulk(catalog, schema, table_name, safe_columns)
            
        print("-" * 50)
        
    print("\n🎉 All metadata successfully applied!")

# Run the execution
apply_mimic_metadata(CATALOG_NAME, SCHEMA_NAME, MIMIC_HOSP_METADATA)