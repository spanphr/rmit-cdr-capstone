# MIMIC-IV Unity Catalog Tagging Framework

This directory contains the PySpark scripts required to implement the automated metadata tagging taxonomy for the MIMIC-IV clinical data warehouse within Databricks Unity Catalog.

These scripts operationalize a **"Default + Override"** tagging pattern, ensuring that all tables and columns are programmatically classified according to the Medallion architecture, the OMOP Common Data Model, and federal healthcare privacy standards (HIPAA and HL7). This automated tagging directly drives the **Attribute-Based Access Control (ABAC)** and dynamic data masking policies across the platform.

---

## Tag Taxonomy Overview

The scripts apply **six governed tags** across three architectural levels:

### 1. Schema-Level Tags (Architectural Boundary)

| Tag | Description |
|---|---|
| `layer` | Identifies the tier within the Medallion architecture (`bronze`, `silver`, `gold`). |

### 2. Table-Level Tags (Discovery & Interoperability)

| Tag | Description |
|---|---|
| `type` | Specifies the data modeling pattern (`fact`, `dimension`, `view`). |
| `mimic_module` | Maps the derived data back to the MIMIC-IV source module (`hosp`, `icu`). |
| `domain` | Categorizes the clinical subject area using the standardized OMOP Common Data Model vocabulary (e.g., `person`, `visit`, `measurement`, `drug`). |

### 3. Column-Level Tags (Security & Compliance)

| Tag | Description |
|---|---|
| `hipaa_identifiability` | Classifies data elements based on the HIPAA Privacy Rule Safe Harbor standard to drive automated data masking (`de_identified`, `phi_quasi_identifier`, `phi_direct_identifier`). |
| `hl7_confidentiality` | Enforces baseline access protection levels based on the HL7 Healthcare Privacy and Security Classification System (`N_Normal`, `R_Restricted`, `V_Very_Restricted`). |

---

## Included Scripts

### `Schema_level.py`

A foundational PySpark script that establishes the baseline architectural tags on the Unity Catalog schemas. It statically applies the `layer` tags to the `bronze`, `silver`, and `gold` schemas.

### `Bronze_Table_level.py`

A PySpark script that iterates through the raw ingestion layer, applying structural lineage tags (`layer = bronze` and `mimic_module`) to mirror the source systems without applying analytical domain modeling.

### `Silver_Table_level.py`

A PySpark script that applies operational tagging to the cleansed layer. It establishes `type` tags to differentiate between materialized tables and staging views (`v_*`), alongside `layer` and `mimic_module` lineage tracking.

### `Gold_Table_level.py`

A PySpark script that loops through a central configuration dictionary to dynamically apply table-level tags for the presentation layer. It maps the 21 Gold-layer star schema tables to their respective OMOP domains, establishing the ontology required for federated clinical research and intuitive catalog discovery.

### `Gold_Column_level.py`

The core security automation script. It utilizes Databricks `information_schema` introspection to execute a scalable **"Default + Override"** tagging methodology across the Gold layer:

- **Default:** Automatically tags all discovered clinical facts, lab measurements, and surrogate keys with `de_identified` and `N_Normal`.
- **Override:** Consults a strict configuration dictionary to elevate specific sensitive fields (e.g., mapping demographic dimensions to `phi_quasi_identifier` and highly restricted markers like `deathtime` to `V_Very_Restricted`).

---

## Execution Instructions

To deploy the tagging framework to a new Databricks workspace:

1. **Prerequisites:** Ensure you have `USE CATALOG` and `MODIFY` privileges on the target `cdr_mimiciv` catalog.
2. **Cluster Requirements:** Scripts can be run on any standard Databricks interactive cluster utilizing PySpark (Databricks Runtime 13.3 LTS or higher recommended).
3. **Execution Order:**
   1. Execute `Schema_level.py` to establish catalog boundaries.
   2. Execute the table-level scripts (`Bronze_Table_level.py`, `Silver_Table_level.py`, `Gold_Table_level.py`) to categorize all objects.
   3. Execute `Gold_Column_level.py` to finalize the security posture.

---

## Security & ABAC Integration

These tags are not strictly for documentation; they **actively restrict access**. Once these scripts run, Unity Catalog evaluates these tags at query time.
