# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Configurable MIMIC-IV Ingestion
# MAGIC
# MAGIC ## Data flow
# MAGIC `AWS S3` → `Unity Catalog external volume` → `configurable Bronze ingestion` → `Delta Bronze tables`
# MAGIC
# MAGIC ## Purpose
# MAGIC One ingestion entry point for all 31 MIMIC-IV tables (22 `hosp`, 9 `icu`). The source is
# MAGIC chosen by configuration only: `SOURCE_MODE → SOURCES[SOURCE_MODE] → common ingestion logic`.
# MAGIC
# MAGIC | `SOURCE_MODE` | Source | Layout | Bronze target |
# MAGIC |---|---|---|---|
# MAGIC | `aws_volume` | AWS-backed external volume `/Volumes/cdr_mimiciv/bronze/s3_mimic_iv` (production / full data) | `hosp/` and `icu/` subfolders | `cdr_mimiciv.bronze` |
# MAGIC | `demo` | MIMIC-IV demo files uploaded to a volume | `hosp/` and `icu/` subfolders | `cdr_mimiciv.bronze_demo` |
# MAGIC | `upload` | Files uploaded through the Databricks UI | flat folder | `cdr_mimiciv.bronze_upload` |
# MAGIC
# MAGIC Only `aws_volume` is intended for the full dataset. The `demo` and `upload` modes are
# MAGIC supported by configuration but use their own schemas so they can never mix with the full data.
# MAGIC
# MAGIC ## Bronze principles
# MAGIC - Source-aligned: the Bronze schema contains only the 31 MIMIC source tables, with source
# MAGIC   columns and values unchanged. Empty CSV fields load as NULL (Spark CSV default).
# MAGIC - Create only missing tables. Existing tables are skipped.
# MAGIC - Tables are never deleted. Nothing is overwritten unless `REBUILD = True` **and** the table is named in `REBUILD_CONFIRM`.
# MAGIC - `.csv.gz` is preferred over `.csv`. A duplicate in the same folder is ignored and reported.
# MAGIC - Parsing is strict (`FAILFAST`): malformed input stops the load instead of silently changing values.
# MAGIC
# MAGIC ## Validation without repeated scans
# MAGIC Compressed files cannot be split, so every scan of a large file is expensive. Each table is
# MAGIC scanned twice, not three times:
# MAGIC 1. **Scan 1** — schema inference (`inferSchema`, the existing Bronze pattern).
# MAGIC 2. **Scan 2** — parse, write and count in the same job. The source row count is captured
# MAGIC    *while writing* with `DataFrame.observe()`, so no separate re-read is needed.
# MAGIC
# MAGIC The Bronze row count is then read from the Delta table (answered from file statistics, not by
# MAGIC re-reading the source) and compared with the observed source count.
# MAGIC
# MAGIC ## Operational evidence
# MAGIC One row per table per run is appended to `cdr_mimiciv.default.ingestion_run_log` (run id, user,
# MAGIC mode, source file and size, action, source and Bronze row counts, status). The Bronze schema is
# MAGIC never used for operational tables.
# MAGIC
# MAGIC ## How to run
# MAGIC 1. Set `SOURCE_MODE` (and optionally `TABLES`) in the configuration cell.
# MAGIC 2. Run with `DRY_RUN = True` (default). It lists sources and checks which Bronze tables exist, and writes nothing.
# MAGIC 3. Review the plan, then set `DRY_RUN = False` to create the missing tables.
# MAGIC
# MAGIC ## Known caveats
# MAGIC - Bronze tables created before this version were loaded with Spark CSV defaults. This version
# MAGIC   adds `multiLine` and RFC-4180 quote escaping; files without embedded newlines or doubled
# MAGIC   quotes parse identically.
# MAGIC - Inference chooses column types from the data, as before. Types therefore follow the file content.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

SOURCE_MODE = "aws_volume"          # "aws_volume" | "demo" | "upload"

SOURCES = {
    "aws_volume": {
        "root": "/Volumes/cdr_mimiciv/bronze/s3_mimic_iv",
        "layout": "module_subfolders",          # <root>/hosp/<table>.csv.gz, <root>/icu/<table>.csv.gz
        "target": "cdr_mimiciv.bronze",
    },
    "demo": {
        # set to the volume folder that contains hosp/ and icu/ once the demo files are uploaded
        "root": "/Volumes/cdr_mimiciv/bronze_demo/mimic_iv_demo",
        "layout": "module_subfolders",
        "target": "cdr_mimiciv.bronze_demo",
    },
    "upload": {
        "root": "/Volumes/cdr_mimiciv/default/raw",
        "layout": "flat",                       # <root>/<table>.csv.gz
        "target": "cdr_mimiciv.bronze_upload",
    },
}

TABLES = "all"                      # "all" or a list of registry table names
REBUILD = False                     # default: create only missing tables
REBUILD_CONFIRM = []                # a rebuild also requires the table names to be listed here
DRY_RUN = True                      # True: print the plan only, write nothing
VALIDATE_ROW_COUNTS = True          # compare observed source rows with Bronze rows for every table

FILE_PREFERENCE = ["csv.gz", "csv"]  # first match wins; later matches in the same folder are ignored
CSV_OPTIONS = {
    "header": "true",
    "inferSchema": "true",           # existing Bronze pattern
    "mode": "FAILFAST",              # malformed records stop the load
    "multiLine": "true",             # quoted fields may contain line breaks
    "escape": '"',                   # RFC 4180: a quote inside a quoted field is written as ""
}

RUN_LOG_TABLE = "cdr_mimiciv.default.ingestion_run_log"

# other places where copies of the same files are known to exist; reported only, never used
OTHER_SOURCE_LOCATIONS = [
    "/Volumes/cdr_mimiciv/default/raw",
    "/Volumes/cdr_mimiciv/bronze/files",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Table registry
# MAGIC The single list of the 31 expected MIMIC-IV tables and the module each belongs to.

# COMMAND ----------

TABLE_REGISTRY = {
    # hosp (22)
    "admissions": "hosp",
    "d_hcpcs": "hosp",
    "d_icd_diagnoses": "hosp",
    "d_icd_procedures": "hosp",
    "d_labitems": "hosp",
    "diagnoses_icd": "hosp",
    "drgcodes": "hosp",
    "emar": "hosp",
    "emar_detail": "hosp",
    "hcpcsevents": "hosp",
    "labevents": "hosp",
    "microbiologyevents": "hosp",
    "omr": "hosp",
    "patients": "hosp",
    "pharmacy": "hosp",
    "poe": "hosp",
    "poe_detail": "hosp",
    "prescriptions": "hosp",
    "procedures_icd": "hosp",
    "provider": "hosp",
    "services": "hosp",
    "transfers": "hosp",
    # icu (9)
    "caregiver": "icu",
    "chartevents": "icu",
    "d_items": "icu",
    "datetimeevents": "icu",
    "icustays": "icu",
    "ingredientevents": "icu",
    "inputevents": "icu",
    "outputevents": "icu",
    "procedureevents": "icu",
}

assert len(TABLE_REGISTRY) == 31, "registry must hold exactly the 31 MIMIC-IV tables"
assert sum(m == "hosp" for m in TABLE_REGISTRY.values()) == 22
assert sum(m == "icu" for m in TABLE_REGISTRY.values()) == 9

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configuration checks and source plan

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from pyspark.sql import Observation, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType, TimestampType
)

if SOURCE_MODE not in SOURCES:
    raise ValueError(f"SOURCE_MODE must be one of {list(SOURCES)}")

SOURCE = SOURCES[SOURCE_MODE]
SOURCE_ROOT = SOURCE["root"].rstrip("/")
LAYOUT = SOURCE["layout"]
TARGET = SOURCE["target"]

selected = list(TABLE_REGISTRY) if TABLES == "all" else list(TABLES)
unknown = [t for t in selected if t not in TABLE_REGISTRY]
if unknown:
    raise ValueError(f"Not in the registry: {unknown}")

if REBUILD and not REBUILD_CONFIRM:
    raise ValueError("REBUILD = True requires the table names to be listed in REBUILD_CONFIRM")
bad_confirm = [t for t in REBUILD_CONFIRM if t not in TABLE_REGISTRY]
if bad_confirm:
    raise ValueError(f"REBUILD_CONFIRM names not in the registry: {bad_confirm}")
rebuild_set = set(REBUILD_CONFIRM) if REBUILD else set()

print(f"SOURCE_MODE: {SOURCE_MODE}")
print(f"Source root: {SOURCE_ROOT}  (layout: {LAYOUT})")
print(f"Target     : {TARGET}")
print(f"Tables     : {len(selected)} selected | REBUILD={REBUILD} {sorted(rebuild_set)} | DRY_RUN={DRY_RUN}")

# COMMAND ----------

def list_directory(path):
    """name -> size for the files in a folder, or None if the folder cannot be listed."""
    try:
        return {f.name.rstrip("/"): f.size for f in dbutils.fs.ls(path)}
    except Exception:
        return None


def mb(n):
    return f"{n / 1e6:,.1f} MB"


def build_plan():
    listings = {}
    plan, warnings, other_copies = [], [], {}

    def listing_for(directory):
        if directory not in listings:
            listings[directory] = list_directory(directory)
            if listings[directory] is None:
                warnings.append(f"Source folder not found or not readable: {directory}")
        return listings[directory]

    other_listings = {loc: list_directory(loc) for loc in OTHER_SOURCE_LOCATIONS}

    for table in selected:
        module = TABLE_REGISTRY[table]
        directory = f"{SOURCE_ROOT}/{module}" if LAYOUT == "module_subfolders" else SOURCE_ROOT
        listing = listing_for(directory) or {}

        present = [f"{table}.{ext}" for ext in FILE_PREFERENCE if f"{table}.{ext}" in listing]
        source_file = f"{directory}/{present[0]}" if present else None
        source_bytes = listing[present[0]] if present else None

        for ignored in present[1:]:
            warnings.append(
                f"{table}: duplicate '{ignored}' ({mb(listing[ignored])}) in the source folder is "
                f"ignored; '{present[0]}' is used"
            )

        # copies elsewhere are never used; a same-named file of a different size is worth a warning
        for loc, other in other_listings.items():
            if not other or loc == directory:
                continue
            if present and present[0] in other and other[present[0]] != source_bytes:
                warnings.append(
                    f"{table}: a different-sized copy of '{present[0]}' exists at {loc} "
                    f"({mb(other[present[0]])} vs {mb(source_bytes)} used); not used"
                )
            elif any(f"{table}.{ext}" in other for ext in FILE_PREFERENCE):
                other_copies.setdefault(loc, []).append(table)

        exists = spark.catalog.tableExists(f"{TARGET}.{table}")
        if source_file is None:
            action = "NO_SOURCE"
        elif exists and table in rebuild_set:
            action = "REBUILD"
        elif exists:
            action = "SKIP_EXISTS"
        else:
            action = "CREATE"

        plan.append({
            "table": table, "module": module, "target_table": f"{TARGET}.{table}",
            "source_file": source_file, "source_bytes": source_bytes, "action": action,
        })

    for loc, tables in other_copies.items():
        warnings.append(f"{len(tables)} tables also have another copy or format at {loc} (not used)")

    # report files in the source folders that are not part of the registry
    for directory, listing in listings.items():
        if not listing:
            continue
        expected = {f"{t}.{ext}" for t in TABLE_REGISTRY for ext in FILE_PREFERENCE}
        extra = sorted(n for n in listing if n not in expected)
        if extra:
            warnings.append(f"Unregistered files in {directory} (ignored): {extra}")

    return plan, warnings


def print_plan(plan, warnings):
    print(f"{'table':<20}{'module':<7}{'action':<13}{'size':>12}  source file")
    for p in sorted(plan, key=lambda x: (x["action"], x["source_bytes"] or 0)):
        size = mb(p["source_bytes"]) if p["source_bytes"] is not None else "-"
        print(f"{p['table']:<20}{p['module']:<7}{p['action']:<13}{size:>12}  {p['source_file'] or '-'}")
    counts = {}
    for p in plan:
        counts[p["action"]] = counts.get(p["action"], 0) + 1
    print("\nSummary:", counts)
    create = [p["table"] for p in plan if p["action"] == "CREATE"]
    print(f"Would create ({len(create)}): {create}")
    if warnings:
        print("\nWarnings:")
        for w in warnings:
            print(" -", w)


plan, warnings = build_plan()
print_plan(plan, warnings)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ingestion
# MAGIC Runs only when `DRY_RUN = False`. A dry run stops after the plan above and writes nothing.

# COMMAND ----------

LOG_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("run_started_at", TimestampType()),
    StructField("run_user", StringType()),
    StructField("source_mode", StringType()),
    StructField("source_root", StringType()),
    StructField("target_schema", StringType()),
    StructField("table_name", StringType()),
    StructField("module", StringType()),
    StructField("source_file", StringType()),
    StructField("source_bytes", LongType()),
    StructField("action", StringType()),
    StructField("status", StringType()),
    StructField("source_rows", LongType()),
    StructField("bronze_rows", LongType()),
    StructField("rows_match", BooleanType()),
    StructField("message", StringType()),
    StructField("started_at", TimestampType()),
    StructField("completed_at", TimestampType()),
])


def now():
    return datetime.now(timezone.utc)


def ensure_log_table():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {RUN_LOG_TABLE} (
            run_id STRING, run_started_at TIMESTAMP, run_user STRING, source_mode STRING,
            source_root STRING, target_schema STRING, table_name STRING, module STRING,
            source_file STRING, source_bytes BIGINT, action STRING, status STRING,
            source_rows BIGINT, bronze_rows BIGINT, rows_match BOOLEAN, message STRING,
            started_at TIMESTAMP, completed_at TIMESTAMP
        ) USING DELTA
    """)


def write_log(run, p, status, started_at, source_rows=None, bronze_rows=None, message=None):
    rows_match = None if source_rows is None or bronze_rows is None else source_rows == bronze_rows
    row = (
        run["run_id"], run["started_at"], run["user"], SOURCE_MODE, SOURCE_ROOT, TARGET,
        p["table"], p["module"], p["source_file"], p["source_bytes"], p["action"], status,
        source_rows, bronze_rows, rows_match, message, started_at, now(),
    )
    (spark.createDataFrame([row], LOG_SCHEMA)
          .write.format("delta").mode("append").saveAsTable(RUN_LOG_TABLE))


def ingest_table(run, p):
    started_at = now()
    table = p["table"]
    try:
        # scan 1: schema inference
        df = spark.read.options(**CSV_OPTIONS).csv(p["source_file"])

        # scan 2: parse + write + count in one job; rows are counted as they are written
        source_count = Observation(f"source_rows_{table}")
        counted = df.observe(source_count, F.count(F.lit(1)).alias("rows"))

        writer = counted.write.format("delta")
        if p["action"] == "REBUILD":
            # the only overwrite path: needs REBUILD = True and the table named in REBUILD_CONFIRM
            if not (REBUILD and table in REBUILD_CONFIRM):
                raise RuntimeError(f"{table}: rebuild requested without REBUILD = True and confirmation")
            writer = writer.mode("overwrite").option("overwriteSchema", "true")
        else:
            writer = writer.mode("errorifexists")     # never replaces an existing table
        writer.saveAsTable(p["target_table"])

        source_rows = source_count.get["rows"]
        bronze_rows = spark.table(p["target_table"]).count()   # Delta statistics, no source re-read

        if VALIDATE_ROW_COUNTS and source_rows != bronze_rows:
            write_log(run, p, "VALIDATION_FAILED", started_at, source_rows, bronze_rows,
                      "source and Bronze row counts differ")
            return "VALIDATION_FAILED"

        write_log(run, p, "SUCCESS", started_at, source_rows, bronze_rows)
        print(f"  {table}: {source_rows:,} source rows -> {bronze_rows:,} Bronze rows")
        return "SUCCESS"
    except Exception as e:
        write_log(run, p, "FAILED", started_at, message=str(e)[:2000])
        print(f"  {table}: FAILED - {str(e)[:300]}")
        return "FAILED"


if DRY_RUN:
    print("DRY RUN - nothing was written, no table or schema was created.")
else:
    target_catalog, target_schema = TARGET.split(".")
    if spark.sql(f"SHOW SCHEMAS IN {target_catalog} LIKE '{target_schema}'").count() == 0:
        spark.sql(f"CREATE SCHEMA {TARGET}")
        print(f"Created schema {TARGET}")
    ensure_log_table()

    run = {
        "run_id": str(uuid.uuid4()),
        "started_at": now(),
        "user": spark.sql("SELECT current_user()").first()[0],
    }
    print(f"Run {run['run_id']} by {run['user']}")

    results = {}
    # smallest sources first, so problems surface before the large files are read
    for p in sorted(plan, key=lambda x: x["source_bytes"] or 0):
        if p["action"] in ("CREATE", "REBUILD"):
            print(f"{p['action']}: {p['target_table']}  <-  {p['source_file']}")
            results[p["table"]] = ingest_table(run, p)
        elif p["action"] == "SKIP_EXISTS":
            write_log(run, p, "SKIPPED", now(), message="table already exists; not modified")
            results[p["table"]] = "SKIPPED"
        else:  # NO_SOURCE
            write_log(run, p, "FAILED", now(), message="no source file found for this table")
            results[p["table"]] = "FAILED"

    # final coverage check against the registry
    present = [t for t in TABLE_REGISTRY if spark.catalog.tableExists(f"{TARGET}.{t}")]
    missing = [t for t in TABLE_REGISTRY if t not in present]
    print(f"\nRegistry coverage in {TARGET}: {len(present)}/{len(TABLE_REGISTRY)}")
    if missing:
        print("Missing:", missing)

    failed = {t: s for t, s in results.items() if s in ("FAILED", "VALIDATION_FAILED")}
    if failed:
        raise RuntimeError(f"Ingestion finished with problems: {failed}")
