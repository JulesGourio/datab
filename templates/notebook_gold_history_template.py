# Databricks notebook source
# MAGIC %md
# MAGIC # GOLD <OBJECT> HISTORY
# MAGIC
# MAGIC **Description:**
# MAGIC Monthly history of <SAP object>, as SAP knew it at the start of each month: one row per snapshot date x
# MAGIC <business key>. Built from the landing zone `*_stack` tables (one full SAP extraction per `extraction_timestamp`),
# MAGIC because Bronze `_latest` tables only keep the last extraction.
# MAGIC
# MAGIC **Highlighted complexities:**
# MAGIC - Snapshot K (first day of a month) uses, for each source, the **latest extraction strictly before K**.
# MAGIC - A stack can repeat a row inside one extraction: `drop_duplicates_by_order` keeps the latest ingested one.
# MAGIC - Partial `DELTA` extractions (when the stack has `file_mode`) are skipped: only `FULL` extractions are read.
# MAGIC - The stacks are never purged: `rebuild_mode = full` can always rebuild the whole history; `incremental` only
# MAGIC   adds the missing snapshot dates (`replace_where` on the `snapshot_date` partition).
# MAGIC - A debug run (`debug_plant` or `snapshot_from_date`) writes to `<table>_debug`, never to the full table.
# MAGIC
# MAGIC **Intended Pipeline**
# MAGIC - M_2_<Domain>_<Object>_History_Data_Asset (monthly, after the 1st of the month)
# MAGIC
# MAGIC **Inputs Data**
# MAGIC - {REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6.<table>_stack
# MAGIC
# MAGIC **Output Tables (Pipeline)**
# MAGIC - {PIPELINE_WRITE_ENV}_gold.<domain>.<object>_history (partitioned by `snapshot_date`)
# MAGIC - {PIPELINE_WRITE_ENV}_gold.<domain>.<object>_history_exposed (view)

# COMMAND ----------

# MAGIC %pip install "great_expectations==0.18.22"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Technical debt
# MAGIC - Lab runs only, to remove before the move to Bitbucket: the `%pip install` cell and its restart, `sys.path.insert(...)`
# MAGIC   and the `persist` / `cache` patch (serverless) in the standard imports cell.
# MAGIC - `sap_number()`, `month_starts()`, `build_snapshot_mapping()`, `list_partition_timestamps()` are shared with the
# MAGIC   other history notebooks: to move to `leap_utils`.

# COMMAND ----------
# MAGIC # Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Standard Package Imports

# COMMAND ----------

import datetime
import sys
import time

from pyspark.sql import functions as f
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

# Lab test runs only: leap_utils is read from a workspace folder. Remove before the move to Bitbucket,
# where it is declared as a library of the job task.
sys.path.insert(0, "/Workspace/Users/jules.gourio.external@latecoere.aero/dev_ict_FI_invoice")

# Lab test runs on serverless compute only: persist / cache are not supported there and Great Expectations calls them.
# Remove before the move to Bitbucket (job clusters support them).
try:
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

    for method_name in ("persist", "cache", "unpersist"):
        setattr(ConnectDataFrame, method_name, lambda self, *args, **kwargs: self)
except ImportError:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config LEAP Function Imports

# COMMAND ----------

from leap_utils.data_asset import table_utils
from leap_utils.common import logger
from leap_utils.common.validation import gx_validation
from great_expectations.dataset import SparkDFDataset

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Widgets

# COMMAND ----------

# Values already typed in the widgets are kept when this cell is run again (removeAll resets them).
WIDGET_NAMES = [
    "pipeline_write_env",
    "pipeline_read_env",
    "reference_read_env",
    "by_pass_quality_checks",
    "lab_target_schema",
    "debug",
    "log_level",
    "snapshot_from_date",
    "debug_plant",
    "rebuild_mode",
]
PREVIOUS_WIDGET_VALUES = {}
for widget_name in WIDGET_NAMES:
    try:
        PREVIOUS_WIDGET_VALUES[widget_name] = dbutils.widgets.get(widget_name)
    except Exception:
        pass

dbutils.widgets.removeAll()  # must be the first widget call

dbutils.widgets.text("pipeline_write_env", PREVIOUS_WIDGET_VALUES.get("pipeline_write_env", "dev"))
PIPELINE_WRITE_ENV = dbutils.widgets.get("pipeline_write_env")

dbutils.widgets.text("pipeline_read_env", PREVIOUS_WIDGET_VALUES.get("pipeline_read_env", "dev"))
PIPELINE_READ_ENV = dbutils.widgets.get("pipeline_read_env")

dbutils.widgets.text("reference_read_env", PREVIOUS_WIDGET_VALUES.get("reference_read_env", "prod"))
REFERENCE_READ_ENV = dbutils.widgets.get("reference_read_env")

dbutils.widgets.text("by_pass_quality_checks", PREVIOUS_WIDGET_VALUES.get("by_pass_quality_checks", "false"))
BYPASS_QUALITY_CHECKS = dbutils.widgets.get("by_pass_quality_checks").lower() == "true"

# Lab test runs only: "<catalog>.<schema>" of the lab redirects every table this notebook writes.
# Always empty in jobs; never commit a non-empty default (hardcoded env = Convention Checker BLOCKER).
dbutils.widgets.text("lab_target_schema", PREVIOUS_WIDGET_VALUES.get("lab_target_schema", ""))
LAB_TARGET_SCHEMA = dbutils.widgets.get("lab_target_schema").strip()

# All widgets are created in this cell so that none disappears when it is run again.
dbutils.widgets.text("debug", PREVIOUS_WIDGET_VALUES.get("debug", "False"))

dbutils.widgets.dropdown(
    "log_level",
    defaultValue=PREVIOUS_WIDGET_VALUES.get("log_level", "info"),
    choices=["debug", "info", "warning", "error", "critical"],
)

dbutils.widgets.text("snapshot_from_date", PREVIOUS_WIDGET_VALUES.get("snapshot_from_date", ""))

dbutils.widgets.text("debug_plant", PREVIOUS_WIDGET_VALUES.get("debug_plant", ""))

dbutils.widgets.dropdown(
    "rebuild_mode",
    defaultValue=PREVIOUS_WIDGET_VALUES.get("rebuild_mode", "full"),
    choices=["full", "incremental"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Debug Boolean
# MAGIC `snapshot_from_date` (yyyy-MM-dd) and `debug_plant` shorten lab runs; both are empty in jobs.
# MAGIC `rebuild_mode`: `full` rewrites every snapshot; `incremental` only adds the snapshot dates missing from the
# MAGIC output table (a stack extraction never changes, so a past snapshot is never recomputed).

# COMMAND ----------

DEBUG = dbutils.widgets.get("debug").lower() == "true"

LOG_LEVEL = dbutils.widgets.get("log_level")

SNAPSHOT_FROM_DATE = dbutils.widgets.get("snapshot_from_date").strip()

DEBUG_PLANT = dbutils.widgets.get("debug_plant").strip()

REBUILD_MODE = dbutils.widgets.get("rebuild_mode")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config logger

# COMMAND ----------

log = logger.setup_applevel_logger(log_level=logger.LOGGER_MAPPING[LOG_LEVEL])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Output Table
# MAGIC Needed before the snapshot calendar: the incremental mode reads the snapshot dates already stored.

# COMMAND ----------

if LAB_TARGET_SCHEMA:
    CATALOG, SCHEMA = LAB_TARGET_SCHEMA.split(".")
else:
    CATALOG = f"{PIPELINE_WRITE_ENV}_gold"
    SCHEMA = "<domain>"
# A debug run (debug_plant or snapshot_from_date) holds a subset of the data: it writes to a separate table,
# so that it can never overwrite the full history in the default table.
HISTORY_TABLE = "<object>_history"  # LEAP naming: the _history suffix goes at the end
DEBUG_RUN = bool(DEBUG_PLANT or SNAPSHOT_FROM_DATE)
DESTINATION_TABLE = f"{HISTORY_TABLE}_debug" if DEBUG_RUN else HISTORY_TABLE
DESTINATION_TABLE_FULL_PATH = f"{CATALOG}.{SCHEMA}.{DESTINATION_TABLE}"

INCREMENTAL_LOAD = REBUILD_MODE == "incremental" and table_utils.UnityCatalogCheckTableExists(
    DESTINATION_TABLE_FULL_PATH
)
if INCREMENTAL_LOAD and DEBUG_PLANT:
    raise ValueError("debug_plant restricts the data: it cannot be combined with rebuild_mode = incremental")
log.info(f"Rebuild mode: {REBUILD_MODE}, incremental load: {INCREMENTAL_LOAD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Business Constants

# COMMAND ----------

LANDING_ZONE_SCHEMA = f"{REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6"
SOURCE_STACK = f"{LANDING_ZONE_SCHEMA}.<table>_stack"
LATEST_ROW_ORDER = ["_ingestion_timestamp", "_stack_row_id"]  # a source extraction can repeat a row
FULL_EXTRACTION = "FULL"  # some stacks also contain partial DELTA extractions
KEY_COLUMNS = ["<key_1>", "<key_2>"]  # business key of one row inside one extraction

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Helper Functions
# MAGIC - `sap_number`: SAP text number -> double. SAP writes negatives with a trailing minus (`1.000-`) and pads with
# MAGIC   spaces; a bare cast would return NULL or fail.
# MAGIC - `sap_flag`: SAP indicator `'X'` -> `True`, anything else -> `False`.
# MAGIC - `month_starts` / `build_snapshot_mapping`: the snapshot calendar (first day of each month) and, for each
# MAGIC   snapshot date, the latest extraction strictly before it.

# COMMAND ----------

def sap_number(column_name):
    raw = f.trim(f.col(column_name))
    value = f.expr(f"try_cast(regexp_replace(trim({column_name}), '-$', '') AS DOUBLE)")
    return f.when(raw.endswith("-"), -value).otherwise(value)


def sap_flag(column_name):
    return f.coalesce(f.trim(f.col(column_name)) == "X", f.lit(False))


def sap_date(column_name):
    return f.try_to_date(f.trim(f.col(column_name)), "yyyyMMdd")


def next_month(day):
    return datetime.date(day.year + day.month // 12, day.month % 12 + 1, 1)


def month_starts(after_day, until_day):
    """First days of month strictly after `after_day`, up to `until_day` included."""
    current = next_month(after_day)
    result = []
    while current <= until_day:
        result.append(current)
        current = next_month(current)
    return result


def build_snapshot_mapping(extraction_timestamps, snapshot_dates, backdate_first=False):
    """(snapshot_date, extraction_timestamp, _is_backdated): latest extraction strictly before the snapshot date.
    With backdate_first, snapshot dates older than the first extraction use the first extraction."""
    rows = []
    for snapshot_date in snapshot_dates:
        eligible = [ts for ts in extraction_timestamps if ts.date() < snapshot_date]
        if eligible:
            rows.append((snapshot_date, max(eligible), False))
        elif backdate_first and extraction_timestamps:
            rows.append((snapshot_date, min(extraction_timestamps), True))
    return spark.createDataFrame(
        rows, "snapshot_date date, extraction_timestamp timestamp, _is_backdated boolean"
    )


def list_partition_timestamps(table_name):
    """Extraction timestamps of a stack, read from the partition metadata (no data scan)."""
    rows = (
        spark.sql(f"SHOW PARTITIONS {table_name}")
        .select(f.expr("try_cast(extraction_timestamp AS TIMESTAMP)").alias("ts"))
        .collect()
    )
    return sorted(r["ts"] for r in rows if r["ts"] is not None)

# COMMAND ----------

# MAGIC %md
# COMMAND ----------

# MAGIC %md
# MAGIC # Inputs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import LEAP tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Landing zone stacks
# MAGIC Full SAP extractions appended at each `extraction_timestamp` (partition column).

# COMMAND ----------

df_source_raw = spark.read.table(SOURCE_STACK)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extraction calendar
# MAGIC Read from the partition metadata (no data scan). If the stack has partial `DELTA` extractions, replace it by the
# MAGIC distinct `extraction_timestamp` of the rows with `file_mode = FULL_EXTRACTION`.

# COMMAND ----------

source_timestamps = list_partition_timestamps(SOURCE_STACK)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep1 - Snapshot calendar
# MAGIC One snapshot on the first day of each month after the first extraction. In incremental mode, only the snapshot
# MAGIC dates missing from the output table are computed.

# COMMAND ----------

first_snapshot_after = source_timestamps[0].date()
if SNAPSHOT_FROM_DATE:
    first_snapshot_after = max(
        first_snapshot_after,
        datetime.date.fromisoformat(SNAPSHOT_FROM_DATE) - datetime.timedelta(days=1),
    )
SNAPSHOT_DATES = month_starts(first_snapshot_after, datetime.date.today())
if INCREMENTAL_LOAD:
    stored_snapshot_dates = {
        r["snapshot_date"] for r in spark.read.table(DESTINATION_TABLE_FULL_PATH).select("snapshot_date").distinct().collect()
    }
    SNAPSHOT_DATES = [d for d in SNAPSHOT_DATES if d not in stored_snapshot_dates]
    if not SNAPSHOT_DATES:
        log.info("No new snapshot date to load")
        dbutils.notebook.exit("No new snapshot date to load")
log.info(f"{len(SNAPSHOT_DATES)} snapshot dates, from {SNAPSHOT_DATES[0]} to {SNAPSHOT_DATES[-1]}")

df_map_source = build_snapshot_mapping(source_timestamps, SNAPSHOT_DATES)

# COMMAND ----------

def select_extractions(df_raw, df_map):
    """Keep only the extractions used by the calendar (partition pruning), tagged with their snapshot date(s)."""
    used_timestamps = [r["extraction_timestamp"] for r in df_map.select("extraction_timestamp").distinct().collect()]
    return df_raw.filter(f.col("extraction_timestamp").isin(used_timestamps)).join(
        f.broadcast(df_map), ["extraction_timestamp"], how="inner"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep2 - <table> at each snapshot date
# MAGIC Select and rename the needed fields (SAP text -> types with `sap_number` / `sap_flag` / `sap_date`), strip
# MAGIC leading zeros of the identifiers, keep one row per key and snapshot date.

# COMMAND ----------

SOURCE_COLUMNS = [
    "snapshot_date",
    f.col("extraction_timestamp").alias("_extraction_timestamp"),
    "_is_backdated",
    f.trim("<sap_field>").alias("<key_1>"),
    f.trim("<sap_field>").alias("<key_2>"),
    sap_number("<sap_quantity_field>").alias("<quantity>"),
    sap_date("<sap_date_field>").alias("<something>_date"),
    f.col("ingestion_timestamp").alias("_ingestion_timestamp"),
    f.col("stack_row_id").alias("_stack_row_id"),
]

df_source_prep = select_extractions(df_source_raw, df_map_source).select(*SOURCE_COLUMNS)

if DEBUG_PLANT:
    df_source_prep = df_source_prep.filter(f.col("plant") == DEBUG_PLANT)  # adapt if the table has no plant

df_source_prep = table_utils.remove_leading_zeros(df=df_source_prep, column_names=["<key_1>"])
df_source_prep = table_utils.drop_duplicates_by_order(
    df=df_source_prep, columns=["snapshot_date"] + KEY_COLUMNS, order_col=LATEST_ROW_ORDER, keep="last"
).drop(*LATEST_ROW_ORDER)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 1 - Create ID column and put it first
# MAGIC The snapshot date is part of the key: one row per business key and snapshot date.

# COMMAND ----------

PK_COL = f"{HISTORY_TABLE}_ID"

df_transf = df_source_prep.withColumn(
    PK_COL, f.concat_ws("-", f.date_format("snapshot_date", "yyyyMMdd"), *KEY_COLUMNS)
)
columns = df_transf.columns
columns.remove(PK_COL)
df_transf = df_transf.select([PK_COL] + columns)

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality Checks

# COMMAND ----------

validation_level = "AMBER" if BYPASS_QUALITY_CHECKS else "RED"
df_gx_final = SparkDFDataset(df_transf, persist=False)

red_quality_check_results = [
    df_gx_final.expect_column_values_to_not_be_null(column=PK_COL),
    df_gx_final.expect_column_values_to_be_unique(column=PK_COL),
]
gx_validation.validate_and_log_gx_results(
    quality_check_results=red_quality_check_results,
    validation_level=validation_level,
    info_message=f"{PK_COL} unique and not null",
    error_message=f"{PK_COL} check failed - check the deduplication on KEY_COLUMNS",
    pipeline_write_env=PIPELINE_WRITE_ENV,
    table_name=DESTINATION_TABLE,
    log_to_event_table=True,
    log_successes=True,
)

amber_quality_check_results = [
    df_gx_final.expect_column_values_to_not_be_null(column="<key_1>"),
]
gx_validation.validate_and_log_gx_results(
    quality_check_results=amber_quality_check_results,
    validation_level="AMBER",
    info_message="Key dimensions populated",
    error_message="Key dimension null - check the source extraction",
    pipeline_write_env=PIPELINE_WRITE_ENV,
    table_name=DESTINATION_TABLE,
    log_to_event_table=True,
    log_successes=True,
)

# COMMAND ----------

# MAGIC # Outputs

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{SCHEMA}")

try:
    startTime = time.time()
    if INCREMENTAL_LOAD:
        result = table_utils.save_table(
            dest_table=DESTINATION_TABLE_FULL_PATH,
            df=df_transf,
            mode="overwrite",
            overwrite_schema=False,
            partition_col="snapshot_date",
            merge_schema=True,
            replace_where="snapshot_date IN ({})".format(
                ", ".join(f"DATE'{d.isoformat()}'" for d in SNAPSHOT_DATES)
            ),
        )
    else:
        result = table_utils.save_table(
            dest_table=DESTINATION_TABLE_FULL_PATH,
            df=df_transf,
            mode="overwrite",
            overwrite_schema=True,
            partition_col="snapshot_date",
        )
except AnalysisException as e:
    log.error(
        f"Table {DESTINATION_TABLE_FULL_PATH} has not been created, an error occurred while saving it. \nError is {e}"
    )
    raise
else:
    duration = time.time() - startTime
    log.info(f"Table {DESTINATION_TABLE_FULL_PATH} saved in {duration} seconds.")
    if result is not None:
        result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Primary key constraint (idempotent)

# COMMAND ----------

TABLE_CONSTRAINT_NAME = f"gold_{DESTINATION_TABLE}_PK"
TABLE_PK_COLS = PK_COL

nb_cons = spark.sql(
    f"select count(*) from system.information_SCHEMA.table_constraints "
    f"where table_CATALOG='{CATALOG}' and table_SCHEMA='{SCHEMA}' "
    f"and table_name='{DESTINATION_TABLE}' and lower(constraint_name)=lower('{TABLE_CONSTRAINT_NAME}')"
).first()[0]

if nb_cons == 0:
    for col in TABLE_PK_COLS.split(","):
        table_utils.set_table_column_not_null(
            table_path=DESTINATION_TABLE_FULL_PATH, column=col.strip()
        )
    table_utils.add_table_primary_key(
        table_path=DESTINATION_TABLE_FULL_PATH,
        pk_name=TABLE_CONSTRAINT_NAME,
        columns=TABLE_PK_COLS,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exposed view
# MAGIC Technical columns (prefix `_`) are excluded.

# COMMAND ----------

excluded_prefixes = ["_"]
selected_columns = [
    c for c in df_transf.columns if not any(c.startswith(p) for p in excluded_prefixes)
]
table_utils.create_table_view(
    src_full_tablename=DESTINATION_TABLE_FULL_PATH,
    dest_full_tablename=f"{DESTINATION_TABLE_FULL_PATH}_exposed",
    select_statement=", ".join(selected_columns),
    replace_view=True,
)

