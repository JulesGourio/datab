# Databricks notebook source
# MAGIC %md
# MAGIC # GOLD AVERAGE BOM HISTORY
# MAGIC
# MAGIC **Description:**
# MAGIC Monthly history of the **average BOM** ("BOM commune"): for each manufactured article (AF) and plant, the single BOM that
# MAGIC SAP considered *the* BOM of the article at the start of each month, phantoms exploded. One row per snapshot date x
# MAGIC plant x AF x BOM line (component), quantities per unit of AF with component scrap included.
# MAGIC It is the "as-designed" Forecast 3 of the BOM reliability use case (UC93) and answers the question
# MAGIC "what was the common BOM of this article on date X".
# MAGIC
# MAGIC **Highlighted complexities:**
# MAGIC - Same selection rule as the existing Average BOM indicator (`create_proj_average_bom`), applied **at each snapshot date**
# MAGIC   instead of today:
# MAGIC   1. production versions that are not locked and valid at the snapshot date (`MKAL`, from the landing zone stack);
# MAGIC   2. one version only: the quota arrangement version when there are several, else the earliest valid-from, else the lowest version id;
# MAGIC   3. its BOM alternative is read in `bom_item_history`; an AF without usable production version takes its lowest alternative
# MAGIC      (`selection_rule = LOWEST_ALTERNATIVE`).
# MAGIC - Quota arrangements are **not historised** (no stack): the current `quotas_arrangement_exposed` is evaluated at each snapshot date.
# MAGIC - Phantom assemblies (`MARC.SOBSL = 50`) are replaced by the BOM selected for the phantom itself (same snapshot, same plant),
# MAGIC   up to `MAX_PHANTOM_DEPTH` levels. Exploded lines carry BOM item `9999` and the phantom chain. A phantom that cannot be exploded
# MAGIC   is kept and flagged (`explosion_status`).
# MAGIC - Bulk material and plant material status (`05`) are **flagged, not filtered**, and the `SPLIT` article is kept (identified by its
# MAGIC   number): the use case decides.
# MAGIC - Sources are the landing zone `*_stack` tables and the Gold `bom_item_history`; for each snapshot date the latest extraction
# MAGIC   **strictly before** that date is used ("known at the start of the day").
# MAGIC
# MAGIC **Role in the BOM reliability measure:**
# MAGIC - Forecast 3 (average BOM): `quantity_per_unit` x OF quantity + `quantity_per_order`, scrap already included.
# MAGIC
# MAGIC **Intended Pipeline**
# MAGIC - M_2_Production_BOM_History_Data_Asset (to be confirmed), task after `create_gold_bom_item_history`
# MAGIC
# MAGIC **Inputs Data**
# MAGIC - {PIPELINE_WRITE_ENV}_gold.production.bom_item_history
# MAGIC - {REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6.mkal_stack
# MAGIC - {REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6.marc_stack
# MAGIC - {REFERENCE_READ_ENV}_gold.production.quotas_arrangement_exposed
# MAGIC
# MAGIC **Output Tables (Pipeline)**
# MAGIC - {PIPELINE_WRITE_ENV}_gold.production.average_bom_history
# MAGIC - {PIPELINE_WRITE_ENV}_gold.production.average_bom_history_exposed (view)
# MAGIC
# MAGIC **Calculated columns (business meaning)**
# MAGIC - `production_version`, `selection_rule`, `nb_active_versions`: which version gave the BOM and how it was chosen
# MAGIC   (`SINGLE_ACTIVE_VERSION`, `QUOTA_VERSION`, `EARLIEST_ACTIVE_VERSION`, `LOWEST_ALTERNATIVE`).
# MAGIC - `quantity_per_unit`: component quantity (base unit) for one unit of AF, scrap included, phantoms exploded; `0` for fixed-quantity items.
# MAGIC - `quantity_per_order`: fixed quantity consumed once per order, scrap included; `0` for proportional items.
# MAGIC - `explosion_status`: `DIRECT`, `EXPLODED`, `PHANTOM_WITHOUT_BOM`, `PHANTOM_DEPTH_EXCEEDED`.
# MAGIC - `phantom_path`: phantom articles crossed from the AF to the component (empty when direct).

# COMMAND ----------

# MAGIC %pip install "great_expectations==0.18.22"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Technical debt
# MAGIC - Lab runs only, to remove before the move to Bitbucket: the `%pip install great_expectations` cell and its restart, `sys.path.insert(...)` (`leap_utils`) and the `persist` / `cache` patch (serverless) in the standard imports cell.
# MAGIC - Quota arrangements are read in their **current** state: a quota deleted since a past snapshot date is unknown, and a quota created since is applied to snapshots before its validity only if its validity dates allow it. Only matters for AF with several valid production versions.
# MAGIC - The existing Average BOM notebooks disagree on the choice among several valid versions (`create_proj_average_bom`: quota, then earliest valid-from; `create_proj_bom_average_multilevel`: lowest alternative). The first one is applied here: to be confirmed.
# MAGIC - Fallback `LOWEST_ALTERNATIVE`: the existing indicator keeps the BOM created first, but the creation date of the BOM header is not kept by `bom_item_history`.
# MAGIC - A phantom that contains itself (loop) is cut at the second occurrence: the pre-run checks of the spec (§4, check 12) must return 0 loops.
# MAGIC - A material can have several BOM numbers for the same alternative (lot size ranges, `MAST.losvn` / `losbs`): all are kept, an AMBER check counts them; spec §4, check 13, gives the expected count before the run.
# MAGIC - `sap_number()`, `sap_flag()`, `sap_date()`, `month_starts()`, `build_snapshot_mapping()` are also defined in `create_gold_bom_item_history`: move them to `leap_utils` before the Bitbucket PR.
# MAGIC - The first run after a change of `bom_item_history` snapshots must use `rebuild_mode = full`.

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Standard Package Imports

# COMMAND ----------

import datetime
import sys
import time
from functools import reduce

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

# Lab test runs only: "<catalog>.<schema>" of the lab redirects every table this notebook writes, and
# `bom_item_history` (same project) is then read from the same schema.
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
# MAGIC `snapshot_from_date` (yyyy-MM-dd) and `debug_plant` shorten lab runs; both are empty in jobs. A debug run reads and writes
# MAGIC the `*_debug` tables, like `create_gold_bom_item_history`.
# MAGIC `rebuild_mode`: `full` rewrites every snapshot; `incremental` only adds the snapshot dates of `bom_item_history` missing from the output table.

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
    SCHEMA = "production"
# A debug run holds a subset of the data: it reads and writes *_debug tables, so that it can never
# overwrite the full history in the default table.
DEBUG_RUN = bool(DEBUG_PLANT or SNAPSHOT_FROM_DATE)
DEBUG_SUFFIX = "_debug" if DEBUG_RUN else ""
DESTINATION_TABLE = f"average_bom_history{DEBUG_SUFFIX}"
DESTINATION_TABLE_FULL_PATH = f"{CATALOG}.{SCHEMA}.{DESTINATION_TABLE}"
SOURCE_BOM_TABLE_FULL_PATH = f"{CATALOG}.{SCHEMA}.bom_item_history{DEBUG_SUFFIX}"

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

FULL_EXTRACTION = "FULL"  # MARC also contains partial DELTA extractions
LATEST_ROW_ORDER = ["_ingestion_timestamp", "_stack_row_id"]  # a source extraction can repeat a row
EXCLUDED_ITEM_CATEGORIES = ["D", "T"]  # document and text items: no material
PHANTOM_ITEM_NUMBER = "9999"
MAX_PHANTOM_DEPTH = 7  # same depth as the existing Average BOM notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Helper Functions
# MAGIC - `sap_number`: SAP text number -> double (trailing minus, padding).
# MAGIC - `sap_flag`: SAP indicator `'X'` -> `True`, anything else -> `False`.
# MAGIC - `sap_date`: SAP `yyyyMMdd` text -> date.
# MAGIC - `build_snapshot_mapping`: for each snapshot date, the latest extraction strictly before it.
# MAGIC - `check_required_columns`: stops the notebook with the list of missing columns instead of failing later in a join.

# COMMAND ----------

def check_required_columns(df, table_label, required_columns):
    existing = {c.lower() for c in df.columns}
    missing = sorted(c for c in required_columns if c.lower() not in existing)
    if missing:
        raise ValueError(f"{table_label}: missing columns {missing}. Columns found: {sorted(df.columns)}")


def sap_number(column_name):
    raw = f.trim(f.col(column_name))
    value = f.expr(f"try_cast(regexp_replace(trim({column_name}), '-$', '') AS DOUBLE)")
    return f.when(raw.endswith("-"), -value).otherwise(value)


def sap_flag(column_name):
    return f.coalesce(f.trim(f.col(column_name)) == "X", f.lit(False))


def sap_date(column_name):
    return f.try_to_date(f.trim(f.col(column_name)), "yyyyMMdd")


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


def select_extractions(df_raw, df_map):
    """Keep only the extractions used by the calendar (partition pruning), tagged with their snapshot date(s).
    One extraction can serve several snapshot dates when an extraction is missing: intended."""
    used_timestamps = [r["extraction_timestamp"] for r in df_map.select("extraction_timestamp").distinct().collect()]
    return df_raw.filter(f.col("extraction_timestamp").isin(used_timestamps)).join(
        f.broadcast(df_map), ["extraction_timestamp"], how="inner"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Inputs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import LEAP tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold table of this project
# MAGIC The standard BOM of every article at each snapshot date, all alternatives. Read from the lab schema during lab runs.

# COMMAND ----------

df_bom_raw = spark.read.table(SOURCE_BOM_TABLE_FULL_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Landing zone stacks
# MAGIC Full SAP extractions appended at each `extraction_timestamp`: production versions (`MKAL`) and article status (`MARC`).
# MAGIC Bronze `_latest` tables only hold the last extraction.

# COMMAND ----------

df_mkal_raw = spark.read.table(f"{LANDING_ZONE_SCHEMA}.mkal_stack")
df_marc_raw = spark.read.table(f"{LANDING_ZONE_SCHEMA}.marc_stack")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reference Gold tables
# MAGIC Quota arrangements: used by the existing Average BOM only to choose among several valid production versions.

# COMMAND ----------

df_quota_raw = spark.read.table(f"{REFERENCE_READ_ENV}_gold.production.quotas_arrangement_exposed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Required columns
# MAGIC The notebook reads SAP field names and Gold columns by name: it stops here, with the list of what is missing,
# MAGIC if one of the inputs does not have the expected structure.

# COMMAND ----------

check_required_columns(
    df_bom_raw,
    SOURCE_BOM_TABLE_FULL_PATH,
    [
        "snapshot_date", "plant", "material_number", "material_description", "BOM_alternative", "BOM_number",
        "BOM_node", "BOM_item_number", "BOM_item_category", "BOM_status", "component_material_number",
        "component_description", "component_base_unit", "component_scrap_percentage",
        "material_component_scrap_percentage", "is_fixed_quantity", "component_quantity_in_base_unit",
        "BOM_base_quantity", "is_phantom_item", "is_bulk_material", "_is_backdated_link",
        "_is_backdated_allocation", "_BOM_extraction_timestamp",
    ],
)
check_required_columns(
    df_mkal_raw,
    "mkal_stack",
    ["extraction_timestamp", "ingestion_timestamp", "file_mode", "stack_row_id", "matnr", "werks", "verid",
     "stlal", "mksp", "adatu", "bdatu"],
)
check_required_columns(
    df_marc_raw,
    "marc_stack",
    ["extraction_timestamp", "ingestion_timestamp", "file_mode", "stack_row_id", "matnr", "werks", "mmsta"],
)
check_required_columns(
    df_quota_raw,
    "quotas_arrangement_exposed",
    ["quota_number", "material_number", "plant", "production_version", "quota", "Start_Validity", "End_Validity"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extraction calendars
# MAGIC Only the `FULL` extractions of the stacks (a `DELTA` extraction only holds the changes of the day).

# COMMAND ----------

mkal_timestamps = sorted(
    r["extraction_timestamp"]
    for r in df_mkal_raw.filter(f.col("file_mode") == FULL_EXTRACTION)
    .select("extraction_timestamp").distinct().collect()
)
marc_timestamps = sorted(
    r["extraction_timestamp"]
    for r in df_marc_raw.filter(f.col("file_mode") == FULL_EXTRACTION)
    .select("extraction_timestamp").distinct().collect()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep1 - Snapshot calendar
# MAGIC The snapshot dates are those of `bom_item_history` (first day of each month), so both tables always describe the same dates.
# MAGIC For each stack, the extraction used is the latest one **strictly before** the snapshot date.

# COMMAND ----------

SNAPSHOT_DATES = sorted(r["snapshot_date"] for r in df_bom_raw.select("snapshot_date").distinct().collect())
if SNAPSHOT_FROM_DATE:
    SNAPSHOT_DATES = [d for d in SNAPSHOT_DATES if d >= datetime.date.fromisoformat(SNAPSHOT_FROM_DATE)]
if INCREMENTAL_LOAD:
    stored_snapshot_dates = {
        r["snapshot_date"]
        for r in spark.read.table(DESTINATION_TABLE_FULL_PATH).select("snapshot_date").distinct().collect()
    }
    SNAPSHOT_DATES = [d for d in SNAPSHOT_DATES if d not in stored_snapshot_dates]
if not SNAPSHOT_DATES:
    log.info("No new snapshot date to load")
    dbutils.notebook.exit("No new snapshot date to load")
log.info(f"{len(SNAPSHOT_DATES)} snapshot dates, from {SNAPSHOT_DATES[0]} to {SNAPSHOT_DATES[-1]}")

df_snapshot_dates = spark.createDataFrame([(d,) for d in SNAPSHOT_DATES], "snapshot_date date")

df_map_mkal = build_snapshot_mapping(mkal_timestamps, SNAPSHOT_DATES)
df_map_marc = build_snapshot_mapping(marc_timestamps, SNAPSHOT_DATES, backdate_first=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep2 - Standard BOM items (bom_item_history)
# MAGIC All alternatives of the snapshot dates to build. Items without component (document / text items) are dropped.
# MAGIC Quantities per unit: proportional items are divided by the BOM base quantity, fixed-quantity items are per order
# MAGIC (`0` in the other column). Component scrap is the BOM item scrap, else the article scrap (SAP rule); the scrap factor is
# MAGIC applied here so that phantom explosion multiplies final quantities.

# COMMAND ----------

df_item_prep = df_bom_raw.filter(f.col("snapshot_date").isin(SNAPSHOT_DATES))
if DEBUG_PLANT:
    df_item_prep = df_item_prep.filter(f.col("plant") == DEBUG_PLANT)

LINE_COLUMNS = [
    "snapshot_date",
    "plant",
    "material_number",
    "material_description",
    "BOM_alternative",
    "BOM_number",
    "BOM_node",
    "BOM_item_number",
    "BOM_item_category",
    "BOM_status",
    "component_material_number",
    "component_description",
    "component_base_unit",
    "quantity_per_unit",
    "quantity_per_order",
    "component_scrap_percentage",
    "is_phantom_item",
    "is_bulk_material",
    "_is_backdated_link",
    "_is_backdated_allocation",
    "_BOM_extraction_timestamp",
]

df_item_prep = (
    df_item_prep.filter(f.col("component_material_number").isNotNull())
    .filter(~f.coalesce(f.col("BOM_item_category"), f.lit("")).isin(EXCLUDED_ITEM_CATEGORIES))
    .withColumn(
        "scrap_factor",
        1
        + f.when(f.col("component_scrap_percentage") > 0, f.col("component_scrap_percentage"))
        .otherwise(f.coalesce(f.col("material_component_scrap_percentage"), f.lit(0.0)))
        / 100,
    )
    .withColumn(
        "quantity_per_unit",
        f.when(
            f.col("is_fixed_quantity"), f.lit(0.0)
        ).otherwise(
            f.try_divide(f.col("component_quantity_in_base_unit"), f.col("BOM_base_quantity")) * f.col("scrap_factor")
        ),
    )
    .withColumn(
        "quantity_per_order",
        f.when(
            f.col("is_fixed_quantity"), f.col("component_quantity_in_base_unit") * f.col("scrap_factor")
        ).otherwise(f.lit(0.0)),
    )
    .withColumn("component_scrap_percentage", (f.col("scrap_factor") - 1) * 100)
    .select(*LINE_COLUMNS)
)

df_gx_item = SparkDFDataset(df_item_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep3 - Alternatives available at each snapshot date
# MAGIC The production version is only usable if the BOM alternative it points to exists at the snapshot date
# MAGIC (status not excluded, items present). One row per (snapshot, plant, AF, alternative).

# COMMAND ----------

df_alternative_prep = df_item_prep.select("snapshot_date", "plant", "material_number", "BOM_alternative").distinct()

df_gx_alternative = SparkDFDataset(df_alternative_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep4 - Production versions valid at the snapshot date (MKAL)
# MAGIC Not locked and valid at the snapshot date (`valid from <= date < valid to`), as in the existing Average BOM.
# MAGIC `stlal` is the BOM alternative of the version.

# COMMAND ----------

MKAL_COLUMNS = [
    "snapshot_date",
    f.trim("matnr").alias("material_number"),
    f.trim("werks").alias("plant"),
    f.trim("verid").alias("production_version"),
    f.when(f.trim("stlal") == "", f.lit(None)).otherwise(f.trim("stlal")).alias("BOM_alternative"),
    sap_flag("mksp").alias("is_locked"),
    sap_date("adatu").alias("version_valid_from_date"),
    sap_date("bdatu").alias("version_valid_to_date"),
    f.col("ingestion_timestamp").alias("_ingestion_timestamp"),
    f.col("stack_row_id").alias("_stack_row_id"),
]

df_mkal_prep = (
    select_extractions(df_mkal_raw.filter(f.col("file_mode") == FULL_EXTRACTION), df_map_mkal)
    .select(*MKAL_COLUMNS)
    .filter(~f.col("is_locked"))
    .filter(f.col("version_valid_from_date") <= f.col("snapshot_date"))
    .filter(f.col("version_valid_to_date") > f.col("snapshot_date"))
    .drop("is_locked")
)

df_mkal_prep = table_utils.remove_leading_zeros(df=df_mkal_prep, column_names=["material_number"])
if DEBUG_PLANT:
    df_mkal_prep = df_mkal_prep.filter(f.col("plant") == DEBUG_PLANT)
df_mkal_prep = table_utils.drop_duplicates_by_order(
    df=df_mkal_prep,
    columns=["snapshot_date", "material_number", "plant", "production_version"],
    order_col=LATEST_ROW_ORDER,
    keep="last",
).drop(*LATEST_ROW_ORDER)

df_gx_mkal = SparkDFDataset(df_mkal_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep5 - Quota arrangement versions
# MAGIC Used only to choose among several valid versions. Per quota arrangement valid at the snapshot date, the version with the
# MAGIC highest quota (no quota-weighted average: same simplification as the existing Average BOM). Current quota table,
# MAGIC see technical debt.

# COMMAND ----------

window_quota = Window.partitionBy("snapshot_date", "quota_number").orderBy(
    f.desc("quota"), f.asc("production_version")
)

df_quota_prep = (
    df_quota_raw.select(
        "quota_number",
        "material_number",
        "plant",
        "production_version",
        "quota",
        f.col("Start_Validity").alias("quota_valid_from_date"),
        f.col("End_Validity").alias("quota_valid_to_date"),
    )
)
df_quota_prep = table_utils.remove_leading_zeros(df=df_quota_prep, column_names=["material_number"])

df_quota_prep = (
    df_quota_prep.join(
        f.broadcast(df_snapshot_dates),
        (f.col("quota_valid_from_date") <= f.col("snapshot_date"))
        & (f.col("quota_valid_to_date") > f.col("snapshot_date")),
        how="inner",
    )
    .withColumn("rn", f.row_number().over(window_quota))
    .filter("rn = 1")
    .select("snapshot_date", "material_number", "plant", "production_version")
    .distinct()
    .withColumn("is_quota_version", f.lit(True))
)

df_gx_quota = SparkDFDataset(df_quota_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep6 - Plant status of the components (MARC)
# MAGIC `mmsta` is the plant-specific material status (`05` = everything available to launch production). Flag only, no filter.

# COMMAND ----------

MARC_COLUMNS = [
    "snapshot_date",
    f.trim("matnr").alias("component_material_number"),
    f.trim("werks").alias("plant"),
    f.trim("mmsta").alias("component_plant_status"),
    f.col("ingestion_timestamp").alias("_ingestion_timestamp"),
    f.col("stack_row_id").alias("_stack_row_id"),
]

df_marc_prep = select_extractions(
    df_marc_raw.filter(f.col("file_mode") == FULL_EXTRACTION), df_map_marc
).select(*MARC_COLUMNS)

df_marc_prep = table_utils.remove_leading_zeros(df=df_marc_prep, column_names=["component_material_number"])
df_marc_prep = table_utils.drop_duplicates_by_order(
    df=df_marc_prep,
    columns=["snapshot_date", "component_material_number", "plant"],
    order_col=LATEST_ROW_ORDER,
    keep="last",
).drop(*LATEST_ROW_ORDER)

df_gx_marc = SparkDFDataset(df_marc_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 1 - Candidate production versions
# MAGIC Only versions whose BOM alternative exists at the snapshot date are candidates (INNER on the alternatives of Prep3).
# MAGIC The quota flag is added (LEFT) and the number of candidates per AF counted.

# COMMAND ----------

df_candidate = df_mkal_prep.join(
    df_alternative_prep, ["snapshot_date", "plant", "material_number", "BOM_alternative"], how="inner"
)
log.info("Joined alternatives on snapshot_date/plant/material_number/BOM_alternative - uniqueness checked in Quality Checks")

# COMMAND ----------

df_candidate = df_candidate.join(
    df_quota_prep, ["snapshot_date", "plant", "material_number", "production_version"], how="left"
).withColumn("is_quota_version", f.coalesce(f.col("is_quota_version"), f.lit(False)))
log.info("Joined quota versions on snapshot_date/plant/material_number/production_version - uniqueness checked in Quality Checks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 2 - One BOM per AF and snapshot date
# MAGIC Rule: the quota version, else the earliest valid-from, else the lowest version id. An AF with no candidate takes its lowest
# MAGIC alternative (`LOWEST_ALTERNATIVE`).

# COMMAND ----------

SELECTION_KEYS = ["snapshot_date", "plant", "material_number"]

window_selection = Window.partitionBy(*SELECTION_KEYS)
window_selection_rank = window_selection.orderBy(
    f.desc("is_quota_version"), f.asc("version_valid_from_date"), f.asc("production_version")
)

df_selected_version = (
    df_candidate.withColumn("nb_active_versions", f.count(f.lit(1)).over(window_selection))
    .withColumn("rn", f.row_number().over(window_selection_rank))
    .filter("rn = 1")
    .withColumn(
        "selection_rule",
        f.when(f.col("nb_active_versions") == 1, f.lit("SINGLE_ACTIVE_VERSION"))
        .when(f.col("is_quota_version"), f.lit("QUOTA_VERSION"))
        .otherwise(f.lit("EARLIEST_ACTIVE_VERSION")),
    )
    .select(*SELECTION_KEYS, "BOM_alternative", "production_version", "selection_rule", "nb_active_versions")
)

# COMMAND ----------

window_lowest_alternative = Window.partitionBy(*SELECTION_KEYS).orderBy(f.asc("BOM_alternative"))

df_fallback_version = (
    df_alternative_prep.join(df_selected_version.select(*SELECTION_KEYS), SELECTION_KEYS, how="left_anti")
    .withColumn("rn", f.row_number().over(window_lowest_alternative))
    .filter("rn = 1")
    .withColumn("production_version", f.lit(None).cast("string"))
    .withColumn("selection_rule", f.lit("LOWEST_ALTERNATIVE"))
    .withColumn("nb_active_versions", f.lit(0).cast("long"))
    .select(*SELECTION_KEYS, "BOM_alternative", "production_version", "selection_rule", "nb_active_versions")
)

df_selection = df_selected_version.unionByName(df_fallback_version)

df_gx_selection = SparkDFDataset(df_selection, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 3 - BOM lines of the selected alternative
# MAGIC INNER: an AF without selected alternative has no BOM line. Every material is selected with the same rule,
# MAGIC including the phantom assemblies exploded in the next step.

# COMMAND ----------

df_direct = df_item_prep.join(
    df_selection, [*SELECTION_KEYS, "BOM_alternative"], how="inner"
)
log.info("Joined selection on snapshot_date/plant/material_number/BOM_alternative - uniqueness checked in Quality Checks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 4 - Phantom explosion
# MAGIC A phantom line is replaced by the lines of the BOM selected for the phantom article (same snapshot, same plant):
# MAGIC `quantity_per_unit = child x parent`, `quantity_per_order = child per unit x parent per order + child per order`.
# MAGIC Exploded lines get BOM item `9999`. Nested phantoms are exploded at the next level, up to `MAX_PHANTOM_DEPTH`.
# MAGIC A phantom without BOM, or still phantom after the last level, is kept and flagged.

# COMMAND ----------

df_child = df_direct.select(
    "snapshot_date",
    "plant",
    f.col("material_number").alias("phantom_material_number"),
    f.col("BOM_number").alias("child_BOM_number"),
    f.col("BOM_node").alias("child_BOM_node"),
    f.col("component_material_number").alias("child_component_material_number"),
    f.col("component_description").alias("child_component_description"),
    f.col("component_base_unit").alias("child_component_base_unit"),
    f.col("quantity_per_unit").alias("child_quantity_per_unit"),
    f.col("quantity_per_order").alias("child_quantity_per_order"),
    f.col("component_scrap_percentage").alias("child_component_scrap_percentage"),
    f.col("is_phantom_item").alias("child_is_phantom_item"),
    f.col("is_bulk_material").alias("child_is_bulk_material"),
)

df_child_keys = df_child.select("snapshot_date", "plant", "phantom_material_number").distinct()

df_level_0 = (
    df_direct.withColumn("explosion_level", f.lit(0))
    .withColumn("phantom_chain", f.array().cast("array<string>"))
    .withColumn("explosion_key", f.concat_ws(":", "BOM_number", "BOM_node"))
)

# COMMAND ----------

df_leaf_parts = [df_level_0.filter(~f.col("is_phantom_item")).withColumn("explosion_status", f.lit("DIRECT"))]
df_unexploded_parts = []
df_phantom_lines = df_level_0.filter(f.col("is_phantom_item"))

for level in range(1, MAX_PHANTOM_DEPTH + 1):
    df_phantom_keyed = df_phantom_lines.withColumn("phantom_material_number", f.col("component_material_number"))

    df_unexploded_parts.append(
        df_phantom_keyed.join(df_child_keys, ["snapshot_date", "plant", "phantom_material_number"], how="left_anti")
        .drop("phantom_material_number")
        .withColumn("explosion_status", f.lit("PHANTOM_WITHOUT_BOM"))
    )

    df_exploded = (
        df_phantom_keyed.join(df_child, ["snapshot_date", "plant", "phantom_material_number"], how="inner")
        .withColumn("phantom_chain", f.array_append(f.col("phantom_chain"), f.col("phantom_material_number")))
        .filter(~f.array_contains(f.col("phantom_chain"), f.col("child_component_material_number")))
        .select(
            "snapshot_date",
            "plant",
            "material_number",
            "material_description",
            "BOM_alternative",
            "BOM_number",
            "BOM_node",
            f.lit(PHANTOM_ITEM_NUMBER).alias("BOM_item_number"),
            "BOM_item_category",
            "BOM_status",
            f.col("child_component_material_number").alias("component_material_number"),
            f.col("child_component_description").alias("component_description"),
            f.col("child_component_base_unit").alias("component_base_unit"),
            (f.col("child_quantity_per_unit") * f.col("quantity_per_unit")).alias("quantity_per_unit"),
            (
                f.col("child_quantity_per_unit") * f.col("quantity_per_order") + f.col("child_quantity_per_order")
            ).alias("quantity_per_order"),
            f.col("child_component_scrap_percentage").alias("component_scrap_percentage"),
            f.col("child_is_phantom_item").alias("is_phantom_item"),
            f.col("child_is_bulk_material").alias("is_bulk_material"),
            "_is_backdated_link",
            "_is_backdated_allocation",
            "_BOM_extraction_timestamp",
            "production_version",
            "selection_rule",
            "nb_active_versions",
            f.lit(level).alias("explosion_level"),
            "phantom_chain",
            f.concat_ws(
                ">", "explosion_key", f.concat_ws(":", "child_BOM_number", "child_BOM_node")
            ).alias("explosion_key"),
        )
    )

    df_leaf_parts.append(
        df_exploded.filter(~f.col("is_phantom_item")).withColumn("explosion_status", f.lit("EXPLODED"))
    )
    df_phantom_lines = df_exploded.filter(f.col("is_phantom_item"))

df_unexploded_parts.append(
    df_phantom_lines.withColumn("explosion_status", f.lit("PHANTOM_DEPTH_EXCEEDED"))
)

df_transf = reduce(lambda a, b: a.unionByName(b), df_leaf_parts + df_unexploded_parts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 5 - Phantom path and plant status of the component
# MAGIC `phantom_path` lists the phantom articles crossed (empty when direct). The component plant status is a flag (LEFT).

# COMMAND ----------

df_transf = df_transf.withColumn("phantom_path", f.array_join(f.col("phantom_chain"), ">")).drop("phantom_chain")

# COMMAND ----------

df_transf = df_transf.join(
    df_marc_prep, ["snapshot_date", "plant", "component_material_number"], how="left"
)
log.info("Joined MARC on snapshot_date/plant/component_material_number - key uniqueness checked in Quality Checks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 6 - Create ID column and put it first
# MAGIC The grain is the line of the AF BOM: snapshot, plant, AF, then the chain `BOM_number:BOM_node` from the AF line down to the
# MAGIC component line (a single token when direct).

# COMMAND ----------

PK_COL = "average_bom_history_ID"

df_transf = df_transf.withColumn(
    PK_COL,
    f.concat_ws(
        "-",
        f.date_format("snapshot_date", "yyyyMMdd"),
        "plant",
        "material_number",
        "explosion_key",
    ),
)

OUTPUT_COLUMNS = [
    PK_COL,
    "snapshot_date",
    "plant",
    "material_number",
    "material_description",
    "production_version",
    "BOM_alternative",
    "selection_rule",
    "nb_active_versions",
    "BOM_number",
    "BOM_item_number",
    "BOM_item_category",
    "BOM_status",
    "component_material_number",
    "component_description",
    "component_base_unit",
    "quantity_per_unit",
    "quantity_per_order",
    "component_scrap_percentage",
    "component_plant_status",
    "is_phantom_item",
    "is_bulk_material",
    "explosion_status",
    "explosion_level",
    "phantom_path",
    "_is_backdated_link",
    "_is_backdated_allocation",
    "_BOM_extraction_timestamp",
]

df_transf = df_transf.select(*OUTPUT_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality Checks
# MAGIC Every check is grouped here. RED stops the job, AMBER only warns.
# MAGIC `by_pass_quality_checks=true` downgrades RED to AMBER (debug / backfill only).

# COMMAND ----------

validation_level = "AMBER" if BYPASS_QUALITY_CHECKS else "RED"

df_gx_final = SparkDFDataset(df_transf, persist=False)
red_quality_check_results = [
    # PK of the final table
    df_gx_final.expect_column_values_to_not_be_null(column=PK_COL),
    df_gx_final.expect_column_values_to_be_unique(column=PK_COL),
    # Join keys of every prepared table: no fan-out
    df_gx_alternative.expect_compound_columns_to_be_unique(
        ["snapshot_date", "plant", "material_number", "BOM_alternative"]
    ),
    df_gx_mkal.expect_compound_columns_to_be_unique(
        ["snapshot_date", "plant", "material_number", "production_version"]
    ),
    df_gx_quota.expect_compound_columns_to_be_unique(
        ["snapshot_date", "plant", "material_number", "production_version"]
    ),
    df_gx_selection.expect_compound_columns_to_be_unique(["snapshot_date", "plant", "material_number"]),
    df_gx_marc.expect_compound_columns_to_be_unique(["snapshot_date", "plant", "component_material_number"]),
]

gx_validation.validate_and_log_gx_results(
    quality_check_results=red_quality_check_results,
    validation_level=validation_level,
    info_message="All checks inspection",
    error_message="Quality checks failed for average_bom_history table, please check log messages",
    pipeline_write_env=PIPELINE_WRITE_ENV,
    table_name="average_bom_history",
    log_to_event_table=True,
    log_successes=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC AMBER:
# MAGIC - most AF should have a usable production version (`production_version` filled);
# MAGIC - phantoms should all be exploded (`explosion_status` DIRECT / EXPLODED);
# MAGIC - a selected alternative should hold a single BOM number (several = lot size ranges, lines are all kept);
# MAGIC - quantities known (unit conversion found in `bom_item_history`).

# COMMAND ----------

df_bom_number_check = df_direct.select(*SELECTION_KEYS, "BOM_number").distinct()
df_gx_bom_number = SparkDFDataset(df_bom_number_check, persist=False)

amber_quality_check_results = [
    df_gx_final.expect_column_values_to_not_be_null(column="production_version", mostly=0.9),
    df_gx_final.expect_column_values_to_be_in_set(
        column="explosion_status", value_set=["DIRECT", "EXPLODED"], mostly=0.999
    ),
    df_gx_bom_number.expect_compound_columns_to_be_unique(SELECTION_KEYS),
    df_gx_final.expect_column_values_to_not_be_null(column="quantity_per_unit", mostly=0.99),
    df_gx_final.expect_column_values_to_not_be_null(column="component_plant_status", mostly=0.95),
]

gx_validation.validate_and_log_gx_results(
    quality_check_results=amber_quality_check_results,
    validation_level="AMBER",
    info_message="Production version found, phantoms exploded, quantities and plant status populated",
    error_message="Missing production version / unexploded phantom / unknown quantity - check MKAL, bom_item_history and MARC",
    pipeline_write_env=PIPELINE_WRITE_ENV,
    table_name="average_bom_history",
    log_to_event_table=True,
    log_successes=True,
)

# COMMAND ----------

# MAGIC %md
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
