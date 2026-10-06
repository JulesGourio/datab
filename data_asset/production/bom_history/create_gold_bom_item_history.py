# Databricks notebook source
# MAGIC %md
# MAGIC # GOLD BOM ITEM HISTORY
# MAGIC
# MAGIC **Description:**
# MAGIC Monthly history of every SAP material BOM (BOM category `M`, usage `1` = production), as SAP knew it at the
# MAGIC start of each month: one row per snapshot date x plant x manufactured article (AF) x alternative x BOM item,
# MAGIC with quantities, base quantity, units, scrap rates and phantom flag.
# MAGIC It is the "standard BOM" (Prévision 3) of the BOM reliability use case and can be reused for any
# MAGIC "what did the BOM look like at date X" question.
# MAGIC
# MAGIC **Highlighted complexities:**
# MAGIC - Sources are the landing zone `*_stack` tables (one full SAP extraction per `extraction_timestamp`), not Bronze:
# MAGIC   Bronze only keeps the latest extraction. For each snapshot date we take the **latest extraction strictly
# MAGIC   before** that date ("known at the start of the day"). STPO/STKO are weekly (since 2023-04-23),
# MAGIC   MAST/STAS daily (since 2024-06-11), MARC/MARM daily but with partial `DELTA` extractions that are skipped.
# MAGIC - Before 2024-06-11, MAST/STAS do not exist: their first extraction is reused (back-dated), restricted to links
# MAGIC   created before the snapshot date (`andat`). Flag `_is_backdated_link`.
# MAGIC - Engineering change numbers: a changed item becomes a **new** BOM node; the old node is ended by a STAS record
# MAGIC   with `lkenz = 'X'`. The latest STAS / STKO record of each node / alternative is kept, then deleted ones are
# MAGIC   dropped. No validity date is ever in the future of its extraction, so "valid at the snapshot date" = latest record.
# MAGIC - `stlnr` is only unique per BOM category (`stlty`): every stack is filtered on `stlty = 'M'` before any join.
# MAGIC - SAP numbers are text with a trailing minus (`1.000-`): converted with `try_cast` after moving the sign.
# MAGIC - Phantom assemblies are flagged from the component plant data (`MARC.SOBSL = '50'`); STPO has no phantom field.
# MAGIC   They are kept here; the explosion is done by the use case.
# MAGIC
# MAGIC **Intended Pipeline**
# MAGIC - M_2_Production_BOM_History_Data_Asset (to be confirmed)
# MAGIC
# MAGIC **Inputs Data**
# MAGIC - {REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6.stpo_stack
# MAGIC - {REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6.stko_stack
# MAGIC - {REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6.stas_stack
# MAGIC - {REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6.mast_stack
# MAGIC - {REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6.marc_stack
# MAGIC - {REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6.marm_stack
# MAGIC - {REFERENCE_READ_ENV}_gold.master_data.material_exposed
# MAGIC
# MAGIC **Output Tables (Pipeline)**
# MAGIC - {PIPELINE_WRITE_ENV}_gold.production.bom_item_history
# MAGIC - {PIPELINE_WRITE_ENV}_gold.production.bom_item_history_exposed (view)

# COMMAND ----------

# MAGIC %md
# MAGIC # Technical debt
# MAGIC - Landing zone stacks are read directly from Gold (no Bronze history table exists). To be replaced if the
# MAGIC   platform exposes Bronze `*_history` tables.
# MAGIC - `sap_number()`, `month_starts()` and `build_snapshot_mapping()` are also defined in
# MAGIC   `create_gold_order_component_requirement_history`: move them to `leap_utils` before the Bitbucket PR
# MAGIC   (`table_utils.transform_float_column` could replace `sap_number()` once its signature is confirmed).
# MAGIC - MAST/STAS before 2024-06-11 are back-dated from their first extraction: a link deleted without change number
# MAGIC   between 2023-04 and 2024-06 is missing for that period.
# MAGIC - Unit conversions of the same dimension (IN -> M, MM -> M, G -> KG...) have no MARM record: they come from the
# MAGIC   constant `ISO_UNIT_FACTORS` below (SAP uses table T006, not available in the lakehouse).
# MAGIC - Base unit of the component comes from the current `material_exposed` (not historised).
# MAGIC - MARC / MARM (and PLAF / AFKO in the requirement history) contain a few repeated rows in some extractions
# MAGIC   (e.g. article F5391312700300 / plant 1900 in the extraction used for 2023-05 and 2023-06):
# MAGIC   `keep_latest_row()` keeps the most recently ingested one. Root cause in the ingestion to be reported.

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Standard Package Imports

# COMMAND ----------

import datetime
import time

from pyspark.sql import functions as f
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

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

dbutils.widgets.removeAll()  # must be the first widget call

dbutils.widgets.text("pipeline_write_env", "dev")
PIPELINE_WRITE_ENV = dbutils.widgets.get("pipeline_write_env")

dbutils.widgets.text("pipeline_read_env", "dev")
PIPELINE_READ_ENV = dbutils.widgets.get("pipeline_read_env")

dbutils.widgets.text("reference_read_env", "prod")
REFERENCE_READ_ENV = dbutils.widgets.get("reference_read_env")

dbutils.widgets.text("by_pass_quality_checks", "false")
BYPASS_QUALITY_CHECKS = dbutils.widgets.get("by_pass_quality_checks").lower() == "true"

# Lab test runs only: "<catalog>.<schema>" of the lab redirects every table this notebook writes.
# Always empty in jobs; never commit a non-empty default (hardcoded env = Convention Checker BLOCKER).
dbutils.widgets.text("lab_target_schema", "")
LAB_TARGET_SCHEMA = dbutils.widgets.get("lab_target_schema").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Debug Boolean
# MAGIC `snapshot_from_date` (yyyy-MM-dd) and `debug_plant` shorten lab runs; both are empty in jobs.

# COMMAND ----------

dbutils.widgets.text("debug", "False")
DEBUG = dbutils.widgets.get("debug").lower() == "true"

dbutils.widgets.dropdown(
    "log_level",
    defaultValue="info",
    choices=["debug", "info", "warning", "error", "critical"],
)
LOG_LEVEL = dbutils.widgets.get("log_level")

dbutils.widgets.text("snapshot_from_date", "")
SNAPSHOT_FROM_DATE = dbutils.widgets.get("snapshot_from_date").strip()

dbutils.widgets.text("debug_plant", "")
DEBUG_PLANT = dbutils.widgets.get("debug_plant").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config logger

# COMMAND ----------

log = logger.setup_applevel_logger(log_level=logger.LOGGER_MAPPING[LOG_LEVEL])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Business Constants

# COMMAND ----------

LANDING_ZONE_SCHEMA = f"{REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6"

BOM_CATEGORY = "M"  # stlty: material BOM (K = costing BOM shares stlnr values with M)
BOM_USAGES = ["1"]  # stlan: production (99.97 % of MAST links)
PHANTOM_SPECIAL_PROCUREMENT = "50"  # MARC.sobsl of a phantom assembly
FULL_EXTRACTION = "FULL"  # MARC/MARM also contain partial DELTA extractions

# Same-dimension conversions SAP does through T006 (no MARM record): (from_unit, base_unit, factor)
ISO_UNIT_FACTORS = [
    ("IN", "M", 0.0254),
    ("FT", "M", 0.3048),
    ("CM", "M", 0.01),
    ("MM", "M", 0.001),
    ("IN2", "M2", 0.00064516),
    ("CM2", "M2", 0.0001),
    ("MM2", "M2", 0.000001),
    ("CCM", "L", 0.001),
    ("QT", "ML", 946.352946),
    ("GLL", "ML", 3785.411784),
    ("MG", "G", 0.001),
    ("G", "KG", 0.001),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Helper Functions
# MAGIC - `sap_number`: SAP text number -> double. SAP writes negatives with a trailing minus (`1.000-`) and pads with
# MAGIC   spaces; a bare cast would return NULL or fail.
# MAGIC - `sap_flag`: SAP indicator `'X'` -> `True`, anything else -> `False`.
# MAGIC - `keep_latest_row`: one row per key when a source extraction repeats a row.
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


def keep_latest_row(df, key_columns):
    """One row per key, the most recently ingested one: a source extraction can repeat a row."""
    window = Window.partitionBy(*key_columns).orderBy(f.desc("_ingestion_timestamp"), f.desc("_stack_row_id"))
    return (
        df.withColumn("rn", f.row_number().over(window))
        .filter("rn = 1")
        .drop("rn", "_ingestion_timestamp", "_stack_row_id")
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
# MAGIC # Inputs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import LEAP tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Landing zone stacks
# MAGIC Full SAP extractions appended at each `extraction_timestamp` (partition column). Bronze `_latest` tables only
# MAGIC hold the last extraction, so the history can only come from here.

# COMMAND ----------

df_stpo_raw = spark.read.table(f"{LANDING_ZONE_SCHEMA}.stpo_stack")
df_stko_raw = spark.read.table(f"{LANDING_ZONE_SCHEMA}.stko_stack")
df_stas_raw = spark.read.table(f"{LANDING_ZONE_SCHEMA}.stas_stack")
df_mast_raw = spark.read.table(f"{LANDING_ZONE_SCHEMA}.mast_stack")
df_marc_raw = spark.read.table(f"{LANDING_ZONE_SCHEMA}.marc_stack")
df_marm_raw = spark.read.table(f"{LANDING_ZONE_SCHEMA}.marm_stack")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold tables
# MAGIC Base unit of each component (unit of reservations and goods movements).

# COMMAND ----------

df_material_raw = spark.read.table(f"{REFERENCE_READ_ENV}_gold.master_data.material_exposed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extraction calendars
# MAGIC Partition metadata only for the full-extraction stacks; MARC/MARM need `file_mode` to skip `DELTA` extractions.

# COMMAND ----------

stpo_timestamps = list_partition_timestamps(f"{LANDING_ZONE_SCHEMA}.stpo_stack")
stko_timestamps = list_partition_timestamps(f"{LANDING_ZONE_SCHEMA}.stko_stack")
stas_timestamps = list_partition_timestamps(f"{LANDING_ZONE_SCHEMA}.stas_stack")
mast_timestamps = list_partition_timestamps(f"{LANDING_ZONE_SCHEMA}.mast_stack")

marc_timestamps = sorted(
    r["extraction_timestamp"]
    for r in df_marc_raw.filter(f.col("file_mode") == FULL_EXTRACTION)
    .select("extraction_timestamp").distinct().collect()
)
marm_timestamps = sorted(
    r["extraction_timestamp"]
    for r in df_marm_raw.filter(f.col("file_mode") == FULL_EXTRACTION)
    .select("extraction_timestamp").distinct().collect()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep1 - Snapshot calendar
# MAGIC One snapshot on the first day of each month, from the first month after the first STPO extraction to the
# MAGIC current month. For each source, the extraction used is the latest one **strictly before** the snapshot date.

# COMMAND ----------

first_snapshot_after = stpo_timestamps[0].date()
if SNAPSHOT_FROM_DATE:
    first_snapshot_after = max(
        first_snapshot_after,
        datetime.date.fromisoformat(SNAPSHOT_FROM_DATE) - datetime.timedelta(days=1),
    )
SNAPSHOT_DATES = month_starts(first_snapshot_after, datetime.date.today())
log.info(f"{len(SNAPSHOT_DATES)} snapshot dates, from {SNAPSHOT_DATES[0]} to {SNAPSHOT_DATES[-1]}")

df_map_stpo = build_snapshot_mapping(stpo_timestamps, SNAPSHOT_DATES)
df_map_stko = build_snapshot_mapping(stko_timestamps, SNAPSHOT_DATES)
df_map_stas = build_snapshot_mapping(stas_timestamps, SNAPSHOT_DATES, backdate_first=True)
df_map_mast = build_snapshot_mapping(mast_timestamps, SNAPSHOT_DATES, backdate_first=True)
df_map_marc = build_snapshot_mapping(marc_timestamps, SNAPSHOT_DATES, backdate_first=True)
df_map_marm = build_snapshot_mapping(marm_timestamps, SNAPSHOT_DATES, backdate_first=True)

# COMMAND ----------

def select_extractions(df_raw, df_map):
    """Keep only the extractions used by the calendar (partition pruning), tagged with their snapshot date(s).
    One extraction can serve several snapshot dates when an extraction week is missing: intended."""
    used_timestamps = [r["extraction_timestamp"] for r in df_map.select("extraction_timestamp").distinct().collect()]
    return df_raw.filter(f.col("extraction_timestamp").isin(used_timestamps)).join(
        f.broadcast(df_map), ["extraction_timestamp"], how="inner"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep2 - STPO (BOM items)
# MAGIC Main table: component, quantity, unit, component scrap, fixed quantity, bulk material, item category.
# MAGIC Items not yet valid at the snapshot date or flagged deleted are dropped (both never happen today, kept as
# MAGIC guards). `vgknt` keeps the link to the node replaced by an engineering change.

# COMMAND ----------

STPO_COLUMNS = [
    "snapshot_date",
    f.trim("stlnr").alias("BOM_number"),
    f.trim("stlkn").alias("BOM_node"),
    f.trim("posnr").alias("BOM_item_number"),
    f.trim("postp").alias("BOM_item_category"),
    f.trim("idnrk").alias("component_material_number"),
    sap_number("menge").alias("component_quantity"),
    f.trim("meins").alias("component_unit"),
    sap_number("ausch").alias("component_scrap_percentage"),
    sap_flag("netau").alias("is_net_scrap"),
    sap_flag("fmeng").alias("is_fixed_quantity"),
    sap_flag("schgt").alias("is_bulk_material"),
    sap_date("datuv").alias("item_valid_from_date"),
    f.trim("aennr").alias("item_change_number"),
    f.trim("vgknt").alias("previous_BOM_node"),
    sap_flag("lkenz").alias("is_item_deleted"),
    f.col("extraction_timestamp").alias("_BOM_extraction_timestamp"),
]

df_stpo_prep = (
    select_extractions(df_stpo_raw, df_map_stpo)
    .filter(f.trim("stlty") == BOM_CATEGORY)
    .select(*STPO_COLUMNS)
    .filter(f.col("item_valid_from_date").isNull() | (f.col("item_valid_from_date") < f.col("snapshot_date")))
    .filter(~f.col("is_item_deleted"))
    .drop("is_item_deleted")
)

df_stpo_prep = table_utils.remove_leading_zeros(
    df=df_stpo_prep, column_names=["component_material_number"]
)

df_gx_stpo = SparkDFDataset(df_stpo_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep3 - STAS (item allocation to an alternative)
# MAGIC STAS says which node belongs to which alternative. An engineering change writes a new record with
# MAGIC `lkenz = 'X'` to end the old node: per (alternative, node) we keep the latest record (validity date, then
# MAGIC counter) known at the snapshot date and drop it if it is a deletion. Records created after the snapshot date
# MAGIC are ignored (matters for back-dated snapshots before 2024-06-11).

# COMMAND ----------

STAS_COLUMNS = [
    "snapshot_date",
    "_is_backdated",
    f.trim("stlnr").alias("BOM_number"),
    f.trim("stlal").alias("BOM_alternative"),
    f.trim("stlkn").alias("BOM_node"),
    f.trim("stasz").alias("allocation_counter"),
    sap_date("datuv").alias("allocation_valid_from_date"),
    sap_date("andat").alias("allocation_created_date"),
    sap_flag("lkenz").alias("is_allocation_deleted"),
]

window_stas = Window.partitionBy("snapshot_date", "BOM_number", "BOM_alternative", "BOM_node").orderBy(
    f.desc_nulls_last("allocation_valid_from_date"), f.desc("allocation_counter")
)

df_stas_prep = (
    select_extractions(df_stas_raw, df_map_stas)
    .filter(f.trim("stlty") == BOM_CATEGORY)
    .select(*STAS_COLUMNS)
    .filter(f.coalesce(f.col("allocation_created_date") < f.col("snapshot_date"), f.lit(True)))
    .filter(f.coalesce(f.col("allocation_valid_from_date") < f.col("snapshot_date"), f.lit(True)))
    .withColumn("rn", f.row_number().over(window_stas))
    .filter("rn = 1")
    .filter(~f.col("is_allocation_deleted"))
    .select("snapshot_date", "BOM_number", "BOM_alternative", "BOM_node", "_is_backdated")
    .withColumnRenamed("_is_backdated", "_is_backdated_allocation")
)

df_gx_stas = SparkDFDataset(df_stas_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep4 - STKO (BOM header per alternative)
# MAGIC Base quantity (component quantities are given for `BOM_base_quantity` units of the AF, 3.5 % of BOMs have
# MAGIC a base quantity other than 1), status. Latest header version per alternative, deleted headers dropped.

# COMMAND ----------

STKO_COLUMNS = [
    "snapshot_date",
    f.trim("stlnr").alias("BOM_number"),
    f.trim("stlal").alias("BOM_alternative"),
    f.trim("stkoz").alias("header_counter"),
    sap_date("datuv").alias("header_valid_from_date"),
    sap_number("bmeng").alias("BOM_base_quantity"),
    f.trim("bmein").alias("BOM_base_unit"),
    f.trim("stlst").alias("BOM_status"),
    sap_flag("lkenz").alias("is_header_deleted"),
]

window_stko = Window.partitionBy("snapshot_date", "BOM_number", "BOM_alternative").orderBy(
    f.desc_nulls_last("header_valid_from_date"), f.desc("header_counter")
)

df_stko_prep = (
    select_extractions(df_stko_raw, df_map_stko)
    .filter(f.trim("stlty") == BOM_CATEGORY)
    .select(*STKO_COLUMNS)
    .filter(f.coalesce(f.col("header_valid_from_date") < f.col("snapshot_date"), f.lit(True)))
    .withColumn("rn", f.row_number().over(window_stko))
    .filter("rn = 1")
    .filter(~f.col("is_header_deleted"))
    .drop("rn", "header_counter", "header_valid_from_date", "is_header_deleted")
)

df_gx_stko = SparkDFDataset(df_stko_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep5 - MAST (manufactured article -> BOM)
# MAGIC Gives plant and AF of each BOM. Production usage only; 80 links without plant are dropped. Back-dated
# MAGIC snapshots only keep links created before the snapshot date.

# COMMAND ----------

MAST_COLUMNS = [
    "snapshot_date",
    "_is_backdated",
    f.trim("matnr").alias("material_number"),
    f.trim("werks").alias("plant"),
    f.trim("stlan").alias("BOM_usage"),
    f.trim("stlnr").alias("BOM_number"),
    f.trim("stlal").alias("BOM_alternative"),
    sap_number("losvn").alias("lot_size_from_quantity"),
    sap_number("losbs").alias("lot_size_to_quantity"),
    sap_date("andat").alias("BOM_link_created_date"),
]

df_mast_prep = (
    select_extractions(df_mast_raw, df_map_mast)
    .select(*MAST_COLUMNS)
    .filter(f.col("BOM_usage").isin(BOM_USAGES))
    .filter(f.col("plant").isNotNull() & (f.col("plant") != ""))
    .filter(f.coalesce(f.col("BOM_link_created_date") < f.col("snapshot_date"), f.lit(True)))
    .drop("BOM_link_created_date")
    .withColumnRenamed("_is_backdated", "_is_backdated_link")
)

if DEBUG_PLANT:
    df_mast_prep = df_mast_prep.filter(f.col("plant") == DEBUG_PLANT)

df_mast_prep = table_utils.remove_leading_zeros(df=df_mast_prep, column_names=["material_number"])

df_gx_mast = SparkDFDataset(df_mast_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep6 - MARC (article x plant: phantom and scrap)
# MAGIC - `sobsl = '50'`: the component is a phantom assembly (never consumed, exploded by the use case).
# MAGIC - `kausf`: component scrap of the article, used by SAP when the BOM item has no own scrap (350 articles).
# MAGIC - `ausss`: assembly scrap of the AF (32k articles) - the main scrap rate actually maintained.
# MAGIC Only `FULL` extractions (a `DELTA` extraction only holds the changes of the day).

# COMMAND ----------

MARC_COLUMNS = [
    "snapshot_date",
    f.trim("matnr").alias("material_number"),
    f.trim("werks").alias("plant"),
    f.trim("sobsl").alias("special_procurement_type"),
    sap_number("kausf").alias("material_component_scrap_percentage"),
    sap_number("ausss").alias("assembly_scrap_percentage"),
    f.col("ingestion_timestamp").alias("_ingestion_timestamp"),
    f.col("stack_row_id").alias("_stack_row_id"),
]

df_marc_prep = (
    select_extractions(df_marc_raw.filter(f.col("file_mode") == FULL_EXTRACTION), df_map_marc)
    .select(*MARC_COLUMNS)
)

df_marc_prep = table_utils.remove_leading_zeros(df=df_marc_prep, column_names=["material_number"])
df_marc_prep = keep_latest_row(df_marc_prep, ["snapshot_date", "material_number", "plant"])

df_gx_marc = SparkDFDataset(df_marc_prep, persist=False)

# COMMAND ----------

# Component side and AF side of MARC, renamed for the two joins
df_marc_component = df_marc_prep.select(
    "snapshot_date",
    f.col("material_number").alias("component_material_number"),
    "plant",
    f.col("special_procurement_type").alias("component_special_procurement_type"),
    "material_component_scrap_percentage",
)

df_marc_assembly = df_marc_prep.select(
    "snapshot_date", "material_number", "plant", "assembly_scrap_percentage"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep7 - Unit conversion
# MAGIC Reservations and goods movements are in the component base unit; 7.6 % of BOM items use another unit.
# MAGIC MARM gives the article-specific conversions (`1 alternative unit = umrez / umren base units`), the constant
# MAGIC `ISO_UNIT_FACTORS` the same-dimension ones (IN -> M is 96 % of the items without MARM record).

# COMMAND ----------

MARM_COLUMNS = [
    "snapshot_date",
    f.trim("matnr").alias("component_material_number"),
    f.trim("meinh").alias("component_unit"),
    sap_number("umrez").alias("unit_numerator"),
    sap_number("umren").alias("unit_denominator"),
    f.col("ingestion_timestamp").alias("_ingestion_timestamp"),
    f.col("stack_row_id").alias("_stack_row_id"),
]

df_marm_prep = (
    select_extractions(df_marm_raw.filter(f.col("file_mode") == FULL_EXTRACTION), df_map_marm)
    .select(*MARM_COLUMNS)
    .filter(f.col("unit_denominator") != 0)
    .withColumn("MARM_unit_factor", f.col("unit_numerator") / f.col("unit_denominator"))
    .drop("unit_numerator", "unit_denominator")
)

df_marm_prep = table_utils.remove_leading_zeros(df=df_marm_prep, column_names=["component_material_number"])
df_marm_prep = keep_latest_row(df_marm_prep, ["snapshot_date", "component_material_number", "component_unit"])

df_gx_marm = SparkDFDataset(df_marm_prep, persist=False)

# COMMAND ----------

df_base_unit_prep = df_material_raw.select(
    f.col("material_number").alias("component_material_number"),
    f.col("material_base_unit").alias("component_base_unit"),
).dropDuplicates(["component_material_number"])

df_gx_base_unit = SparkDFDataset(df_base_unit_prep, persist=False)

df_iso_unit_factors = spark.createDataFrame(
    ISO_UNIT_FACTORS, "component_unit string, component_base_unit string, ISO_unit_factor double"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 1 - Items -> alternatives (STPO x STAS)
# MAGIC INNER: an item without allocation belongs to no alternative (97 items out of 4.7 M today).

# COMMAND ----------

df_transf = df_stpo_prep.join(df_stas_prep, ["snapshot_date", "BOM_number", "BOM_node"], how="inner")
log.info("Joined STAS on snapshot_date/BOM_number/BOM_node - uniqueness of STAS key checked in Quality Checks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 2 - Header (STKO)

# COMMAND ----------

df_transf = df_transf.join(df_stko_prep, ["snapshot_date", "BOM_number", "BOM_alternative"], how="inner")
log.info("Joined STKO on snapshot_date/BOM_number/BOM_alternative - uniqueness checked in Quality Checks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 3 - Manufactured article and plant (MAST)
# MAGIC A BOM shared by several articles / plants (219 BOM numbers) is repeated for each of them: this is the grain,
# MAGIC not a fan-out.

# COMMAND ----------

df_transf = df_transf.join(df_mast_prep, ["snapshot_date", "BOM_number", "BOM_alternative"], how="inner")
log.info("Joined MAST on snapshot_date/BOM_number/BOM_alternative - PK uniqueness checked in Quality Checks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 4 - Phantom flag and scrap rates (MARC)

# COMMAND ----------

df_transf = df_transf.join(
    df_marc_component, ["snapshot_date", "component_material_number", "plant"], how="left"
)
df_transf = df_transf.join(df_marc_assembly, ["snapshot_date", "material_number", "plant"], how="left")
log.info("Joined MARC (component and AF) - key uniqueness checked in Quality Checks")

# COMMAND ----------

df_transf = df_transf.withColumn(
    "is_phantom_item",
    f.coalesce(f.col("component_special_procurement_type") == PHANTOM_SPECIAL_PROCUREMENT, f.lit(False)),
).drop("component_special_procurement_type")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 5 - Quantity in the component base unit
# MAGIC Same unit -> factor 1; else MARM factor of the article at the snapshot date; else ISO factor. NULL when no
# MAGIC conversion is known (AMBER check).

# COMMAND ----------

df_transf = df_transf.join(df_base_unit_prep, ["component_material_number"], how="left")
df_transf = df_transf.join(
    df_marm_prep, ["snapshot_date", "component_material_number", "component_unit"], how="left"
)
df_transf = df_transf.join(
    f.broadcast(df_iso_unit_factors), ["component_unit", "component_base_unit"], how="left"
)

df_transf = df_transf.withColumn(
    "component_quantity_in_base_unit",
    f.col("component_quantity")
    * f.when(f.col("component_unit") == f.col("component_base_unit"), f.lit(1.0)).otherwise(
        f.coalesce(f.col("MARM_unit_factor"), f.col("ISO_unit_factor"))
    ),
).drop("MARM_unit_factor", "ISO_unit_factor")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 6 - Create ID column and put it first

# COMMAND ----------

PK_COL = "bom_item_history_ID"

df_transf = df_transf.withColumn(
    PK_COL,
    f.concat_ws(
        "-",
        f.date_format("snapshot_date", "yyyyMMdd"),
        "plant",
        "material_number",
        "BOM_usage",
        "BOM_alternative",
        "BOM_number",
        "BOM_node",
    ),
)

OUTPUT_COLUMNS = [
    PK_COL,
    "snapshot_date",
    "plant",
    "material_number",
    "BOM_usage",
    "BOM_alternative",
    "BOM_number",
    "BOM_node",
    "BOM_item_number",
    "BOM_item_category",
    "component_material_number",
    "component_quantity",
    "component_unit",
    "component_quantity_in_base_unit",
    "component_base_unit",
    "BOM_base_quantity",
    "BOM_base_unit",
    "BOM_status",
    "component_scrap_percentage",
    "material_component_scrap_percentage",
    "assembly_scrap_percentage",
    "is_net_scrap",
    "is_fixed_quantity",
    "is_phantom_item",
    "is_bulk_material",
    "item_valid_from_date",
    "item_change_number",
    "previous_BOM_node",
    "lot_size_from_quantity",
    "lot_size_to_quantity",
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
    df_gx_stpo.expect_compound_columns_to_be_unique(["snapshot_date", "BOM_number", "BOM_node"]),
    df_gx_stas.expect_compound_columns_to_be_unique(
        ["snapshot_date", "BOM_number", "BOM_alternative", "BOM_node"]
    ),
    df_gx_stko.expect_compound_columns_to_be_unique(["snapshot_date", "BOM_number", "BOM_alternative"]),
    df_gx_mast.expect_compound_columns_to_be_unique(
        ["snapshot_date", "material_number", "plant", "BOM_usage", "BOM_number", "BOM_alternative"]
    ),
    df_gx_marc.expect_compound_columns_to_be_unique(["snapshot_date", "material_number", "plant"]),
    df_gx_marm.expect_compound_columns_to_be_unique(
        ["snapshot_date", "component_material_number", "component_unit"]
    ),
    df_gx_base_unit.expect_column_values_to_be_unique(column="component_material_number"),
]

gx_validation.validate_and_log_gx_results(
    quality_check_results=red_quality_check_results,
    validation_level=validation_level,
    info_message="All checks inspection",
    error_message="Quality checks failed for bom_item_history table, please check log messages",
    pipeline_write_env=PIPELINE_WRITE_ENV,
    table_name="bom_item_history",
    log_to_event_table=True,
    log_successes=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC AMBER: key dimensions filled; quantity in base unit known (53k items had no conversion before the ISO factors,
# MAGIC the remaining NULLs are listed by the warning).

# COMMAND ----------

amber_quality_check_results = [
    df_gx_final.expect_column_values_to_not_be_null(column="component_material_number", mostly=0.97),
    df_gx_final.expect_column_values_to_not_be_null(column="component_quantity"),
    df_gx_final.expect_column_values_to_not_be_null(column="BOM_base_quantity"),
    df_gx_final.expect_column_values_to_not_be_null(column="component_quantity_in_base_unit", mostly=0.99),
]

gx_validation.validate_and_log_gx_results(
    quality_check_results=amber_quality_check_results,
    validation_level="AMBER",
    info_message="Key dimensions and base-unit quantities populated",
    error_message="Missing component / quantity / unit conversion - check STPO units and MARM",
    pipeline_write_env=PIPELINE_WRITE_ENV,
    table_name="bom_item_history",
    log_to_event_table=True,
    log_successes=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Outputs

# COMMAND ----------

if LAB_TARGET_SCHEMA:
    CATALOG, SCHEMA = LAB_TARGET_SCHEMA.split(".")
else:
    CATALOG = f"{PIPELINE_WRITE_ENV}_gold"
    SCHEMA = "production"
DESTINATION_TABLE = "bom_item_history"
DESTINATION_TABLE_FULL_PATH = f"{CATALOG}.{SCHEMA}.{DESTINATION_TABLE}"

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{SCHEMA}")

try:
    startTime = time.time()
    result = table_utils.save_table(
        dest_table=DESTINATION_TABLE_FULL_PATH,
        df=df_transf,
        mode="overwrite",
        overwrite_schema=True,
    )
except AnalysisException as e:
    log.error(
        f"Table {DESTINATION_TABLE_FULL_PATH} has not been created, an error occured during saveAsTable. \nError is {e}"
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
    f"and table_name='{DESTINATION_TABLE}' and constraint_name='{TABLE_CONSTRAINT_NAME}'"
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
