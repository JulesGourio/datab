# Databricks notebook source
# MAGIC %md
# MAGIC # GOLD ORDER COMPONENT REQUIREMENT HISTORY
# MAGIC
# MAGIC **Description:**
# MAGIC Monthly history of the **open** component requirements of every planned order (OP, dependent requirements
# MAGIC `SB`) and production order (OF, reservations `AR`), as SAP knew them at the start of each month: one row per
# MAGIC snapshot date x reservation item, with requirement quantity (scrap included), BOM quantity (scrap excluded),
# MAGIC order header (article, quantity, dates, BOM alternative) and the planned order -> production order link.
# MAGIC It is the MRP forecast (Prévisions 1 and 2) of the BOM reliability use case.
# MAGIC
# MAGIC **Highlighted complexities:**
# MAGIC - Source is `resb_stack` (~59 bn rows, one extraction per `extraction_timestamp`, weekly until 2024-09 then
# MAGIC   daily). A full RESB extraction is split into two file families: `SAP-RESB-F-ACT` (open and closed lines)
# MAGIC   and `SAP-RESB-F-NOACT` (almost only open lines). Some `extraction_timestamp` values hold only one family:
# MAGIC   taking a single timestamp then misses half of the open requirements. For each snapshot date we therefore take,
# MAGIC   **per file family**, the latest extraction before the date that holds it, and union both (a reservation
# MAGIC   present in both keeps its most recent version). A snapshot missing a family in the window is dropped (logged).
# MAGIC - "Open" = `bdart` AR/SB, not deleted (`xloek`), not finally issued (`kzear`), nothing withdrawn (`enmng = 0`),
# MAGIC   requirement date within `MAX_HORIZON_MONTHS` (planned orders run years ahead: 20 M lines beyond 13 months).
# MAGIC - Order headers (PLAF for OP, AFKO/AFPO for OF) come from their latest extraction not after the RESB one.
# MAGIC - Numbers are SAP text with a trailing minus; order / material numbers carry leading zeros in the stacks.
# MAGIC - Phantom lines (`dumps = 'X'`) are kept and flagged: their components are already exploded by SAP in the
# MAGIC   following lines (`baugr` = phantom article).
# MAGIC
# MAGIC **Intended Pipeline**
# MAGIC - M_2_MRP_Order_Requirement_History_Data_Asset (to be confirmed)
# MAGIC
# MAGIC **Inputs Data**
# MAGIC - {REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6.resb_stack
# MAGIC - {REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6.plaf_stack
# MAGIC - {REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6.afko_stack
# MAGIC - {REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6.afpo_stack
# MAGIC
# MAGIC **Output Tables (Pipeline)**
# MAGIC - {PIPELINE_WRITE_ENV}_gold.mrp.order_component_requirement_history
# MAGIC - {PIPELINE_WRITE_ENV}_gold.mrp.order_component_requirement_history_exposed (view)

# COMMAND ----------

# MAGIC %md
# MAGIC # Technical debt
# MAGIC - Landing zone stacks are read directly from Gold (no Bronze history table exists).
# MAGIC - `sap_number()`, `month_starts()` are duplicated from `create_gold_bom_item_history`: move to `leap_utils`
# MAGIC   before the Bitbucket PR.
# MAGIC - The RESB file family is read from `file_name` (`SAP-RESB-F-ACT` / `SAP-RESB-F-NOACT`): a change of file
# MAGIC   naming at the source breaks the snapshot selection (files without family are treated as a full extraction).
# MAGIC - When the two families come from different extractions (logged), a line closed in the newer one but still
# MAGIC   open in the older one is kept as open (the open filter runs before the deduplication, to keep the volume low).
# MAGIC - `component_BOM_quantity` (`esmng`) is taken as the requirement without component scrap (checked on samples:
# MAGIC   `bdmng` = `esmng` x (1 + `ausch`) rounded up). To be confirmed on planned orders (`SB`).
# MAGIC - `order_scrap_quantity` of planned orders uses `plaf.avmng` (to be confirmed).
# MAGIC - MARC / MARM (and PLAF / AFKO in the requirement history) contain a few repeated rows in some extractions
# MAGIC   (e.g. article F5391312700300 / plant 1900 in the extraction used for 2023-05 and 2023-06):
# MAGIC   `keep_latest_row()` keeps one. The repeat is inside the SAP extract file (identical values), not the ingestion.

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

REQUIREMENT_TYPES = {"AR": "WORK_ORDER", "SB": "PLANNED_ORDER"}  # bdart -> order category
MAX_HORIZON_MONTHS = 13  # longest analysed period P1 + P2 is 12 months
RESB_FILE_FAMILIES = ["ACT", "NOACT"]  # a full RESB extraction = both file families
CANDIDATE_MAX_AGE_DAYS = 35  # RESB extractions considered before each snapshot date (weekly until 2024-09)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Helper Functions
# MAGIC Same helpers as `create_gold_bom_item_history` (see Technical debt).

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


def latest_before(extraction_timestamps, limit_timestamp):
    eligible = [ts for ts in extraction_timestamps if ts <= limit_timestamp]
    return max(eligible) if eligible else None

# COMMAND ----------

# MAGIC %md
# MAGIC # Inputs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import LEAP tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Landing zone stacks

# COMMAND ----------

df_resb_raw = spark.read.table(f"{LANDING_ZONE_SCHEMA}.resb_stack")
df_plaf_raw = spark.read.table(f"{LANDING_ZONE_SCHEMA}.plaf_stack")
df_afko_raw = spark.read.table(f"{LANDING_ZONE_SCHEMA}.afko_stack")
df_afpo_raw = spark.read.table(f"{LANDING_ZONE_SCHEMA}.afpo_stack")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extraction calendars

# COMMAND ----------

resb_timestamps = list_partition_timestamps(f"{LANDING_ZONE_SCHEMA}.resb_stack")
plaf_timestamps = list_partition_timestamps(f"{LANDING_ZONE_SCHEMA}.plaf_stack")
afko_timestamps = list_partition_timestamps(f"{LANDING_ZONE_SCHEMA}.afko_stack")
afpo_timestamps = list_partition_timestamps(f"{LANDING_ZONE_SCHEMA}.afpo_stack")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep1 - Open requirement filter
# MAGIC Applied as early as possible: it turns ~75 M RESB lines per extraction into ~18 M.

# COMMAND ----------

def open_requirements(df_resb):
    return (
        df_resb.filter(f.trim("bdart").isin(list(REQUIREMENT_TYPES)))
        .filter(~sap_flag("xloek"))
        .filter(~sap_flag("kzear"))
        .filter(f.coalesce(sap_number("enmng"), f.lit(0.0)) == 0)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep2 - Snapshot calendar and choice of the RESB extractions
# MAGIC Snapshot dates = first day of each month after the first RESB extraction. Candidates = the RESB extractions of
# MAGIC the `CANDIDATE_MAX_AGE_DAYS` days strictly before the snapshot date. Their file families are read from
# MAGIC `file_name` (candidate partitions only). For each family, the latest candidate holding it is kept.

# COMMAND ----------

RESB_FILE_FAMILY = f.regexp_extract(f.col("file_name"), "SAP-RESB-F-(NOACT|ACT)", 1)

first_snapshot_after = resb_timestamps[0].date()
if SNAPSHOT_FROM_DATE:
    first_snapshot_after = max(
        first_snapshot_after,
        datetime.date.fromisoformat(SNAPSHOT_FROM_DATE) - datetime.timedelta(days=1),
    )
SNAPSHOT_DATES = month_starts(first_snapshot_after, datetime.date.today())


def candidates_before(snapshot_date):
    return [
        ts
        for ts in resb_timestamps
        if snapshot_date - datetime.timedelta(days=CANDIDATE_MAX_AGE_DAYS) <= ts.date() < snapshot_date
    ]


candidate_timestamps = sorted({ts for d in SNAPSHOT_DATES for ts in candidates_before(d)})

families_per_extraction = {}
for r in (
    df_resb_raw.filter(f.col("extraction_timestamp").isin(candidate_timestamps))
    .select("extraction_timestamp", RESB_FILE_FAMILY.alias("resb_file_family"))
    .distinct()
    .collect()
):
    families_per_extraction.setdefault(r["extraction_timestamp"], set()).add(r["resb_file_family"])

# COMMAND ----------

resb_rows = set()  # (snapshot_date, extraction_timestamp, resb_file_family as found in file_name)
mapping_rows = []
for snapshot_date in SNAPSHOT_DATES:
    chosen = {}
    for family in RESB_FILE_FAMILIES:
        # an extraction whose files carry no family ("") is taken as a full extraction
        holding = [
            ts
            for ts in candidates_before(snapshot_date)
            if family in families_per_extraction.get(ts, set()) or "" in families_per_extraction.get(ts, set())
        ]
        if holding:
            chosen[family] = max(holding)
    if len(chosen) < len(RESB_FILE_FAMILIES):
        log.warning(
            f"Snapshot {snapshot_date} dropped: RESB file families found {sorted(chosen)} "
            f"in the {CANDIDATE_MAX_AGE_DAYS} days before"
        )
        continue
    for family, ts in chosen.items():
        resb_rows.add((snapshot_date, ts, family if family in families_per_extraction[ts] else ""))
    latest_resb_ts = max(chosen.values())
    if len(set(chosen.values())) > 1:
        log.info(f"Snapshot {snapshot_date}: RESB file families taken from different extractions {chosen}")
    mapping_rows.append(
        (
            snapshot_date,
            latest_resb_ts,
            latest_before(plaf_timestamps, latest_resb_ts),
            latest_before(afko_timestamps, latest_resb_ts),
            latest_before(afpo_timestamps, latest_resb_ts),
        )
    )

df_snapshot_map = spark.createDataFrame(
    mapping_rows, "snapshot_date date, resb_ts timestamp, plaf_ts timestamp, afko_ts timestamp, afpo_ts timestamp"
)
df_resb_map = spark.createDataFrame(
    sorted(resb_rows), "snapshot_date date, extraction_timestamp timestamp, resb_file_family string"
)
log.info(f"{len(mapping_rows)} snapshots kept out of {len(SNAPSHOT_DATES)}")

# COMMAND ----------

MAPPING_INDEX = {"resb_ts": 1, "plaf_ts": 2, "afko_ts": 3, "afpo_ts": 4}


def select_extractions(df_raw, timestamp_column):
    """Extractions used by the calendar (partition pruning), tagged with their snapshot date(s)."""
    df_map = df_snapshot_map.select("snapshot_date", f.col(timestamp_column).alias("extraction_timestamp"))
    used_timestamps = sorted(
        {row[MAPPING_INDEX[timestamp_column]] for row in mapping_rows if row[MAPPING_INDEX[timestamp_column]]}
    )
    return df_raw.filter(f.col("extraction_timestamp").isin(used_timestamps)).join(
        f.broadcast(df_map), ["extraction_timestamp"], how="inner"
    )


def select_resb_extractions():
    """RESB rows of the chosen (extraction, file family) pairs, tagged with their snapshot date(s)."""
    used_timestamps = sorted({row[1] for row in resb_rows})
    return (
        df_resb_raw.filter(f.col("extraction_timestamp").isin(used_timestamps))
        .withColumn("resb_file_family", RESB_FILE_FAMILY)
        .join(f.broadcast(df_resb_map), ["extraction_timestamp", "resb_file_family"], how="inner")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep3 - RESB (open requirements)
# MAGIC - `bdmng`: requirement quantity, component scrap included and rounded up by SAP -> Prévision 1.
# MAGIC - `esmng`: quantity before component scrap -> Prévision 2.
# MAGIC - `stlnr/stlkn/stpoz`: BOM item the line comes from (empty = component added by hand on the order).

# COMMAND ----------

RESB_COLUMNS = [
    "snapshot_date",
    f.trim("rsnum").alias("reservation_number"),
    f.trim("rspos").alias("reservation_item"),
    f.coalesce(f.trim("rsart"), f.lit("")).alias("reservation_record_type"),
    f.trim("bdart").alias("requirement_type"),
    f.trim("aufnr").alias("work_order_number"),
    f.trim("plnum").alias("planned_order_number"),
    f.trim("matnr").alias("component_material_number"),
    f.trim("werks").alias("plant"),
    sap_number("bdmng").alias("requirement_quantity"),
    sap_number("esmng").alias("component_BOM_quantity"),
    f.trim("meins").alias("component_base_unit"),
    sap_date("bdter").alias("requirement_date"),
    sap_number("ausch").alias("component_scrap_percentage"),
    sap_flag("dumps").alias("is_phantom_item"),
    f.trim("baugr").alias("higher_level_assembly"),
    sap_flag("schgt").alias("is_bulk_material"),
    f.trim("postp").alias("BOM_item_category"),
    f.trim("posnr").alias("BOM_item_number"),
    f.trim("stlnr").alias("BOM_number"),
    f.trim("stlkn").alias("BOM_node"),
    f.trim("vornr").alias("operation_number"),
    f.col("extraction_timestamp").alias("_RESB_extraction_timestamp"),
    f.col("stack_row_id").alias("_stack_row_id"),
]

# A reservation open in both chosen extractions keeps its most recent version
window_resb = Window.partitionBy(
    "snapshot_date", "reservation_number", "reservation_item", "reservation_record_type"
).orderBy(f.desc("_RESB_extraction_timestamp"), f.desc("_stack_row_id"))

df_resb_prep = (
    open_requirements(select_resb_extractions())
    .select(*RESB_COLUMNS)
    .withColumn("rn", f.row_number().over(window_resb))
    .filter("rn = 1")
    .drop("rn", "_stack_row_id")
    .filter(f.col("requirement_date") <= f.add_months(f.col("snapshot_date"), MAX_HORIZON_MONTHS))
    .withColumn("order_category", f.col("requirement_type"))
    .replace(REQUIREMENT_TYPES, subset=["order_category"])
)

if DEBUG_PLANT:
    df_resb_prep = df_resb_prep.filter(f.col("plant") == DEBUG_PLANT)

df_resb_prep = table_utils.remove_leading_zeros(
    df=df_resb_prep,
    column_names=["work_order_number", "planned_order_number", "component_material_number", "higher_level_assembly"],
)

df_gx_resb = SparkDFDataset(df_resb_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep4 - PLAF (planned order header)
# MAGIC Article, quantity, dates and BOM alternative of each planned order at the snapshot date.

# COMMAND ----------

PLAF_COLUMNS = [
    "snapshot_date",
    f.trim("plnum").alias("planned_order_number"),
    f.trim("matnr").alias("material_number"),
    sap_number("gsmng").alias("order_quantity"),
    sap_number("avmng").alias("order_scrap_quantity"),
    sap_date("psttr").alias("order_planned_start_date"),
    sap_date("pedtr").alias("order_planned_end_date"),
    f.trim("paart").alias("order_type"),
    f.trim("stlan").alias("BOM_usage"),
    f.trim("stlal").alias("BOM_alternative"),
    f.trim("verid").alias("production_version"),
    f.col("ingestion_timestamp").alias("_ingestion_timestamp"),
    f.col("stack_row_id").alias("_stack_row_id"),
]

df_plaf_prep = select_extractions(df_plaf_raw, "plaf_ts").select(*PLAF_COLUMNS)

df_plaf_prep = table_utils.remove_leading_zeros(
    df=df_plaf_prep, column_names=["planned_order_number", "material_number"]
)
df_plaf_prep = keep_latest_row(df_plaf_prep, ["snapshot_date", "planned_order_number"])

df_gx_plaf = SparkDFDataset(df_plaf_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep5 - AFKO + AFPO (production order header)
# MAGIC AFKO: quantities, scheduled dates, BOM alternative. AFPO: article and the planned order the production order
# MAGIC was converted from (`plnum`). AFPO is reduced to its first item per order.

# COMMAND ----------

AFKO_COLUMNS = [
    "snapshot_date",
    f.trim("aufnr").alias("work_order_number"),
    sap_number("gamng").alias("order_quantity"),
    sap_number("gasmg").alias("order_scrap_quantity"),
    sap_date("gstrp").alias("order_planned_start_date"),
    sap_date("gltrp").alias("order_planned_end_date"),
    f.trim("stlan").alias("BOM_usage"),
    f.trim("stlal").alias("BOM_alternative"),
    f.col("ingestion_timestamp").alias("_ingestion_timestamp"),
    f.col("stack_row_id").alias("_stack_row_id"),
]

df_afko_prep = keep_latest_row(
    select_extractions(df_afko_raw, "afko_ts").select(*AFKO_COLUMNS), ["snapshot_date", "work_order_number"]
)

AFPO_COLUMNS = [
    "snapshot_date",
    f.trim("aufnr").alias("work_order_number"),
    f.trim("posnr").alias("work_order_item"),
    f.trim("matnr").alias("material_number"),
    f.trim("plnum").alias("origin_planned_order_number"),
    f.trim("verid").alias("production_version"),
]

window_afpo = Window.partitionBy("snapshot_date", "work_order_number").orderBy("work_order_item")

df_afpo_prep = (
    select_extractions(df_afpo_raw, "afpo_ts")
    .select(*AFPO_COLUMNS)
    .withColumn("rn", f.row_number().over(window_afpo))
    .filter("rn = 1")
    .drop("rn", "work_order_item")
)

df_work_order_header_prep = df_afko_prep.join(df_afpo_prep, ["snapshot_date", "work_order_number"], how="left")

df_work_order_header_prep = table_utils.remove_leading_zeros(
    df=df_work_order_header_prep,
    column_names=["work_order_number", "material_number", "origin_planned_order_number"],
)

df_gx_afko = SparkDFDataset(df_afko_prep, persist=False)
df_gx_work_order_header = SparkDFDataset(df_work_order_header_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 1 - Production order requirements + header

# COMMAND ----------

df_work_order_requirements = df_resb_prep.filter(f.col("requirement_type") == "AR").join(
    df_work_order_header_prep, ["snapshot_date", "work_order_number"], how="left"
)
log.info("Joined OF header on snapshot_date/work_order_number - header uniqueness checked in Quality Checks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 2 - Planned order requirements + header

# COMMAND ----------

df_planned_order_requirements = (
    df_resb_prep.filter(f.col("requirement_type") == "SB")
    .join(df_plaf_prep, ["snapshot_date", "planned_order_number"], how="left")
)
log.info("Joined PLAF on snapshot_date/planned_order_number - header uniqueness checked in Quality Checks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 3 - Union of both order categories
# MAGIC Columns specific to one category (`origin_planned_order_number` for OF, `order_type` for OP) are NULL for
# MAGIC the other. The OF order type is in `work_orders_sap_exposed`.

# COMMAND ----------

df_transf = df_work_order_requirements.unionByName(df_planned_order_requirements, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 4 - Create ID column and put it first

# COMMAND ----------

PK_COL = "order_component_requirement_history_ID"

df_transf = df_transf.withColumn(
    PK_COL,
    f.concat_ws(
        "-",
        f.date_format("snapshot_date", "yyyyMMdd"),
        "reservation_number",
        "reservation_item",
        "reservation_record_type",
    ),
)

OUTPUT_COLUMNS = [
    PK_COL,
    "snapshot_date",
    "order_category",
    "requirement_type",
    "work_order_number",
    "planned_order_number",
    "origin_planned_order_number",
    "plant",
    "material_number",
    "order_type",
    "order_quantity",
    "order_scrap_quantity",
    "order_planned_start_date",
    "order_planned_end_date",
    "BOM_usage",
    "BOM_alternative",
    "production_version",
    "reservation_number",
    "reservation_item",
    "reservation_record_type",
    "component_material_number",
    "requirement_quantity",
    "component_BOM_quantity",
    "component_base_unit",
    "requirement_date",
    "component_scrap_percentage",
    "is_phantom_item",
    "higher_level_assembly",
    "is_bulk_material",
    "BOM_item_category",
    "BOM_item_number",
    "BOM_number",
    "BOM_node",
    "operation_number",
    "_RESB_extraction_timestamp",
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
    df_gx_final.expect_column_values_to_not_be_null(column=PK_COL),
    df_gx_final.expect_column_values_to_be_unique(column=PK_COL),
    df_gx_resb.expect_compound_columns_to_be_unique(
        ["snapshot_date", "reservation_number", "reservation_item", "reservation_record_type"]
    ),
    df_gx_plaf.expect_compound_columns_to_be_unique(["snapshot_date", "planned_order_number"]),
    df_gx_afko.expect_compound_columns_to_be_unique(["snapshot_date", "work_order_number"]),
    df_gx_work_order_header.expect_compound_columns_to_be_unique(["snapshot_date", "work_order_number"]),
]

gx_validation.validate_and_log_gx_results(
    quality_check_results=red_quality_check_results,
    validation_level=validation_level,
    info_message="All checks inspection",
    error_message="Quality checks failed for order_component_requirement_history table, please check log messages",
    pipeline_write_env=PIPELINE_WRITE_ENV,
    table_name="order_component_requirement_history",
    log_to_event_table=True,
    log_successes=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC AMBER: header found for each requirement, quantities filled.

# COMMAND ----------

amber_quality_check_results = [
    df_gx_final.expect_column_values_to_not_be_null(column="material_number", mostly=0.99),
    df_gx_final.expect_column_values_to_not_be_null(column="order_quantity", mostly=0.99),
    df_gx_final.expect_column_values_to_not_be_null(column="requirement_quantity"),
    df_gx_final.expect_column_values_to_not_be_null(column="component_BOM_quantity", mostly=0.99),
    df_gx_final.expect_column_values_to_not_be_null(column="component_material_number", mostly=0.99),
]

gx_validation.validate_and_log_gx_results(
    quality_check_results=amber_quality_check_results,
    validation_level="AMBER",
    info_message="Order headers and quantities populated",
    error_message="Missing order header or quantity - check PLAF/AFKO extraction alignment",
    pipeline_write_env=PIPELINE_WRITE_ENV,
    table_name="order_component_requirement_history",
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
    SCHEMA = "mrp"
DESTINATION_TABLE = "order_component_requirement_history"
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
