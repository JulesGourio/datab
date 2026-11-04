# Databricks notebook source
# MAGIC %md
# MAGIC # PROJ BOM AVERAGE MULTILEVEL
# MAGIC
# MAGIC **Description:**
# MAGIC Multilevel average BOM per ISM (material_number / plant). The structure and the quantities come from the
# MAGIC **average BOM** built on finished work orders (`manuf_engineering.average_bom`), not from the standard BOM.
# MAGIC Each run explodes one snapshot of the average BOM and appends it to a history table, so a backtest can use
# MAGIC the average BOM as it was known at a past date.
# MAGIC
# MAGIC **Highlighted complexities:**
# MAGIC - Single-level edges (material, plant, component, quantity) are read from `average_bom`; the multilevel quantity is the product of the edge quantities along the path.
# MAGIC - `snapshot_date` widget: empty = latest snapshot available for the horizon; a date = that exact snapshot. `current_average_bom` is only refreshed when the widget is empty.
# MAGIC - Component explosion follows the plant switch (`plant_buy` of the component in the material master).
# MAGIC - Fixed-quantity items (`STPO_fmeng`) are not handled any more: the average already comes from real work orders.
# MAGIC
# MAGIC **Intended Pipeline**
# MAGIC - BOM forecast pipeline (runs after `create_proj_average_bom`, before `create_proj_bom_forecast`)
# MAGIC
# MAGIC **Inputs Data**
# MAGIC - {PIPELINE_READ_ENV}_proj.manuf_engineering.average_bom
# MAGIC - {REFERENCE_READ_ENV}_gold.master_data.material_plant_exposed
# MAGIC - {REFERENCE_READ_ENV}_proj.manuf_engineering.mod_effectivity
# MAGIC
# MAGIC **Output Tables (Pipeline)**
# MAGIC - {PIPELINE_WRITE_ENV}_proj.supply_chain.average_bom_multilevel_history
# MAGIC - {PIPELINE_WRITE_ENV}_proj.supply_chain.current_average_bom
# MAGIC
# MAGIC **Calculated columns (business meaning)**
# MAGIC - `quantity`: average quantity of `component` needed for **one** unit of `material_number`, all levels multiplied.
# MAGIC - `level`: depth of the component in the BOM (0 = the ISM itself, 1 = direct component).
# MAGIC - `path`: ordered list of materials from the ISM down to the component.
# MAGIC - `type`: `WO` if the component is made in-house (IS...M), `PO` otherwise; drives which lead time is used.
# MAGIC - `branch_flag`: `X` when the component sits below a phantom-like purchased-in-house parent (procurement type F / special type 20).
# MAGIC - `snapshot_date`: date the average BOM is valid for; `horizon`: number of months of finished work orders behind it.

# COMMAND ----------

# MAGIC %md
# MAGIC # Technical debt
# MAGIC - `quantity_column` defaults to `Avg_Qty_WO_total` (as planned). Missing / over / extra consumption (`Qty_Miss`, `Qty_Over`, `Qty_Extra`) are **not** added: to be confirmed with supply.
# MAGIC - Past snapshots are an approximation: `average_bom` reads the BOM / production version / material master in their current state.
# MAGIC - No input quality checks yet (section still TBD in the guidelines).
# MAGIC - Folder `proj/supply_chain` and table names are to be confirmed (no sibling use case yet in this repo).

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Standard Package Imports

# COMMAND ----------

import time
from functools import reduce

from pyspark.sql import functions as f
from pyspark.sql.utils import AnalysisException

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

# Horizon (months of finished work orders) of the average BOM to explode
dbutils.widgets.text("horizon", "12")
HORIZON = int(dbutils.widgets.get("horizon"))

# yyyy-MM-dd snapshot of average_bom to explode, empty = latest available
dbutils.widgets.text("snapshot_date", "")
SNAPSHOT_DATE_PARAM = dbutils.widgets.get("snapshot_date")

# Column of average_bom used as quantity per unit
dbutils.widgets.text("quantity_column", "Avg_Qty_WO_total")
QUANTITY_COLUMN = dbutils.widgets.get("quantity_column")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Debug Boolean

# COMMAND ----------

dbutils.widgets.text("debug", "False")
DEBUG = dbutils.widgets.get("debug").lower() == "true"

dbutils.widgets.dropdown(
    "log_level",
    defaultValue="info",
    choices=["debug", "info", "warning", "error", "critical"],
)
LOG_LEVEL = dbutils.widgets.get("log_level")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config logger

# COMMAND ----------

log = logger.setup_applevel_logger(log_level=logger.LOGGER_MAPPING[LOG_LEVEL])

# COMMAND ----------

# MAGIC %md
# MAGIC # Inputs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import LEAP tables
# MAGIC ### Proj tables

# COMMAND ----------

# MAGIC %md
# MAGIC Average BOM built on finished work orders (single level), one snapshot per `snapshot_date` and `horizon`.

# COMMAND ----------

df_average_bom_raw = spark.read.table(
    f"{PIPELINE_READ_ENV}_proj.manuf_engineering.average_bom"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Effectivity of the ISM: gives the list of material / plant to explode.

# COMMAND ----------

df_a320 = spark.read.table(f"{REFERENCE_READ_ENV}_proj.manuf_engineering.mod_effectivity")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold tables

# COMMAND ----------

df_material = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.master_data.material_plant_exposed"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep1 - Snapshot selection
# MAGIC One snapshot only is exploded per run. Empty widget = most recent snapshot of the horizon.

# COMMAND ----------

df_average_bom_horizon = df_average_bom_raw.filter(f.col("horizon") == HORIZON)

if SNAPSHOT_DATE_PARAM:
    SNAPSHOT_DATE = SNAPSHOT_DATE_PARAM
else:
    SNAPSHOT_DATE = (
        df_average_bom_horizon.agg(f.max("snapshot_date").cast("string")).first()[0]
    )
IS_LATEST_RUN = not SNAPSHOT_DATE_PARAM

if SNAPSHOT_DATE is None:
    raise ValueError(f"No average_bom snapshot found for horizon {HORIZON}")
log.info(f"Exploding average_bom snapshot {SNAPSHOT_DATE} (horizon {HORIZON}, quantity from {QUANTITY_COLUMN})")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep2 - Single-level edges
# MAGIC One edge per (material, plant, component). `average_bom` can hold several rows for the same edge
# MAGIC (item type, phantom path): their average quantities are added up. Edges with no quantity are dropped.

# COMMAND ----------

EDGE_KEYS = ["material_number", "plant", "component"]

df_edges = (
    df_average_bom_horizon.filter(f.col("snapshot_date") == f.to_date(f.lit(SNAPSHOT_DATE)))
    .select(
        f.col("Material_Number").alias("material_number"),
        f.col("Plant").alias("plant"),
        f.col("Component").alias("component"),
        f.col(QUANTITY_COLUMN).alias("edge_qty"),
    )
    .filter(f.col("component").isNotNull())
    .groupBy(*EDGE_KEYS)
    .agg(f.sum("edge_qty").alias("edge_qty"))
    .filter(f.col("edge_qty") > 0)
    .filter(f.col("material_number") != f.col("component"))
)

df_gx_edges = SparkDFDataset(df_edges, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep3 - Plant switch and procurement flags
# MAGIC Plant that really supplies the component (`plant_buy`) and flags used by the explosion.
# MAGIC Deduplicated on the join key so the joins cannot fan out the edges.

# COMMAND ----------

df_mat_switch = df_material.select(
    f.col("material_number").alias("component"), "plant", "plant_buy"
).dropDuplicates(["component", "plant"])

df_gx_mat_switch = SparkDFDataset(df_mat_switch, persist=False)

# COMMAND ----------

df_mat_flag = df_material.select(
    f.col("material_number").alias("component"),
    f.col("plant").alias("plant_buy"),
    "procurement_type",
    "procurement_special_type",
).dropDuplicates(["component", "plant_buy"])

df_gx_mat_flag = SparkDFDataset(df_mat_flag, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep4 - Material enrichment (price, lead times, supplier)
# MAGIC Only the MRP-unit plant row of each component is kept.

# COMMAND ----------

df_mat_full = (
    df_material.filter(f.col("plant") == f.col("MRP_unit"))
    .select(
        f.col("material_number").alias("component"),
        f.col("plant").alias("plant_buy"),
        "material_description",
        "external_lead_time",
        "internal_lead_time",
        f.col("standard_price_eur_budget").alias("unit_price"),
        "supplier_code",
        "supplier_name",
        "abc_indicator",
        "bulk_material",
    )
    .dropDuplicates(["component", "plant_buy"])
)

df_gx_mat_full = SparkDFDataset(df_mat_full, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep5 - ISM profit center

# COMMAND ----------

df_profit_center = (
    df_material.filter(f.col("plant") == f.col("MRP_unit"))
    .select("material_number", "plant", "profit_center")
    .dropDuplicates(["material_number", "plant"])
)

df_gx_profit_center = SparkDFDataset(df_profit_center, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep6 - Roots
# MAGIC ISM (material / plant) of the effectivity file that have an average BOM in the snapshot.

# COMMAND ----------

df_roots = (
    df_a320.select("material_number", "plant")
    .distinct()
    .join(df_edges.select("material_number", "plant").distinct(), ["material_number", "plant"], "inner")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 1 - Edges with plant switch and flags
# MAGIC Each edge gets the plant that supplies the component and its procurement flags (LEFT joins, one per cell).

# COMMAND ----------

df_edges_switch = df_edges.join(df_mat_switch, ["component", "plant"], "left").withColumn(
    "plant_buy", f.coalesce("plant_buy", "plant")
)

# COMMAND ----------

df_edges_flag = df_edges_switch.join(df_mat_flag, ["component", "plant_buy"], "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 2 - Multilevel explosion
# MAGIC Level 0 is the ISM, level 1 its direct components, and so on (max 20 levels).
# MAGIC A component already in the path is not exploded again (cycle guard).

# COMMAND ----------

level_0 = df_roots.select(
    f.col("material_number"),
    f.col("material_number").alias("parent"),
    f.col("plant"),
    f.col("plant").alias("plant_buy"),
    f.lit(None).cast("string").alias("component"),
    f.lit(1.0).alias("quantity"),
    f.lit(0).alias("level"),
    f.lit(None).cast("string").alias("procurement_type"),
    f.lit(None).cast("string").alias("procurement_special_type"),
    f.lit(None).cast("string").alias("branch_flag"),
    f.array(f.col("material_number")).alias("path"),
)

current_level = (
    df_edges_flag.alias("e")
    .join(
        df_roots.alias("r"),
        (f.col("r.material_number") == f.col("e.material_number"))
        & (f.col("r.plant") == f.col("e.plant")),
        "inner",
    )
    .select(
        f.col("r.material_number").alias("material_number"),
        f.col("e.material_number").alias("parent"),
        f.col("e.plant").alias("plant"),
        f.col("e.plant_buy").alias("plant_buy"),
        f.col("e.component").alias("component"),
        f.col("e.edge_qty").alias("quantity"),
        f.lit(1).alias("level"),
        f.col("e.procurement_type").alias("procurement_type"),
        f.col("e.procurement_special_type").alias("procurement_special_type"),
        f.lit(None).cast("string").alias("branch_flag"),
        f.array(f.col("r.material_number"), f.col("e.component")).alias("path"),
    )
)

levels = [level_0, current_level]

for i in range(2, 21):
    next_level = (
        current_level.alias("cur")
        .join(
            df_edges_flag.alias("e"),
            (f.col("cur.component") == f.col("e.material_number"))
            & (f.col("cur.plant_buy") == f.col("e.plant")),
            "inner",
        )
        .filter(~f.array_contains(f.col("cur.path"), f.col("e.component")))
        .select(
            f.col("cur.material_number").alias("material_number"),
            f.col("cur.component").alias("parent"),
            f.col("cur.plant").alias("plant"),
            f.col("e.plant_buy").alias("plant_buy"),
            f.col("e.component").alias("component"),
            (f.col("cur.quantity") * f.col("e.edge_qty")).alias("quantity"),
            f.lit(i).alias("level"),
            f.col("e.procurement_type").alias("procurement_type"),
            f.col("e.procurement_special_type").alias("procurement_special_type"),
            f.when(f.col("cur.branch_flag") == "X", f.lit("X"))
            .when(
                (f.col("cur.procurement_type") == "F")
                & (f.col("cur.procurement_special_type") == "20"),
                f.lit("X"),
            )
            .alias("branch_flag"),
            f.array_append(f.col("cur.path"), f.col("e.component")).alias("path"),
        )
    )
    if not next_level.take(1):
        log.info(f"Explosion stopped at level {i - 1}")
        break
    levels.append(next_level)
    current_level = next_level

df_exploded = reduce(lambda a, b: a.unionByName(b), levels).withColumn(
    "type",
    f.when(
        f.col("component").startswith("IS") & f.col("component").endswith("M"), "WO"
    ).otherwise("PO"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 3 - Enrich with material master
# MAGIC Component description, price, lead time (external for `PO`, internal for `WO`) and ISM profit center.

# COMMAND ----------

df_enriched = df_exploded.join(df_mat_full, ["component", "plant_buy"], "left").withColumn(
    "lead_time",
    f.when(f.col("type") == "PO", f.col("external_lead_time")).otherwise(
        f.col("internal_lead_time")
    ),
)

# COMMAND ----------

df_enriched = df_enriched.join(df_profit_center, ["material_number", "plant"], "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 4 - Snapshot columns and ID column
# MAGIC `snapshot_date` + `horizon` identify the snapshot. The ID is the table's own grain: snapshot, ISM, plant and path.
# MAGIC The ID is moved to the first column.

# COMMAND ----------

ID_COL = "average_bom_multilevel_ID"

df_final = (
    df_enriched.withColumn("snapshot_date", f.to_date(f.lit(SNAPSHOT_DATE)))
    .withColumn("horizon", f.lit(HORIZON))
    .withColumn(
        ID_COL,
        f.concat_ws(
            "-",
            f.col("snapshot_date").cast("string"),
            f.col("horizon").cast("string"),
            f.col("material_number"),
            f.col("plant"),
            f.array_join(f.col("path"), ">"),
        ),
    )
)
columns = df_final.columns
columns.remove(ID_COL)
df_final = df_final.select([ID_COL] + columns)

log.info("Exploded average BOM built — uniqueness of the ID and of every reference join key checked in Quality Checks below")

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality Checks

# COMMAND ----------

validation_level = "AMBER" if BYPASS_QUALITY_CHECKS else "RED"

df_gx_final = SparkDFDataset(df_final, persist=False)

red_quality_check_results = [
    df_gx_final.expect_column_values_to_not_be_null(column=ID_COL),
    df_gx_final.expect_column_values_to_be_unique(column=ID_COL),
    # join fan-out guards: every reference side is unique on its join key
    df_gx_edges.expect_compound_columns_to_be_unique(EDGE_KEYS),
    df_gx_mat_switch.expect_compound_columns_to_be_unique(["component", "plant"]),
    df_gx_mat_flag.expect_compound_columns_to_be_unique(["component", "plant_buy"]),
    df_gx_mat_full.expect_compound_columns_to_be_unique(["component", "plant_buy"]),
    df_gx_profit_center.expect_compound_columns_to_be_unique(["material_number", "plant"]),
]
gx_validation.validate_and_log_gx_results(
    quality_check_results=red_quality_check_results,
    validation_level=validation_level,
    info_message="average_bom_multilevel_ID unique and not null, reference join keys unique",
    error_message="Quality checks failed for average_bom_multilevel_history, please check log messages",
    pipeline_write_env=PIPELINE_WRITE_ENV,
    table_name="average_bom_multilevel_history",
    log_to_event_table=True,
    log_successes=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Outputs

# COMMAND ----------

CATALOG = f"{PIPELINE_WRITE_ENV}_proj"
SCHEMA = "supply_chain"
HISTORY_TABLE = "average_bom_multilevel_history"
HISTORY_TABLE_FULL_PATH = f"{CATALOG}.{SCHEMA}.{HISTORY_TABLE}"
CURRENT_TABLE_FULL_PATH = f"{CATALOG}.{SCHEMA}.current_average_bom"

# COMMAND ----------

# MAGIC %md
# MAGIC History: one snapshot per run, `replace_where` makes a rerun of the same snapshot idempotent
# MAGIC and leaves the other snapshots untouched.

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{SCHEMA}")

try:
    startTime = time.time()
    result = table_utils.save_table(
        dest_table=HISTORY_TABLE_FULL_PATH,
        df=df_final,
        mode="overwrite",
        replace_where=f"snapshot_date = '{SNAPSHOT_DATE}' AND horizon = {HORIZON}",
        merge_schema=True,
    )
except AnalysisException as e:
    log.error(
        f"Table {HISTORY_TABLE_FULL_PATH} has not been created, an error occured during saveAsTable. \nError is {e}"
    )
    raise
else:
    duration = time.time() - startTime
    log.info(f"Table {HISTORY_TABLE_FULL_PATH} saved in {duration} seconds.")
    if result is not None:
        result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Current view for existing consumers (`create_proj_bom_forecast` today): only refreshed by a run on the
# MAGIC latest snapshot, never by a backfill of a past snapshot.

# COMMAND ----------

if IS_LATEST_RUN:
    try:
        startTime = time.time()
        result = table_utils.save_table(
            dest_table=CURRENT_TABLE_FULL_PATH,
            df=df_final,
            mode="overwrite",
            overwrite_schema=True,
        )
    except AnalysisException as e:
        log.error(
            f"Table {CURRENT_TABLE_FULL_PATH} has not been created, an error occured during saveAsTable. \nError is {e}"
        )
        raise
    else:
        duration = time.time() - startTime
        log.info(f"Table {CURRENT_TABLE_FULL_PATH} saved in {duration} seconds.")
        if result is not None:
            result.display()
else:
    log.info(f"Backfill of snapshot {SNAPSHOT_DATE}: {CURRENT_TABLE_FULL_PATH} left untouched")
