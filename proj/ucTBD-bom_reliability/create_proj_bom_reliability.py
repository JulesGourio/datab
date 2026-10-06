# Databricks notebook source
# MAGIC %md
# MAGIC # PROJ BOM RELIABILITY
# MAGIC
# MAGIC **Description:**
# MAGIC Measures, for each manufactured article (AF), how reliable its BOM was: the component forecast known at a
# MAGIC date T0 is compared with what was really consumed by the production orders (OF) started after T1 = T0 + P1
# MAGIC and finished before T2 = T1 + P2. One row per period x plant x AF x component forecast and/or consumed.
# MAGIC Feeds the BOM reliability PowerBI report (summary per profit center, analysis per plant / profit center / AF).
# MAGIC
# MAGIC **Highlighted complexities:**
# MAGIC - The forecast of an OF at T0 is the OF itself if it already existed at T0, else the planned order (OP) it was
# MAGIC   converted from (`planned_order_link`). OFs with neither at T0 are listed but kept out of the reliability.
# MAGIC - Forecasts are scaled to the final OF quantity (`volume_scaling_factor`) so the reliability measures the BOM,
# MAGIC   not the change of order quantity between T0 and production (to be confirmed with the business).
# MAGIC - Phantom assemblies (`MARC.SOBSL = '50'`) are never consumed: SAP already explodes them in the reservations;
# MAGIC   for the standard BOM (forecast 3) they are exploded here, level by level, with BOM item number `9999`.
# MAGIC - Periods are pre-computed on a grid (T0 = first day of each month with both histories, P1/P2 lists): the
# MAGIC   per-component error then average cannot be computed by DAX in DirectQuery.
# MAGIC
# MAGIC **Intended Pipeline**
# MAGIC - M_3_Supply_Chain_BOM_Reliability_Project (to be confirmed), after the two Gold history notebooks
# MAGIC
# MAGIC **Inputs Data**
# MAGIC - {PIPELINE_WRITE_ENV}_gold.production.bom_item_history
# MAGIC - {PIPELINE_WRITE_ENV}_gold.mrp.order_component_requirement_history
# MAGIC - {REFERENCE_READ_ENV}_gold.production.work_orders_sap_exposed
# MAGIC - {REFERENCE_READ_ENV}_gold.supply_chain_logistic.part_movement_exposed
# MAGIC - {REFERENCE_READ_ENV}_gold.master_data.material_plant
# MAGIC - {REFERENCE_READ_ENV}_gold.master_data.material_exposed
# MAGIC - {REFERENCE_READ_ENV}_gold.master_data.plant_master_data_latest_exposed
# MAGIC - {REFERENCE_READ_ENV}_gold.finance.profit_center_exposed
# MAGIC
# MAGIC **Output Tables (Pipeline)**
# MAGIC - {PIPELINE_WRITE_ENV}_proj.supply_chain.ucTBD_bom_reliability_component
# MAGIC - {PIPELINE_WRITE_ENV}_proj.supply_chain.ucTBD_bom_reliability_work_order
# MAGIC
# MAGIC **Calculated columns (business meaning)**
# MAGIC - `forecast_1_quantity`: gross forecast = requirement quantity of the OP/OF known at T0 (scrap included),
# MAGIC   scaled to the final OF quantity.
# MAGIC - `forecast_2_quantity`: same without the component scrap (BOM quantity of the reservation).
# MAGIC - `forecast_3_quantity`: standard BOM known at T0 x final OF quantity, component scrap included, phantoms exploded.
# MAGIC - `consumption_1_quantity`: all goods movements on the OF for the component (issues +, returns -), AF receipts excluded.
# MAGIC - `consumption_2_quantity`: nominal movements only (261 / 262).
# MAGIC - `consumption_3_quantity`: consumption 2 + inventory differences pro rata - method to be defined, NULL for now.
# MAGIC - `error_percentage_forecast_X_consumption_Y`: |forecast - consumption| / max(forecast, consumption), 0 when
# MAGIC   both are 0, consumption below 0 counted as 0.
# MAGIC - `reliability_status`: default pair (forecast 2 vs consumption 2): `OVERSTOCK` (forecast > consumption),
# MAGIC   `SHORTAGE` (forecast < consumption), `OK`.
# MAGIC - Reliability of an AF / profit center = 1 - average of `error_percentage_forecast_2_consumption_2` of its
# MAGIC   component rows (DAX `AVERAGE`).
# MAGIC - `volume_scaling_factor` (work order table): final OF quantity / order quantity known at T0.
# MAGIC - `forecast_source`: `WORK_ORDER_AT_T0`, `PLANNED_ORDER_AT_T0` or `NONE`; `is_in_reliability_scope` = not `NONE`.

# COMMAND ----------

# MAGIC %md
# MAGIC # Technical debt
# MAGIC - Use case number `ucTBD`, Proj schema `supply_chain` and the folder name are placeholders (JIRA EPIC to come).
# MAGIC - Period grid limited to P1 = P2 = 3 months until the business confirms the other choices (volume).
# MAGIC - `consumption_3_quantity` not computed (inventory differences 701/702 are never posted on an OF).
# MAGIC - Component price is the current budget standard price (`material_plant`), not the price at T0
# MAGIC   (`material_plant_price_history_exposed` holds it per fiscal period).
# MAGIC - "PF usage" column of the specification not delivered: definition to obtain (`quota_usage`?).
# MAGIC - Optional page 3 (per BOM item) and comments per BOM item not delivered.
# MAGIC - Alternative of the BOM used by a phantom assembly: the lowest alternative of the phantom article (to be confirmed).
# MAGIC - `profit_center_exposed` may hold several validity rows per profit center: one is kept arbitrarily
# MAGIC   (`dropDuplicates`) - to be replaced by the row valid today once the validity columns are confirmed.
# MAGIC - GX expectations `expect_column_values_to_be_in_set` / `_to_be_between` are not used by the reference
# MAGIC   notebook yet: check they exist in the installed GX version.

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

# Lab test runs only (CLAUDE.md §0.1): e.g. "dev_lab.lab_jules" redirects every table this notebook writes,
# and the two Gold history tables of this project are then read from the same schema.
# Always empty in jobs; never commit a non-empty default (hardcoded env = Convention Checker BLOCKER).
dbutils.widgets.text("lab_target_schema", "")
LAB_TARGET_SCHEMA = dbutils.widgets.get("lab_target_schema").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Debug Boolean
# MAGIC `debug_plant` restricts the run to one plant (lab only, empty in jobs).

# COMMAND ----------

dbutils.widgets.text("debug", "False")
DEBUG = dbutils.widgets.get("debug").lower() == "true"

dbutils.widgets.dropdown(
    "log_level",
    defaultValue="info",
    choices=["debug", "info", "warning", "error", "critical"],
)
LOG_LEVEL = dbutils.widgets.get("log_level")

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

DOMAIN = "supply_chain"  # to be confirmed
UC_PREFIX = "ucTBD"  # to be confirmed (JIRA EPIC)

P1_MONTHS = [3]  # ignored period after T0 (business list to be confirmed: 1, 2, 3, 6)
P2_MONTHS = [3]  # analysed period after T1
DEFAULT_T0_MONTHS_AGO = 6  # default T0 = first day of the month, 6 months ago; P1 = P2 = 3

BOM_USAGE = "1"
EXCLUDED_ITEM_CATEGORIES = ["D", "T"]  # document and text items: no material
PHANTOM_ITEM_NUMBER = "9999"
MAX_PHANTOM_DEPTH = 5

AF_RECEIPT_MOVEMENT_TYPES = ["101", "102", "122"]  # goods receipt of the AF itself: never a consumption
NOMINAL_MOVEMENT_TYPES = ["261", "262"]  # consumption 2 (to be confirmed: how to exclude breakage)
ISSUE_INDICATOR = "H"  # DC_indicator (SHKZG): H = credit = issue from stock (+), S = return (-)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Helper Functions
# MAGIC - `own_gold_table`: tables produced by this project are read from the lab schema during lab runs.
# MAGIC - `sap_number`: `Quantity` of the goods movements is SAP text (trailing minus possible).
# MAGIC - `error_percentage`: the error of one component for one (forecast, consumption) pair.

# COMMAND ----------

def own_gold_table(schema, table):
    if LAB_TARGET_SCHEMA:
        return f"{LAB_TARGET_SCHEMA}.{table}"
    return f"{PIPELINE_WRITE_ENV}_gold.{schema}.{table}"


def sap_number(column_name):
    raw = f.trim(f.col(column_name))
    value = f.expr(f"try_cast(regexp_replace(trim({column_name}), '-$', '') AS DOUBLE)")
    return f.when(raw.endswith("-"), -value).otherwise(value)


def error_percentage(forecast_column, consumption_column):
    forecast = f.greatest(f.col(forecast_column), f.lit(0.0))
    consumption = f.greatest(f.col(consumption_column), f.lit(0.0))
    largest = f.greatest(forecast, consumption)
    return f.when(largest == 0, f.lit(0.0)).otherwise(f.abs(forecast - consumption) / largest)

# COMMAND ----------

# MAGIC %md
# MAGIC # Inputs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import LEAP tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold tables of this project
# MAGIC Standard BOM and MRP requirements, one snapshot per month.

# COMMAND ----------

df_bom_raw = spark.read.table(own_gold_table("production", "bom_item_history"))
df_requirement_raw = spark.read.table(own_gold_table("mrp", "order_component_requirement_history"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reference Gold tables

# COMMAND ----------

df_work_order_raw = spark.read.table(f"{REFERENCE_READ_ENV}_gold.production.work_orders_sap_exposed")
df_part_movement_raw = spark.read.table(f"{REFERENCE_READ_ENV}_gold.supply_chain_logistic.part_movement_exposed")
df_material_plant_raw = spark.read.table(f"{REFERENCE_READ_ENV}_gold.master_data.material_plant")
df_material_raw = spark.read.table(f"{REFERENCE_READ_ENV}_gold.master_data.material_exposed")
df_plant_raw = spark.read.table(f"{REFERENCE_READ_ENV}_gold.master_data.plant_master_data_latest_exposed")
df_profit_center_raw = spark.read.table(f"{REFERENCE_READ_ENV}_gold.finance.profit_center_exposed")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep1 - Period grid
# MAGIC T0 = every snapshot date present in both histories; T1 = T0 + P1, T2 = T1 + P2; only periods already over
# MAGIC (T2 <= today). The default period of the report is flagged.

# COMMAND ----------

bom_snapshot_dates = {r["snapshot_date"] for r in df_bom_raw.select("snapshot_date").distinct().collect()}
requirement_snapshot_dates = {
    r["snapshot_date"] for r in df_requirement_raw.select("snapshot_date").distinct().collect()
}
T0_DATES = sorted(bom_snapshot_dates & requirement_snapshot_dates)

today = datetime.date.today()
default_month = today.month - DEFAULT_T0_MONTHS_AGO
DEFAULT_T0 = datetime.date(today.year + (default_month - 1) // 12, (default_month - 1) % 12 + 1, 1)


def add_months(day, months):
    month_index = day.month - 1 + months
    return datetime.date(day.year + month_index // 12, month_index % 12 + 1, day.day)


period_rows = []
for t0 in T0_DATES:
    for p1 in P1_MONTHS:
        for p2 in P2_MONTHS:
            t1 = add_months(t0, p1)
            t2 = add_months(t1, p2)
            if t2 <= today:
                period_rows.append(
                    (f"{t0:%Y%m%d}-{p1}-{p2}", t0, p1, p2, t1, t2, t0 == DEFAULT_T0 and p1 == 3 and p2 == 3)
                )

df_period = spark.createDataFrame(
    period_rows,
    "period_ID string, T0_date date, P1_months int, P2_months int, T1_date date, T2_date date, "
    "is_default_period boolean",
)
log.info(f"{len(period_rows)} periods, T0 from {T0_DATES[0] if T0_DATES else None} - default T0 {DEFAULT_T0}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep2 - Production orders
# MAGIC Not cancelled, with real start and end dates. `quantity_planned` (GAMNG, assembly scrap included) is the final
# MAGIC OF quantity used to scale the forecasts.

# COMMAND ----------

COL_WORK_ORDER = [
    "work_order_number",
    "material_number",
    "plant",
    f.col("profit_center").alias("work_order_profit_center"),
    "work_order_type",
    "work_order_type_description",
    f.col("planned_order_link").alias("origin_planned_order_number"),
    f.col("quantity_planned").alias("work_order_quantity"),
    "real_start_date",
    "real_end_date",
]

df_work_order_prep = (
    df_work_order_raw.filter(~f.coalesce(f.col("is_cancelled"), f.lit(False)))
    .filter(f.col("real_start_date").isNotNull() & f.col("real_end_date").isNotNull())
    .select(*COL_WORK_ORDER)
)

if DEBUG_PLANT:
    df_work_order_prep = df_work_order_prep.filter(f.col("plant") == DEBUG_PLANT)

df_work_order_prep = table_utils.remove_leading_zeros(
    df=df_work_order_prep,
    column_names=["work_order_number", "material_number", "origin_planned_order_number"],
)

df_gx_work_order = SparkDFDataset(df_work_order_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep3 - MRP requirements at the T0 dates
# MAGIC Phantom lines (exploded by SAP in the next lines) and document / text items are removed. Quantities are summed
# MAGIC per order and component (an order can need the same component on several items / operations).

# COMMAND ----------

df_requirement_prep = (
    df_requirement_raw.filter(f.col("snapshot_date").isin(T0_DATES))
    .filter(~f.col("is_phantom_item"))
    .filter(~f.coalesce(f.col("BOM_item_category"), f.lit("")).isin(EXCLUDED_ITEM_CATEGORIES))
    .filter(f.col("component_material_number").isNotNull())
)

df_wo_requirement_prep = (
    df_requirement_prep.filter(f.col("order_category") == "WORK_ORDER")
    .groupBy(f.col("snapshot_date").alias("T0_date"), "work_order_number", "component_material_number")
    .agg(
        f.sum("requirement_quantity").alias("forecast_1_quantity"),
        f.sum("component_BOM_quantity").alias("forecast_2_quantity"),
    )
)

df_po_requirement_prep = (
    df_requirement_prep.filter(f.col("order_category") == "PLANNED_ORDER")
    .groupBy(
        f.col("snapshot_date").alias("T0_date"),
        f.col("planned_order_number").alias("origin_planned_order_number"),
        "component_material_number",
    )
    .agg(
        f.sum("requirement_quantity").alias("forecast_1_quantity"),
        f.sum("component_BOM_quantity").alias("forecast_2_quantity"),
    )
)

# COMMAND ----------

# Order header at T0 (one value per order; the requirement lines repeat it)
df_wo_header_t0_prep = (
    df_requirement_raw.filter(f.col("snapshot_date").isin(T0_DATES))
    .filter(f.col("order_category") == "WORK_ORDER")
    .groupBy(f.col("snapshot_date").alias("T0_date"), "work_order_number")
    .agg(
        f.max("order_quantity").alias("WO_order_quantity_T0"),
        f.max("BOM_alternative").alias("WO_BOM_alternative_T0"),
    )
)

df_po_header_t0_prep = (
    df_requirement_raw.filter(f.col("snapshot_date").isin(T0_DATES))
    .filter(f.col("order_category") == "PLANNED_ORDER")
    .groupBy(
        f.col("snapshot_date").alias("T0_date"),
        f.col("planned_order_number").alias("origin_planned_order_number"),
    )
    .agg(
        f.max("order_quantity").alias("PO_order_quantity_T0"),
        f.max("BOM_alternative").alias("PO_BOM_alternative_T0"),
    )
)

df_gx_wo_header_t0 = SparkDFDataset(df_wo_header_t0_prep, persist=False)
df_gx_po_header_t0 = SparkDFDataset(df_po_header_t0_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep4 - Standard BOM at the T0 dates
# MAGIC Quantity per unit of AF in the component base unit: proportional items are divided by the BOM base
# MAGIC quantity, fixed-quantity items are per order. Component scrap: BOM item scrap, else article component scrap
# MAGIC (SAP rule). Assembly scrap is not added: the final OF quantity (GAMNG) already contains it.

# COMMAND ----------

df_bom_prep = (
    df_bom_raw.filter(f.col("snapshot_date").isin(T0_DATES))
    .filter(f.col("BOM_usage") == BOM_USAGE)
    .filter(~f.coalesce(f.col("BOM_item_category"), f.lit("")).isin(EXCLUDED_ITEM_CATEGORIES))
    .filter(f.col("component_material_number").isNotNull())
    .withColumn(
        "scrap_factor",
        1
        + f.when(f.col("component_scrap_percentage") > 0, f.col("component_scrap_percentage"))
        .otherwise(f.coalesce(f.col("material_component_scrap_percentage"), f.lit(0.0)))
        / 100,
    )
    .withColumn(
        "quantity_per_unit",
        f.when(f.col("is_fixed_quantity"), f.expr("try_cast(NULL AS DOUBLE)")).otherwise(
            f.col("component_quantity_in_base_unit") / f.col("BOM_base_quantity") * f.col("scrap_factor")
        ),
    )
    .withColumn(
        "quantity_per_order",
        f.when(
            f.col("is_fixed_quantity"), f.col("component_quantity_in_base_unit") * f.col("scrap_factor")
        ).otherwise(f.lit(0.0)),
    )
    .select(
        f.col("snapshot_date").alias("T0_date"),
        "plant",
        "material_number",
        "BOM_alternative",
        "BOM_item_number",
        "component_material_number",
        "quantity_per_unit",
        "quantity_per_order",
        "is_phantom_item",
    )
)

# COMMAND ----------

# BOM of the phantom assemblies: lowest alternative of the phantom article (to be confirmed)
window_phantom_alternative = Window.partitionBy("T0_date", "plant", "material_number")
df_phantom_bom_prep = (
    df_bom_prep.withColumn("min_alternative", f.min("BOM_alternative").over(window_phantom_alternative))
    .filter(f.col("BOM_alternative") == f.col("min_alternative"))
    .select(
        "T0_date",
        "plant",
        f.col("material_number").alias("phantom_material_number"),
        f.col("component_material_number").alias("exploded_component_material_number"),
        f.col("quantity_per_unit").alias("exploded_quantity_per_unit"),
        f.col("quantity_per_order").alias("exploded_quantity_per_order"),
        f.col("is_phantom_item").alias("exploded_is_phantom_item"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep5 - Goods movements of production orders
# MAGIC `Quantity` is already in the component base unit (checked: 0 movement with another unit). Sign from the
# MAGIC debit/credit indicator.

# COMMAND ----------

df_movement_prep = (
    df_part_movement_raw.filter(f.col("Work_order").isNotNull() & (f.trim("Work_order") != ""))
    .filter(~f.col("Movement_type").isin(AF_RECEIPT_MOVEMENT_TYPES))
    .select(
        f.trim("Work_order").alias("work_order_number"),
        f.trim("Material_number").alias("component_material_number"),
        f.trim("Movement_type").alias("movement_type"),
        (
            sap_number("Quantity")
            * f.when(f.col("DC_indicator") == ISSUE_INDICATOR, f.lit(1.0)).otherwise(f.lit(-1.0))
        ).alias("signed_quantity"),
    )
)

df_movement_prep = table_utils.remove_leading_zeros(
    df=df_movement_prep, column_names=["work_order_number", "component_material_number"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep6 - Descriptions and component attributes
# MAGIC Article description and classification (`material_exposed`), plant data of the component (lead times, price,
# MAGIC ABC) and of the AF (profit center), plant and profit center descriptions.

# COMMAND ----------

df_material_prep = df_material_raw.select(
    "material_number", "material_description", "familly_std", "classe_std"
).dropDuplicates(["material_number"])
df_material_prep = table_utils.remove_leading_zeros(df=df_material_prep, column_names=["material_number"])

df_material_plant_prep = df_material_plant_raw.select(
    "material_number",
    "plant",
    "profit_center",
    "material_base_unit",
    f.col("external_lead_time").alias("external_lead_time_days"),
    f.col("internal_lead_time").alias("internal_lead_time_days"),
    f.col("standard_price_eur_budget").alias("standard_price_EUR_budget"),
    "abc_indicator",
).dropDuplicates(["material_number", "plant"])
df_material_plant_prep = table_utils.remove_leading_zeros(
    df=df_material_plant_prep, column_names=["material_number"]
)

df_plant_prep = df_plant_raw.select(
    f.col("Plant").alias("plant"), f.col("Plant_Description").alias("plant_description")
).dropDuplicates(["plant"])

df_profit_center_prep = df_profit_center_raw.select(
    f.col("Profit_Center").alias("profit_center"),
    f.col("Short_Description").alias("profit_center_description"),
).dropDuplicates(["profit_center"])

df_gx_material = SparkDFDataset(df_material_prep, persist=False)
df_gx_material_plant = SparkDFDataset(df_material_plant_prep, persist=False)
df_gx_plant = SparkDFDataset(df_plant_prep, persist=False)
df_gx_profit_center = SparkDFDataset(df_profit_center_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 1 - Production orders of each period
# MAGIC OF started on or after T1 and finished on or before T2.

# COMMAND ----------

df_scope = df_work_order_prep.join(
    f.broadcast(df_period),
    (df_work_order_prep["real_start_date"] >= df_period["T1_date"])
    & (df_work_order_prep["real_end_date"] <= df_period["T2_date"]),
    how="inner",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 2 - Forecast source at T0
# MAGIC The OF itself if it existed at T0, else its original planned order, else nothing.

# COMMAND ----------

df_scope = df_scope.join(df_wo_header_t0_prep, ["T0_date", "work_order_number"], how="left")
df_scope = df_scope.join(df_po_header_t0_prep, ["T0_date", "origin_planned_order_number"], how="left")
log.info("Joined order headers at T0 - uniqueness checked in Quality Checks")

df_scope = (
    df_scope.withColumn(
        "forecast_source",
        f.when(f.col("WO_order_quantity_T0").isNotNull(), f.lit("WORK_ORDER_AT_T0"))
        .when(f.col("PO_order_quantity_T0").isNotNull(), f.lit("PLANNED_ORDER_AT_T0"))
        .otherwise(f.lit("NONE")),
    )
    .withColumn("order_quantity_T0", f.coalesce("WO_order_quantity_T0", "PO_order_quantity_T0"))
    .withColumn("BOM_alternative_T0", f.coalesce("WO_BOM_alternative_T0", "PO_BOM_alternative_T0"))
    .withColumn("is_in_reliability_scope", f.col("forecast_source") != "NONE")
    .withColumn(
        "volume_scaling_factor",
        f.when(
            f.col("order_quantity_T0") > 0, f.col("work_order_quantity") / f.col("order_quantity_T0")
        ).otherwise(f.lit(1.0)),
    )
    .drop("WO_order_quantity_T0", "PO_order_quantity_T0", "WO_BOM_alternative_T0", "PO_BOM_alternative_T0")
)

df_gx_scope = SparkDFDataset(df_scope, persist=False)

df_scope_in = df_scope.filter(f.col("is_in_reliability_scope")).select(
    "period_ID",
    "T0_date",
    "work_order_number",
    "origin_planned_order_number",
    "forecast_source",
    "plant",
    "material_number",
    "BOM_alternative_T0",
    "work_order_quantity",
    "volume_scaling_factor",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 3 - Forecasts 1 and 2 (MRP requirements at T0, scaled)

# COMMAND ----------

df_forecast_wo = df_scope_in.filter(f.col("forecast_source") == "WORK_ORDER_AT_T0").join(
    df_wo_requirement_prep, ["T0_date", "work_order_number"], how="inner"
)
df_forecast_po = (
    df_scope_in.filter(f.col("forecast_source") == "PLANNED_ORDER_AT_T0")
    .join(df_po_requirement_prep, ["T0_date", "origin_planned_order_number"], how="inner")
)

df_forecast_mrp = df_forecast_wo.unionByName(df_forecast_po).select(
    "period_ID",
    "work_order_number",
    "plant",
    "material_number",
    "component_material_number",
    (f.col("forecast_1_quantity") * f.col("volume_scaling_factor")).alias("forecast_1_quantity"),
    (f.col("forecast_2_quantity") * f.col("volume_scaling_factor")).alias("forecast_2_quantity"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 4 - Forecast 3 (standard BOM at T0 x final OF quantity)
# MAGIC BOM of the OF alternative known at T0.

# COMMAND ----------

df_forecast_bom = df_scope_in.join(
    df_bom_prep.withColumnRenamed("BOM_alternative", "BOM_alternative_T0"),
    ["T0_date", "plant", "material_number", "BOM_alternative_T0"],
    how="inner",
).select(
    "period_ID",
    "T0_date",
    "work_order_number",
    "plant",
    "material_number",
    "BOM_item_number",
    "component_material_number",
    (
        f.coalesce(f.col("quantity_per_unit") * f.col("work_order_quantity"), f.lit(0.0))
        + f.col("quantity_per_order")
    ).alias("forecast_3_quantity"),
    "is_phantom_item",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 5 - Phantom explosion (forecast 3)
# MAGIC A phantom line is replaced by the BOM of the phantom article (same T0), quantities multiplied by the phantom
# MAGIC quantity; nested phantoms are exploded at the next level. Exploded components get BOM item `9999`.
# MAGIC A phantom without BOM at T0 is kept (flagged) so that the AMBER check below can report it.

# COMMAND ----------

df_phantom_bom_keys = df_phantom_bom_prep.select(
    "T0_date", "plant", f.col("phantom_material_number").alias("component_material_number")
).distinct()

for level in range(MAX_PHANTOM_DEPTH):
    df_phantom_lines = df_forecast_bom.filter(f.col("is_phantom_item"))
    df_exploded = (
        df_phantom_lines.join(
            df_phantom_bom_prep,
            (df_phantom_lines["T0_date"] == df_phantom_bom_prep["T0_date"])
            & (df_phantom_lines["plant"] == df_phantom_bom_prep["plant"])
            & (df_phantom_lines["component_material_number"] == df_phantom_bom_prep["phantom_material_number"]),
            how="inner",
        )
        .select(
            df_phantom_lines["period_ID"],
            df_phantom_lines["T0_date"],
            df_phantom_lines["work_order_number"],
            df_phantom_lines["plant"],
            df_phantom_lines["material_number"],
            f.lit(PHANTOM_ITEM_NUMBER).alias("BOM_item_number"),
            f.col("exploded_component_material_number").alias("component_material_number"),
            (
                f.coalesce(f.col("exploded_quantity_per_unit"), f.lit(0.0)) * f.col("forecast_3_quantity")
                + f.col("exploded_quantity_per_order")
            ).alias("forecast_3_quantity"),
            f.col("exploded_is_phantom_item").alias("is_phantom_item"),
        )
    )
    df_unexploded = df_phantom_lines.join(
        df_phantom_bom_keys, ["T0_date", "plant", "component_material_number"], how="left_anti"
    )
    df_forecast_bom = (
        df_forecast_bom.filter(~f.col("is_phantom_item")).unionByName(df_exploded).unionByName(df_unexploded)
    )

# Phantoms left after MAX_PHANTOM_DEPTH levels (or without BOM) are checked in Quality Checks, then dropped
df_gx_forecast_bom = SparkDFDataset(df_forecast_bom, persist=False)

df_forecast_standard = df_forecast_bom.filter(~f.col("is_phantom_item")).select(
    "period_ID", "work_order_number", "plant", "material_number", "component_material_number", "forecast_3_quantity"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 6 - Consumptions of the in-scope OFs
# MAGIC Movements on the AF itself (rework) are excluded: the forecast never contains the AF.

# COMMAND ----------

df_consumption = (
    df_scope_in.select("period_ID", "work_order_number", "plant", "material_number")
    .join(df_movement_prep, ["work_order_number"], how="inner")
    .filter(f.col("component_material_number") != f.col("material_number"))
    .select(
        "period_ID",
        "work_order_number",
        "plant",
        "material_number",
        "component_material_number",
        f.col("signed_quantity").alias("consumption_1_quantity"),
        f.when(f.col("movement_type").isin(NOMINAL_MOVEMENT_TYPES), f.col("signed_quantity"))
        .otherwise(f.lit(0.0))
        .alias("consumption_2_quantity"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 7 - One row per period x plant x AF x component
# MAGIC The three sources are stacked (missing measures = 0) then summed over the OFs of the period: no join between
# MAGIC forecast and consumption, hence no fan-out.

# COMMAND ----------

MEASURES = [
    "forecast_1_quantity",
    "forecast_2_quantity",
    "forecast_3_quantity",
    "consumption_1_quantity",
    "consumption_2_quantity",
]
KEYS = ["period_ID", "plant", "material_number", "component_material_number"]

df_stacked = (
    df_forecast_mrp.unionByName(df_forecast_standard, allowMissingColumns=True)
    .unionByName(df_consumption, allowMissingColumns=True)
    .fillna(0.0, subset=MEASURES)
)

df_transf = df_stacked.groupBy(*KEYS).agg(*[f.sum(m).alias(m) for m in MEASURES])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 8 - Errors, status and flags

# COMMAND ----------

df_transf = (
    df_transf.withColumn("consumption_3_quantity", f.expr("try_cast(NULL AS DOUBLE)"))
    .withColumn("is_negative_consumption", f.col("consumption_2_quantity") < 0)
    .withColumn(
        "error_percentage_forecast_2_consumption_2",
        error_percentage("forecast_2_quantity", "consumption_2_quantity"),
    )
    .withColumn(
        "error_percentage_forecast_2_consumption_1",
        error_percentage("forecast_2_quantity", "consumption_1_quantity"),
    )
    .withColumn(
        "error_percentage_forecast_1_consumption_1",
        error_percentage("forecast_1_quantity", "consumption_1_quantity"),
    )
    .withColumn(
        "error_percentage_forecast_3_consumption_2",
        error_percentage("forecast_3_quantity", "consumption_2_quantity"),
    )
    .withColumn(
        "reliability_status",
        f.when(
            f.col("forecast_2_quantity") > f.greatest(f.col("consumption_2_quantity"), f.lit(0.0)),
            f.lit("OVERSTOCK"),
        )
        .when(f.col("forecast_2_quantity") < f.col("consumption_2_quantity"), f.lit("SHORTAGE"))
        .otherwise(f.lit("OK")),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 9 - Period, descriptions and component attributes

# COMMAND ----------

df_transf = df_transf.join(f.broadcast(df_period), ["period_ID"], how="left")

df_transf = df_transf.join(
    df_material_prep.select(
        "material_number", f.col("material_description").alias("material_description")
    ),
    ["material_number"],
    how="left",
)

df_transf = df_transf.join(
    df_material_prep.select(
        f.col("material_number").alias("component_material_number"),
        f.col("material_description").alias("component_description"),
        f.col("familly_std").alias("component_family"),
        f.col("classe_std").alias("component_class"),
    ),
    ["component_material_number"],
    how="left",
)

df_transf = df_transf.join(
    df_material_plant_prep.select(
        f.col("material_number").alias("component_material_number"),
        "plant",
        f.col("material_base_unit").alias("component_base_unit"),
        "external_lead_time_days",
        "internal_lead_time_days",
        f.col("standard_price_EUR_budget").alias("component_standard_price_EUR_budget"),
        f.col("abc_indicator").alias("component_ABC_indicator"),
    ),
    ["component_material_number", "plant"],
    how="left",
)

df_transf = df_transf.join(
    df_material_plant_prep.select("material_number", "plant", "profit_center"),
    ["material_number", "plant"],
    how="left",
)
df_transf = df_transf.join(df_plant_prep, ["plant"], how="left")
df_transf = df_transf.join(df_profit_center_prep, ["profit_center"], how="left")
log.info("Joined descriptions and plant data - key uniqueness checked in Quality Checks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 10 - Create ID columns and put them first

# COMMAND ----------

PK_COL = f"{UC_PREFIX}_bom_reliability_component_ID"

df_transf = df_transf.withColumn(
    PK_COL, f.concat_ws("-", "period_ID", "plant", "material_number", "component_material_number")
)

COMPONENT_OUTPUT_COLUMNS = [
    PK_COL,
    "period_ID",
    "T0_date",
    "P1_months",
    "P2_months",
    "T1_date",
    "T2_date",
    "is_default_period",
    "plant",
    "plant_description",
    "profit_center",
    "profit_center_description",
    "material_number",
    "material_description",
    "component_material_number",
    "component_description",
    "component_base_unit",
    "reliability_status",
    "error_percentage_forecast_2_consumption_2",
    "error_percentage_forecast_2_consumption_1",
    "error_percentage_forecast_1_consumption_1",
    "error_percentage_forecast_3_consumption_2",
    "forecast_1_quantity",
    "forecast_2_quantity",
    "forecast_3_quantity",
    "consumption_1_quantity",
    "consumption_2_quantity",
    "consumption_3_quantity",
    "is_negative_consumption",
    "external_lead_time_days",
    "internal_lead_time_days",
    "component_standard_price_EUR_budget",
    "component_family",
    "component_class",
    "component_ABC_indicator",
]

df_transf = df_transf.select(*COMPONENT_OUTPUT_COLUMNS)

# COMMAND ----------

WO_PK_COL = f"{UC_PREFIX}_bom_reliability_work_order_ID"

df_work_order_out = (
    df_scope.withColumn(WO_PK_COL, f.concat_ws("-", "period_ID", "work_order_number"))
    .join(df_plant_prep, ["plant"], how="left")
    .select(
        WO_PK_COL,
        "period_ID",
        "T0_date",
        "P1_months",
        "P2_months",
        "T1_date",
        "T2_date",
        "is_default_period",
        "work_order_number",
        "origin_planned_order_number",
        "plant",
        "plant_description",
        f.col("work_order_profit_center").alias("profit_center"),
        "material_number",
        "work_order_type",
        "work_order_type_description",
        "real_start_date",
        "real_end_date",
        "work_order_quantity",
        "order_quantity_T0",
        "volume_scaling_factor",
        "BOM_alternative_T0",
        "forecast_source",
        "is_in_reliability_scope",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality Checks
# MAGIC Every check is grouped here. RED stops the job, AMBER only warns.
# MAGIC `by_pass_quality_checks=true` downgrades RED to AMBER (debug / backfill only).

# COMMAND ----------

validation_level = "AMBER" if BYPASS_QUALITY_CHECKS else "RED"

df_gx_final = SparkDFDataset(df_transf, persist=False)
df_gx_work_order_out = SparkDFDataset(df_work_order_out, persist=False)
red_quality_check_results = [
    df_gx_final.expect_column_values_to_not_be_null(column=PK_COL),
    df_gx_final.expect_column_values_to_be_unique(column=PK_COL),
    df_gx_work_order_out.expect_column_values_to_not_be_null(column=WO_PK_COL),
    df_gx_work_order_out.expect_column_values_to_be_unique(column=WO_PK_COL),
    # Join keys: no fan-out
    df_gx_work_order.expect_column_values_to_be_unique(column="work_order_number"),
    df_gx_wo_header_t0.expect_compound_columns_to_be_unique(["T0_date", "work_order_number"]),
    df_gx_po_header_t0.expect_compound_columns_to_be_unique(["T0_date", "origin_planned_order_number"]),
    df_gx_scope.expect_compound_columns_to_be_unique(["period_ID", "work_order_number"]),
    df_gx_material.expect_column_values_to_be_unique(column="material_number"),
    df_gx_material_plant.expect_compound_columns_to_be_unique(["material_number", "plant"]),
    df_gx_plant.expect_column_values_to_be_unique(column="plant"),
    df_gx_profit_center.expect_column_values_to_be_unique(column="profit_center"),
]

gx_validation.validate_and_log_gx_results(
    quality_check_results=red_quality_check_results,
    validation_level=validation_level,
    info_message="All checks inspection",
    error_message="Quality checks failed for bom_reliability tables, please check log messages",
    pipeline_write_env=PIPELINE_WRITE_ENV,
    table_name=f"{UC_PREFIX}_bom_reliability_component",
    log_to_event_table=True,
    log_successes=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC AMBER: every phantom exploded within `MAX_PHANTOM_DEPTH` levels; descriptions and base units found;
# MAGIC errors between 0 and 1.

# COMMAND ----------

amber_quality_check_results = [
    df_gx_forecast_bom.expect_column_values_to_be_in_set(column="is_phantom_item", value_set=[False], mostly=0.999),
    df_gx_final.expect_column_values_to_not_be_null(column="component_description", mostly=0.99),
    df_gx_final.expect_column_values_to_not_be_null(column="component_base_unit", mostly=0.99),
    df_gx_final.expect_column_values_to_not_be_null(column="profit_center", mostly=0.99),
    df_gx_final.expect_column_values_to_be_between(
        column="error_percentage_forecast_2_consumption_2", min_value=0, max_value=1
    ),
]

gx_validation.validate_and_log_gx_results(
    quality_check_results=amber_quality_check_results,
    validation_level="AMBER",
    info_message="Phantoms exploded, descriptions populated, errors within [0, 1]",
    error_message="Phantom left unexploded or missing descriptions - check bom_item_history and material master",
    pipeline_write_env=PIPELINE_WRITE_ENV,
    table_name=f"{UC_PREFIX}_bom_reliability_component",
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
    CATALOG = f"{PIPELINE_WRITE_ENV}_proj"
    SCHEMA = DOMAIN

OUTPUTS = [
    (f"{UC_PREFIX}_bom_reliability_component", df_transf),
    (f"{UC_PREFIX}_bom_reliability_work_order", df_work_order_out),
]

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{SCHEMA}")

for destination_table, df_output in OUTPUTS:
    destination_table_full_path = f"{CATALOG}.{SCHEMA}.{destination_table}"
    try:
        startTime = time.time()
        result = table_utils.save_table(
            dest_table=destination_table_full_path,
            df=df_output,
            mode="overwrite",
            overwrite_schema=True,
        )
    except AnalysisException as e:
        log.error(
            f"Table {destination_table_full_path} has not been created, an error occured during saveAsTable. \nError is {e}"
        )
        raise
    else:
        duration = time.time() - startTime
        log.info(f"Table {destination_table_full_path} saved in {duration} seconds.")
        if result is not None:
            result.display()
