# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # GOLD WORK ORDER OPERATION
# MAGIC
# MAGIC
# MAGIC **Description:**  
# MAGIC This data asset has been created to centralize and structure all relevant information related to work order operations.
# MAGIC The objective is to provide a trusted, historical, and unified view of manufacturing operations that can be reused across different analytical and reporting use cases.
# MAGIC
# MAGIC
# MAGIC **Highlighted complexities:**  
# MAGIC Enriched from may different tables with different data sources.
# MAGIC
# MAGIC **Intended Pipeline**
# MAGIC - Work order operation SAP Daily
# MAGIC
# MAGIC **Inputs Data**
# MAGIC - {REFERENCE_READ_ENV}_gold.production.work_orders_sap_exposed
# MAGIC - {REFERENCE_READ_ENV}_gold.production.allocated_time_per_wo_sap
# MAGIC - {REFERENCE_READ_ENV}_gold.master_data.wbs_exposed
# MAGIC - {REFERENCE_READ_ENV}_gold.production.workcenter_exposed
# MAGIC - {REFERENCE_READ_ENV}_gold.production.work_order_operation_mes
# MAGIC - {REFERENCE_READ_ENV}_gold.production.hourly_rate_exposed
# MAGIC - {PIPELINE_READ_ENV}_gold.production.sp2_WO_operation_status_exposed
# MAGIC - {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.afru_latest
# MAGIC - {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.afko_latest
# MAGIC - {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.afvc_latest
# MAGIC - {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.jest_latest
# MAGIC - {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.tj02t_latest
# MAGIC
# MAGIC **Output Tables (Pipeline)**
# MAGIC   - {PIPELINE_WRITE_ENV}_gold.production.work_order_operation  
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Technical debt
# MAGIC - PIPELINE_READ_ENV for the work_order_operation_mes dataset instead of REFERENCE_READ_ENV

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Standard Package Imports
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.utils import AnalysisException
import time
from pyspark.sql.types import DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config LEAP Function Imports
# MAGIC

# COMMAND ----------

from leap_utils.data_asset import table_utils

from leap_utils.common import logger
from great_expectations.dataset import SparkDFDataset
from leap_utils.common.validation import gx_validation
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Widgets

# COMMAND ----------

dbutils.widgets.text("pipeline_write_env", "dev")
PIPELINE_WRITE_ENV = dbutils.widgets.get("pipeline_write_env")

dbutils.widgets.text("pipeline_read_env", "dev")
PIPELINE_READ_ENV = dbutils.widgets.get("pipeline_read_env")

dbutils.widgets.text("reference_read_env", "prod")
REFERENCE_READ_ENV = dbutils.widgets.get("reference_read_env")

dbutils.widgets.text("by_pass_quality_checks", "false")
BYPASS_QUALITY_CHECKS = dbutils.widgets.get("by_pass_quality_checks").lower() == "true"

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
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import LEAP tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold tables

# COMMAND ----------

df_work_order = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.production.work_orders_sap_exposed"
)

df_allocated_times = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.production.allocated_time_per_wo_sap"
)

df_wbs_gold = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.master_data.wbs_exposed"
)

df_workcenter_raw = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.production.workcenter_exposed"
)
df_customer_order_raw = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.order_to_cash.customer_order_latest_exposed"
)
df_wo_ope_mes = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.production.work_order_operation_mes"
)

df_hourly_rate_per_cc = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.production.hourly_rate_exposed"
)

df_hqa_clocking = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.production.infocenter_hqa"
)

df_sp2_status_raw = spark.read.table(
    f"{PIPELINE_READ_ENV}_gold.production.sp2_WO_operation_status_exposed"
)

df_mapping_site = spark.read.table('prod_bronze.manual_input.mapping_site_division_region')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze tables for operation status

# COMMAND ----------

df_afru_raw = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.afru_latest"
)
df_afko_raw = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.afko_latest"
)
df_afvc_raw = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.afvc_latest"
)
df_jest_raw = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.jest_latest"
)
df_tj02t_raw = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.tj02t_latest"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Preparation
# MAGIC  
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep1 - Allocated time per work order operation 
# MAGIC This table is used as the reference for this asset of work order operation, and will be re-arranged and enriched to obtain a relevant work order operation table. 

# COMMAND ----------

COL_ALLOC_TABLE = [
    "work_order",
    "work_order_operation",
    "operation_description",
    "is_cancelled",
    "actual_operation_start_date",
    "actual_operation_finish_date",
    "work_center",
    "global_time_allocated_min",
    f.col("global_time_spend_min").alias("spent_time_min"),
    "last_scheduled_start_date",
    "last_scheduled_end_date",
    f.col("afvv_lmnga").alias("operation_quantity_ordered"),
]

# COMMAND ----------

df_allocated_times_prep = (
    df_allocated_times.select(*COL_ALLOC_TABLE)
    .where(f.col("is_cancelled").isNull())
    .drop("is_cancelled")
)

# COMMAND ----------

df_allocated_times_prep = (
    df_allocated_times_prep.withColumn(
        "actual_operation_start_date",
        f.try_to_date(f.col("actual_operation_start_date"), "yyyyMMdd"),
    )
    .withColumn(
        "actual_operation_finish_date",
        f.try_to_date(f.col("actual_operation_finish_date"), "yyyyMMdd"),
    )
    .withColumn(
        "last_scheduled_start_date",
        f.try_to_date(f.col("last_scheduled_start_date"), "yyyyMMdd"),
    )
    .withColumn(
        "last_scheduled_end_date",
        f.try_to_date(f.col("last_scheduled_end_date"), "yyyyMMdd"),
    )
)

# COMMAND ----------

df_allocated_times_prep = df_allocated_times_prep.withColumnRenamed(
    "work_center", "workcenter_code"
).dropDuplicates(["work_order", "work_order_operation"])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep2 - Workcenter & hourly rate
# MAGIC - Workcenter to get description and group per (workcenter; plant) 
# MAGIC - Hourly rate to get a possibility to value the hours spent on an operation based of the workcenter and the fiscal year

# COMMAND ----------

df_workcenter_prep = df_workcenter_raw.drop("workcenter_id").dropDuplicates(
    ["workcenter_code", "plant"]
)

# COMMAND ----------

df_hourly_rate_per_cc_prep = (
    df_hourly_rate_per_cc.where(f.col("cost_center") == f.col("activity_type"))
    .select(
        "cost_center", "fiscal_year", "cost_center_description", "hourly_rate_total_EUR"
    )
    .dropDuplicates()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep3 - WBS table
# MAGIC To get WBS description, the MSN, and the status of the MSN 

# COMMAND ----------

df_wbs_prep = df_wbs_gold.select(
    "WBS_code", "WBS_description", "aircraft_rank", "is_wbs_invoiced"
).distinct()

# COMMAND ----------

df_gx_wbs = SparkDFDataset(df_wbs_gold, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep4 - Operation status 
# MAGIC - JEST table: status system per object number 
# MAGIC - TJ02T table: description of the status code 
# MAGIC - AFVC/AFKO tables: to link the object number to a work order operation   

# COMMAND ----------

df_afko = df_afko_raw.select("aufnr", "aufpl").dropDuplicates()
df_afvc = df_afvc_raw.select("aufpl", "objnr", "vornr").dropDuplicates()
df_jest = (
    df_jest_raw.select("objnr", "stat", "inact", "chgnr")
    .dropDuplicates()
    .where(f.col("inact").isNull())
)
df_tj02t = (
    df_tj02t_raw.select("istat", "spras", "txt04", "txt30")
    .where(f.col("spras") == "E")
    .drop("spras")
    .withColumnRenamed("istat", "stat")
)

df_wo_ope_status = (
    df_afko.join(df_afvc, ["aufpl"], "left")
    .join(df_jest, ["objnr"], "left")
    .join(df_tj02t, ["stat"], how="left")
)

df_wo_ope_status = table_utils.remove_leading_zeros(
    df_wo_ope_status, ["aufnr", "aufpl", "vornr"]
)

# COMMAND ----------

df_wo_ope_status_prep = (
    df_wo_ope_status.withColumnsRenamed(
        {"aufnr": "work_order", "vornr": "work_order_operation"}
    )
    .groupBy("work_order", "work_order_operation")
    .agg(
        f.concat_ws(" ", f.collect_list(f.col("txt04"))).alias(
            "WO_operation_status_system"
        ),
        f.concat_ws(", ", f.collect_list(f.col("txt30"))).alias(
            "WO_operation_status_system_description"
        ),
    )
    .withColumn(
        "is_confirmed",
        f.when(
            f.col("WO_operation_status_system").contains("PCNF"), f.lit(False)
        ).when(
            f.col("WO_operation_status_system").contains("CNF"), f.lit(True)
        ).otherwise(f.lit(False)),
    )
)

# COMMAND ----------

df_gx_ope_status = SparkDFDataset(df_wo_ope_status_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep5 - WO operation MES table
# MAGIC - touchup time 
# MAGIC - aircraft rank (functionality of pairing inside the MES)

# COMMAND ----------

df_wo_ope_mes_prep = df_wo_ope_mes.select(
    "work_order",
    "work_order_operation",
    "touchup_time_sec",
    f.col("aircraft_rank").alias("aircraft_rank_from_mes")
).dropDuplicates(["work_order", "work_order_operation"])

# COMMAND ----------

df_gx_mes = SparkDFDataset(df_wo_ope_mes_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep6 - WO gold table
# MAGIC Clean information relative to the work order (header) 

# COMMAND ----------

COL_WO = [
    f.col("work_order_number").alias("work_order"),
    "material_number",
    "company",
    f.col("production_site").alias("site"),
    f.col("plant"),
    f.col("location").alias("work_order_location"),
    "work_order_type",
    f.col("sales_orders").alias("customer_order"),
    f.col("sales_orders_item").alias("customer_order_item"),
    f.col("start_planned_date").alias("WO_start_planned_date"),
    f.col("end_planned_date").alias("WO_end_planned_date"),
    f.col("real_start_date").alias("WO_actual_start_date"),
    f.col("real_end_date").alias("WO_actual_end_date"),
    f.col("start_scheduled_date").alias("WO_scheduled_start_date"),
    f.col("end_scheduled_date").alias("WO_scheduled_end_date"),
    f.col("quantity_planned").alias("WO_quantity_planned"),
    f.col("quantity_delivered").alias("WO_quantity_delivered"),
    f.col("WBS_code"),
    "profit_center",
    "profit_center_description",
    "customer_group",
    "work_order_type_description",
    "work_order_status",
    "WO_status_system"
]

# COMMAND ----------

df_work_order_prep = df_work_order.select(*COL_WO)

# COMMAND ----------

df_work_order_prep = df_work_order_prep.join(df_mapping_site, ['site'], how='left')

# COMMAND ----------

df_gx_wo = SparkDFDataset(df_work_order_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep7 - HQA table 
# MAGIC Get spent time from HQA per work order operation. 

# COMMAND ----------

df_hqa_clocking_prep = (
    df_hqa_clocking.where(f.col("event") == "PROD")
    .groupby("work_order", f.col("operation").alias("work_order_operation"))
    .agg(f.sum("spent_times_hc").alias("spent_time_from_hqa"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep8 - Quantity ordered and scrapped
# MAGIC From AFRU SAP Table

# COMMAND ----------

AFRU_COLUMNS = ["aufnr", "vornr", "xmnga", "rmzhl", "stokz", "stzhl", "lmnga"]

AFRU_COLUMNS_RENAME = {
    "rmzhl": "compt_id",
    "aufnr": "work_order",
    "vornr": "work_order_operation",
    "xmnga": "operation_quantity_scrapped",
    "lmnga": "operation_quantity_delivered",
    "stokz": "reversed_indicator",
    "stzhl": "cancelled_confirmation",
}


df_afru_prep = (
    df_afru_raw.select(AFRU_COLUMNS)
    .withColumnsRenamed(AFRU_COLUMNS_RENAME)
    .filter(f.col("reversed_indicator").isNull())
    .filter(f.col("cancelled_confirmation") == "00000000")
)

df_afru_prep = table_utils.remove_leading_zeros(
    df=df_afru_prep,
    column_names=["work_order", "work_order_operation"],
)
window_spec = Window.partitionBy("work_order", "work_order_operation").orderBy(
    f.desc("compt_id")
)
df_afru_prep = df_afru_prep.withColumn("rn", f.row_number().over(window_spec))

df_afru_prep = df_afru_prep.filter("rn = 1").drop("rn", "compt_id")

# COMMAND ----------

df_qty_prep = (
    df_afru_prep.withColumn(
        "operation_quantity_delivered",
        f.col("operation_quantity_delivered").cast(DoubleType()),
    )
    .withColumn(
        "operation_quantity_scrapped",
        f.col("operation_quantity_scrapped").cast(DoubleType()),
    )
    .dropDuplicates()
)

# COMMAND ----------

df_gx_qty_prep = SparkDFDataset(df_qty_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prep 9 - SP2 Work Order Operation Status

# COMMAND ----------

# Trim sp2_source_country for safe comparison
df_sp2_status_prep = df_sp2_status_raw.withColumn(
    "sp2_source_country_clean", f.trim(f.col("sp2_source_country"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 1 - Join all tables
# MAGIC - Work order operation (from allocated time table)
# MAGIC - WBS 
# MAGIC - Workcenter
# MAGIC - Hourly rate 
# MAGIC - MES operation table

# COMMAND ----------

df_ope_transf = df_allocated_times_prep.join(
    df_work_order_prep, ["work_order"], how="left"
)

# COMMAND ----------

df_ope_transf = df_ope_transf.join(
    df_workcenter_prep, ["workcenter_code", "plant"], how="left"
)

# COMMAND ----------

df_ope_transf = df_ope_transf.withColumn(
    "fiscal_year",
    f.coalesce(
        f.year(f.col("actual_operation_finish_date")), f.lit(f.year(f.current_date()))
    ),
)

# COMMAND ----------

df_ope_transf = df_ope_transf.join(
    df_hourly_rate_per_cc_prep, ["cost_center", "fiscal_year"], how="left"
)

# COMMAND ----------

df_ope_transf = df_ope_transf.join(df_wbs_prep, ["WBS_code"], how="left")

# COMMAND ----------

df_ope_transf = (
    df_ope_transf.join(
        df_wo_ope_mes_prep, ["work_order", "work_order_operation"], how="left"
    )
    .withColumn(
        "aircraft_rank",
        f.coalesce(f.col("aircraft_rank"), f.col("aircraft_rank_from_mes")),
    )
    .drop("aircraft_rank_from_mes")
)

# COMMAND ----------

df_ope_transf = df_ope_transf.join(
    df_hqa_clocking_prep, ["work_order", "work_order_operation"], how="left"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 2 - Time conversion & ratio per operation calculation

# COMMAND ----------

df_ope_transf = df_ope_transf.withColumn("spent_time_hrs", f.col("spent_time_min") / 60)

# COMMAND ----------

df_ope_transf = df_ope_transf.withColumn(
    "allocated_time_hrs", f.col("global_time_allocated_min") / 60
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Tr. 3 - Operation status
# MAGIC NOT STARTED - IN PROGRESS - CLOSED

# COMMAND ----------

df_ope_transf = df_ope_transf.join(
    df_wo_ope_status_prep, ["work_order", "work_order_operation"], how="left"
).withColumn("is_confirmed", f.coalesce(f.col("is_confirmed"), f.lit(False)))

# COMMAND ----------

df_ope_transf = df_ope_transf.withColumn(
    "operation_status",
    f.when(f.col("is_confirmed") == True, f.lit("CONFIRMED"))
    .when(
        ((f.col("is_confirmed") == False) & (f.col("spent_time_min") > 0))
        | (f.col("WO_operation_status_system").contains("PCNF")),
        f.lit("IN PROGRESS"),
    )
    .otherwise(f.lit("NOT STARTED")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Tr. 4 - Create ID column

# COMMAND ----------

df_ope_transf = df_ope_transf.withColumn(
    "WO_operation_id",
    f.concat_ws("-", f.col("work_order"), f.col("work_order_operation")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Tr. 5 - Add operation quantities information
# MAGIC - Quantity ordered
# MAGIC - Quantity delivered 
# MAGIC - Quantity scrapped  

# COMMAND ----------

df_ope_transf = df_ope_transf.join(
    df_qty_prep, ["work_order", "work_order_operation"], how="left"
).drop('cancelled_confirmation', 'operation_quantity_delivered')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 6 - Add SP2 work order operation status
# MAGIC Use formatted site information to match with SP2 country data

# COMMAND ----------

# Extract site prefix from the MES site column (everything before " - ", trimmed)
df_ope_transf = df_ope_transf.withColumn(
    "site_prefix", 
    f.when(
        f.trim(f.element_at(f.split(f.col("site"), " - "), 1)).isin("LATE-LFR1", "LATE", 'LCA1'), 
        "LFR1"
    ).otherwise(
        f.trim(f.element_at(f.split(f.col("site"), " - "), 1))
    )
)



# COMMAND ----------

join_cond = [
    df_ope_transf["work_order"] == df_sp2_status_prep["work_order_ID"],
    df_ope_transf["work_order_operation"] == df_sp2_status_prep["operation_ID"],
    df_ope_transf["site_prefix"] == df_sp2_status_prep["sp2_source_country_clean"]
    ]

df_ope_transf = df_ope_transf.join(df_sp2_status_prep, join_cond, how="left").drop("work_order_ID", "operation_ID", "sp2_source_country_clean", "site_prefix")

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality Checks

# COMMAND ----------

pk_col = "WO_operation_id"
columns = df_ope_transf.columns
columns.remove(pk_col)
new_column_order = [pk_col] + columns
df_ope_transf = df_ope_transf.select(new_column_order)

# COMMAND ----------

df_to_check = df_ope_transf
col_pk = "WO_operation_id"
validation_level = "RED"
if BYPASS_QUALITY_CHECKS:
    validation_level = "AMBER"
df_gx_final = SparkDFDataset(df_to_check, persist=False)
red_quality_check_results = [
    df_gx_final.expect_column_values_to_not_be_null(column=col_pk),
    df_gx_final.expect_column_values_to_be_unique(column=col_pk),
    df_gx_wbs.expect_column_values_to_be_unique(column="WBS_code"),
    df_gx_wo.expect_column_values_to_be_unique(column="work_order"),
    df_gx_mes.expect_compound_columns_to_be_unique(
        ["work_order", "work_order_operation"]
    ),
    df_gx_ope_status.expect_compound_columns_to_be_unique(
        ["work_order", "work_order_operation"]
    ),
    df_gx_qty_prep.expect_compound_columns_to_be_unique(
        ["work_order", "work_order_operation"]
    ),
]

gx_validation.validate_and_log_gx_results(
    quality_check_results=red_quality_check_results,
    validation_level=validation_level,
    info_message=f"All checks inspection",
    error_message="Quality checks failed for work order operation table, please check log messages",
    pipeline_write_env=PIPELINE_WRITE_ENV,
    table_name="work_order_operation",
    log_to_event_table=True,
    log_successes=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Outputs

# COMMAND ----------

CATALOG = f"{PIPELINE_WRITE_ENV}_gold"
SCHEMA = "production"
DESTINATION_TABLE = "work_order_operation"
DESTINATION_TABLE_FULL_PATH = f"{CATALOG}.{SCHEMA}.{DESTINATION_TABLE}"

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{SCHEMA}")

try:
    startTime = time.time()
    result = table_utils.save_table(
        dest_table=DESTINATION_TABLE_FULL_PATH,
        df=df_ope_transf,
        mode="overwrite",
        overwrite_schema=True,
    )
except AnalysisException as e:
    log.error(
        f"Table {DESTINATION_TABLE_FULL_PATH} has not been created, an error occured during saveAsTable. \nError is {e}"
    )
    raise Exception()
else:
    duration = time.time() - startTime
    log.info(f"Table {DESTINATION_TABLE_FULL_PATH} saved in {duration} seconds.")
    log.info(f"Wrote {df_ope_transf.count()} rows.")
    if result is not None:
        result.display()

# COMMAND ----------

TABLE_CONSTRAINT_NAME = "work_order_operation_pk"
TABLE_PK_COLS = "WO_operation_id"

nb_cons = spark.sql(
    f"select count(*) from system.information_SCHEMA.table_constraints where table_CATALOG='{CATALOG}' and table_SCHEMA='{SCHEMA}' and table_name='{DESTINATION_TABLE}' and constraint_name='{TABLE_CONSTRAINT_NAME}' "
).first()[0]

if nb_cons == 0:
    # Before adding Primary Key, need to set columns NOT NULL
    table_dest_pk_cols_list = TABLE_PK_COLS.split(",")
    for col in table_dest_pk_cols_list:
        table_utils.set_table_column_not_null(
            table_path=DESTINATION_TABLE_FULL_PATH, column=col.strip()
        )

    # Add primary key
    table_utils.add_table_primary_key(
        table_path=DESTINATION_TABLE_FULL_PATH,
        pk_name=TABLE_CONSTRAINT_NAME,
        columns=TABLE_PK_COLS,
    )