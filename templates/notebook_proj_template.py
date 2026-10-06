# Databricks notebook source
# MAGIC %md
# MAGIC # PROJ <USE CASE / ASSET NAME IN CAPITALS>
# MAGIC
# MAGIC **Description:**
# MAGIC <KPIs / flags / aggregations pre-computed for the use case and the PowerBI report.>
# MAGIC
# MAGIC **Highlighted complexities:**
# MAGIC <Scope rules specific to the use case.>
# MAGIC
# MAGIC **Intended Pipeline**
# MAGIC - <job / pipeline name> (task runs after the Gold it reads)
# MAGIC
# MAGIC **Inputs Data** (Gold only, via PIPELINE_WRITE_ENV)
# MAGIC - {PIPELINE_WRITE_ENV}_gold.<domain>.<table>
# MAGIC
# MAGIC **Output Tables (Pipeline)**
# MAGIC - {PIPELINE_WRITE_ENV}_proj.<domain>.uc<number>_<table_name>
# MAGIC
# MAGIC **Calculated columns (business meaning)**
# MAGIC - `is_<flag>`: <meaning>
# MAGIC - `<weighted_numerator>`: <meaning>

# COMMAND ----------

# MAGIC %md
# MAGIC # Technical debt
# MAGIC #N/A

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Standard Package Imports

# COMMAND ----------

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
# MAGIC ### Gold tables - never Silver or Bronze

# COMMAND ----------

DOMAIN = "<domain>"

df_gold_raw = spark.read.table(f"{PIPELINE_WRITE_ENV}_gold.{DOMAIN}.<table>")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep1 - Use-case scope filter
# MAGIC <Inclusion lists of order types / statuses specific to the use case.>

# COMMAND ----------

df_scope = df_gold_raw  # TODO: .filter(...)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 1 - Flags and weighted numerators
# MAGIC Booleans pre-computed for PBI slicers; integer-cast flags so KPIs are summable.

# COMMAND ----------

df_transf = df_scope  # TODO: f.when(...) flags, weighted numerators, optional coarser aggregation

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality Checks

# COMMAND ----------

PK_COL = "<pk_column>"
validation_level = "AMBER" if BYPASS_QUALITY_CHECKS else "RED"

df_gx_final = SparkDFDataset(df_transf, persist=False)
red_quality_check_results = [
    df_gx_final.expect_column_values_to_not_be_null(column=PK_COL),
    df_gx_final.expect_column_values_to_be_unique(column=PK_COL),
]
gx_validation.validate_and_log_gx_results(
    quality_check_results=red_quality_check_results,
    validation_level=validation_level,
    info_message="All checks inspection",
    error_message="Quality checks failed for <table> table, please check log messages",
    pipeline_write_env=PIPELINE_WRITE_ENV,
    table_name="uc<number>_<table_name>",
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
DESTINATION_TABLE = "uc<number>_<table_name>"
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
