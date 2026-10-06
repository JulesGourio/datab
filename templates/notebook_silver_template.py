# Databricks notebook source
# MAGIC %md
# MAGIC # SILVER <ASSET NAME IN CAPITALS>
# MAGIC
# MAGIC **Description:**
# MAGIC <What this Silver table contains: cleansed / renamed / typed source, grain.>
# MAGIC
# MAGIC **Highlighted complexities:**
# MAGIC <Special grain, SAP quirks, known traps.>
# MAGIC
# MAGIC **Intended Pipeline**
# MAGIC - <job / pipeline name>
# MAGIC
# MAGIC **Inputs Data**
# MAGIC - {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.<table>_latest
# MAGIC - {REFERENCE_READ_ENV}_bronze.headers_sap.headers_sap_<table>
# MAGIC
# MAGIC **Output Tables (Pipeline)**
# MAGIC - {PIPELINE_WRITE_ENV}_silver.<domain>.<table>

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
# MAGIC Each source table is read once, here only.

# COMMAND ----------

TABLE = "<table>"
DOMAIN = "<domain>"

df_source_raw = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.{TABLE}_latest"
)
df_header = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.headers_sap.headers_sap_{TABLE}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep1 - Prefix and SAP header rename
# MAGIC Prefix first (join disambiguation), then technical SAP names -> business names.

# COMMAND ----------

df_prep = table_utils.add_prefix_to_columns(df_source_raw, "<SOURCE_PREFIX>")
df_prep = table_utils.rename_columns_with_sap_business_header(df_prep, df_header)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep2 - SAP negative signs and type casting
# MAGIC `transform_float_column` on EVERY float/double column, between rename and cast (SAP stores `525.00-`).

# COMMAND ----------

# TODO: table_utils.transform_float_column(...) on every float/double column
df_prep = table_utils.cast_columns_with_sap_business_headers(df_prep, df_header)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep3 - Leading zeros
# MAGIC Strip from every document / master-data identifier, on both sides of any future join.

# COMMAND ----------

df_prep = table_utils.remove_leading_zeros(
    df=df_prep,
    column_names=["<identifier_1>", "<identifier_2>"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformations
# MAGIC Silver keeps every source column: no `.select()` / `.drop()`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 1 - Create ID column and put it first
# MAGIC Composite key `{table}_ID`; skip if a natural column is already unique.

# COMMAND ----------

PK_COL = f"{TABLE}_ID"

df_transf = df_prep.withColumn(
    PK_COL, f.concat_ws("-", f.col("<key_1>"), f.col("<key_2>"))
)
df_transf = df_transf.select(
    [PK_COL] + [c for c in df_transf.columns if c != PK_COL]
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality Checks
# MAGIC Every check is grouped here. RED stops the job, AMBER only warns.

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
    info_message="All checks inspection",
    error_message=f"Quality checks failed for {TABLE} table, please check log messages",
    pipeline_write_env=PIPELINE_WRITE_ENV,
    table_name=TABLE,
    log_to_event_table=True,
    log_successes=True,
)

# COMMAND ----------

# AMBER checks (key dimensions not null, low fill rates with mostly=...)
# amber_results = [df_gx_final.expect_column_values_to_not_be_null(column="<dimension>")]

# COMMAND ----------

# MAGIC %md
# MAGIC # Outputs

# COMMAND ----------

if LAB_TARGET_SCHEMA:
    CATALOG, SCHEMA = LAB_TARGET_SCHEMA.split(".")
else:
    CATALOG = f"{PIPELINE_WRITE_ENV}_silver"
    SCHEMA = DOMAIN
DESTINATION_TABLE = TABLE
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

TABLE_CONSTRAINT_NAME = f"silver_{DESTINATION_TABLE}_PK"
TABLE_PK_COLS = PK_COL  # comma-separated if several

nb_cons = spark.sql(
    f"select count(*) from system.information_SCHEMA.table_constraints "
    f"where table_CATALOG='{CATALOG}' and table_SCHEMA='{SCHEMA}' "
    f"and table_name='{DESTINATION_TABLE}' and constraint_name='{TABLE_CONSTRAINT_NAME}'"
).first()[0]

if nb_cons == 0:
    # Columns must be NOT NULL before adding a primary key
    for col in TABLE_PK_COLS.split(","):
        table_utils.set_table_column_not_null(
            table_path=DESTINATION_TABLE_FULL_PATH, column=col.strip()
        )
    table_utils.add_table_primary_key(
        table_path=DESTINATION_TABLE_FULL_PATH,
        pk_name=TABLE_CONSTRAINT_NAME,
        columns=TABLE_PK_COLS,
    )
