# Databricks notebook source
# MAGIC %md
# MAGIC # GOLD <ASSET NAME IN CAPITALS>
# MAGIC
# MAGIC **Description:**
# MAGIC <What this data asset centralises, and the business objective (trusted / historical / unified view …).>
# MAGIC
# MAGIC **Highlighted complexities:**
# MAGIC <Anything a reader must know before touching the code: many sources, special grain, known traps.>
# MAGIC
# MAGIC **Intended Pipeline**
# MAGIC - <job / pipeline name>
# MAGIC
# MAGIC **Inputs Data**
# MAGIC - {REFERENCE_READ_ENV}_gold.<domain>.<reference_table>_exposed
# MAGIC - {PIPELINE_READ_ENV}_gold.<domain>.<other_asset_table>   (only for tables from another data asset of the same pipeline)
# MAGIC - {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.<table>_latest
# MAGIC
# MAGIC **Output Tables (Pipeline)**
# MAGIC - {PIPELINE_WRITE_ENV}_gold.<domain>.<table>
# MAGIC - {PIPELINE_WRITE_ENV}_gold.<domain>.<table>_exposed (view)

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold tables

# COMMAND ----------

# Reference / master-data Gold tables -> REFERENCE_READ_ENV
df_main_raw = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.<domain>.<main_table>_exposed"
)

df_reference_raw = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.<domain>.<reference_table>_exposed"
)

# Table produced by another data asset of the same pipeline -> PIPELINE_READ_ENV
# df_other_raw = spark.read.table(f"{PIPELINE_READ_ENV}_gold.<domain>.<other_table>")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze tables
# MAGIC <Why Bronze is read directly here, e.g. a status that no Gold table exposes yet.>

# COMMAND ----------

df_sap_raw = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.<table>_latest"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep1 - <Main table: reference grain of this asset>
# MAGIC <Explain what this table is used for and how it will be enriched.>

# COMMAND ----------

# Column selection declared once, in an UPPERCASE constant (aliases allowed)
COL_MAIN_TABLE = [
    "<key_1>",
    "<key_2>",
    f.col("<technical_name>").alias("<business_name>"),
]

# COMMAND ----------

df_main_prep = (
    df_main_raw.select(*COL_MAIN_TABLE)
    .where(f.col("is_cancelled").isNull())
)

# COMMAND ----------

# Dates: try_to_date, never a raw to_date
df_main_prep = df_main_prep.withColumn(
    "<some>_date", f.try_to_date(f.col("<some>_date"), "yyyyMMdd")
)

# COMMAND ----------

# Dedup on the join key BEFORE any join (guards against fan-out)
df_main_prep = df_main_prep.dropDuplicates(["<key_1>", "<key_2>"])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep2 - <Reference table>
# MAGIC <Why this table is joined and which attributes it brings.>

# COMMAND ----------

df_reference_prep = df_reference_raw.select(
    "<join_key>", "<enrichment_col>"
).dropDuplicates(["<join_key>"])

# COMMAND ----------

# GX dataset declared next to the prepared table (no Spark action here);
# the check on it runs later, in the grouped "Quality Checks" section.
df_gx_reference = SparkDFDataset(df_reference_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Prep3 - <Bronze-based status / quantity table>
# MAGIC <Source tables, how they link, which rows are kept.>

# COMMAND ----------

# Filter + rename + strip leading zeros (leap_utils) on every identifier used in a join
SAP_COLUMNS = ["<sap_col_1>", "<sap_col_2>"]
SAP_COLUMNS_RENAME = {"<sap_col_1>": "<business_1>", "<sap_col_2>": "<business_2>"}

df_sap_prep = (
    df_sap_raw.select(SAP_COLUMNS)
    .withColumnsRenamed(SAP_COLUMNS_RENAME)
    .filter(f.col("<reversed_indicator>").isNull())
)

df_sap_prep = table_utils.remove_leading_zeros(
    df=df_sap_prep,
    column_names=["<business_1>", "<business_2>"],
)

# COMMAND ----------

# Keep the latest row per key (window + row_number), instead of an arbitrary dropDuplicates
window_spec = Window.partitionBy("<business_1>", "<business_2>").orderBy(f.desc("<counter>"))
df_sap_prep = (
    df_sap_prep.withColumn("rn", f.row_number().over(window_spec))
    .filter("rn = 1")
    .drop("rn", "<counter>")
)

# COMMAND ----------

df_gx_sap = SparkDFDataset(df_sap_prep, persist=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 1 - Join all tables
# MAGIC - Main table (reference grain)
# MAGIC - Reference table
# MAGIC - <other tables>

# COMMAND ----------

# One LEFT join per cell. Risk logged here, checked in "Quality Checks" below.
df_transf = df_main_prep.join(df_reference_prep, ["<join_key>"], how="left")
log.info("Joined <reference_table> on <join_key> - row count / PK uniqueness checked in Quality Checks below")

# COMMAND ----------

# Multi-condition join with a temporary helper column: drop the helper columns afterwards
# join_cond = [
#     df_transf["<a>"] == df_other["<a_other>"],
#     df_transf["<b>"] == df_other["<b_other>"],
# ]
# df_transf = df_transf.join(df_other, join_cond, how="left").drop("<a_other>", "<b_other>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 2 - Business calculations
# MAGIC <Business meaning of each calculated column.>

# COMMAND ----------

df_transf = df_transf.withColumn(
    "<calculated_column>",
    f.when(f.col("<cond>"), f.lit("<A>")).otherwise(f.lit("<B>")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 3 - Amount correction and currency conversion
# MAGIC Only if the asset carries monetary amounts (see the LEAP guidelines, Gold layer).
# MAGIC #N/A

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr. 4 - Create ID column and put it first

# COMMAND ----------

PK_COL = "<table>_ID"  # or keep the natural column if it is already unique

df_transf = df_transf.withColumn(
    PK_COL, f.concat_ws("-", f.col("<key_1>"), f.col("<key_2>"))
)
df_transf = df_transf.select(
    [PK_COL] + [c for c in df_transf.columns if c != PK_COL]
)

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
    # Join keys of every prepared reference table: no fan-out
    df_gx_reference.expect_column_values_to_be_unique(column="<join_key>"),
    df_gx_sap.expect_compound_columns_to_be_unique(["<business_1>", "<business_2>"]),
]

gx_validation.validate_and_log_gx_results(
    quality_check_results=red_quality_check_results,
    validation_level=validation_level,
    info_message="All checks inspection",
    error_message="Quality checks failed for <table> table, please check log messages",
    pipeline_write_env=PIPELINE_WRITE_ENV,
    table_name="<table>",
    log_to_event_table=True,
    log_successes=True,
)

# COMMAND ----------

# AMBER checks (optional columns, key dimensions, low fill rates with mostly=...)
# amber_results = [df_gx_final.expect_column_values_to_not_be_null(column="<dimension>")]
# gx_validation.validate_and_log_gx_results(quality_check_results=amber_results, validation_level="AMBER", ...)

# COMMAND ----------

# MAGIC %md
# MAGIC # Outputs

# COMMAND ----------

if LAB_TARGET_SCHEMA:
    CATALOG, SCHEMA = LAB_TARGET_SCHEMA.split(".")
else:
    CATALOG = f"{PIPELINE_WRITE_ENV}_gold"
    SCHEMA = "<domain>"
DESTINATION_TABLE = "<table>"
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exposed view
# MAGIC Technical (raw-prefix) columns are excluded. Remove this block only if the DAS says no exposed view.

# COMMAND ----------

excluded_prefixes = ["<RAW_PREFIX>"]  # raw source-table prefixes, technical only
selected_columns = [
    c for c in df_transf.columns if not any(c.startswith(p) for p in excluded_prefixes)
]
table_utils.create_table_view(
    src_full_tablename=DESTINATION_TABLE_FULL_PATH,
    dest_full_tablename=f"{DESTINATION_TABLE_FULL_PATH}_exposed",
    select_statement=", ".join(selected_columns),
    replace_view=True,
)
