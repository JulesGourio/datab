# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # [BOM Quality checks]
# MAGIC
# MAGIC
# MAGIC **Description:**  
# MAGIC This notebook performs quality check on BOM (Bill of materials). Quality checks are written directly by supply user.
# MAGIC
# MAGIC
# MAGIC **Intended Pipeline**
# MAGIC - BOM quality checks
# MAGIC
# MAGIC **Input Tables Pipeline**
# MAGIC - {REFERENCE_READ_ENV}_gold.production.bom_exposed
# MAGIC
# MAGIC **Inputs Reference Data**
# MAGIC - {PIPELINE_READ_ENV}_bronze.manual_input.bom_IS_QC_latest
# MAGIC - {REFERENCE_READ_ENV}_bronze.headers_sap.headers_sap_business_mast
# MAGIC - {REFERENCE_READ_ENV}_bronze.headers_sap.headers_sap_business_stko
# MAGIC - {REFERENCE_READ_ENV}_bronze.headers_sap.header_sap_business_stpo
# MAGIC - {REFERENCE_READ_ENV}_bronze.headers_sap.headers_sap_business_stas
# MAGIC
# MAGIC **Output Tables (Pipeline)**
# MAGIC - {pipeline_write_env}_proj.production.bom_qc

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Standard Package Imports

# COMMAND ----------

import pyspark.sql.functions as f

from leap_utils.common import logger
from leap_utils.data_asset import table_utils
from leap_utils.business_data_consistency import validate_business_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Widgets

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Pipeline Environments
# MAGIC
# MAGIC - PIPELINE_WRITE_ENV -> Used for specifying which catalog environment the script should write data to, NOTE by definition any writing table would mean it is part of the same "pipeline"
# MAGIC - PIPELINE_READ_ENV -> Used for specifying which catalog environment the upstream pipeline data is in.
# MAGIC - REFERENCE_READ_ENV -> Used for specifying which catalog environment that non-pipeline "reference" data is in (generally used in enrichments).

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("pipeline_write_env", "dev")
PIPELINE_WRITE_ENV = dbutils.widgets.get("pipeline_write_env")

dbutils.widgets.text("pipeline_read_env", "dev")
PIPELINE_READ_ENV = dbutils.widgets.get("pipeline_read_env")

dbutils.widgets.text("reference_read_env", "prod")
REFERENCE_READ_ENV = dbutils.widgets.get("reference_read_env")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Debug Boolean
# MAGIC
# MAGIC - DEBUG -> Used for specifying whether or not the script should be in "debug mode" or not. 
# MAGIC - LOG_LEVEL -> Used for specifying what log level is defined inside the notebook

# COMMAND ----------

dbutils.widgets.text("debug", "False")
DEBUG = dbutils.widgets.get("debug").lower() == "true"

dbutils.widgets.dropdown("log_level", defaultValue="info", choices=["debug", "info", "warning", "error", "critical"])
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
# MAGIC ## Inputs - Functions
# MAGIC
# MAGIC Quality Checks function

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inputs - LEAP tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## LEAP Table - Dataset BOM

# COMMAND ----------

df_bom = spark.read.table(f"{REFERENCE_READ_ENV}_gold.production.bom_exposed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LEAP TABLE - Quality Checks Dataset

# COMMAND ----------

df_qc = spark.read.table(f"{PIPELINE_READ_ENV}_bronze.manual_input.bom_is_qc_latest")
common_column = ["rule_idx", "object", "description", "error_type", "ID"]
column_renaming= {}
column_renaming = {column: column.split("__")[-1] for column in df_qc.columns if column not in common_column}
df_qc_renamed = df_qc.withColumnsRenamed(column_renaming).drop("id")

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality Checks 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute

# COMMAND ----------

df_qc_rule = validate_business_data.apply_quality_checks_by_rule(target_df=df_bom, quality_checks_df=df_qc_renamed, column_mapping={})

# COMMAND ----------

# MAGIC %md
# MAGIC # Outputs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output - LEAP Tables

# COMMAND ----------
catalog = f"{PIPELINE_WRITE_ENV}_gold"
domain = "production"
table = "bom_qc"

table_utils.save_table(dest_table=f"{catalog}.{domain}.{table}", df=df_qc_rule, mode="overwrite")
