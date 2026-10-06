# Databricks notebook source
# MAGIC %md
# MAGIC # 1. SCRIPT OVERVIEW
# MAGIC **Script:** `create_silver_<asset_name>.py` — Layer: Silver
# MAGIC
# MAGIC **Purpose:** <what this notebook does, business-wise>
# MAGIC
# MAGIC **Pipeline:** <job / data asset it belongs to>
# MAGIC
# MAGIC **Inputs**
# MAGIC - `{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.<table>`
# MAGIC - `{REFERENCE_READ_ENV}_bronze.headers_sap.headers_sap_<table>`
# MAGIC
# MAGIC **Outputs**
# MAGIC - `{PIPELINE_WRITE_ENV}_silver.<domain>.<table>`

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. TECHNICAL DEBT
# MAGIC #N/A

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. CONFIGURATION
# MAGIC ## 3.1 Imports

# COMMAND ----------

import pyspark.sql.functions as f
from leap_utils.data_asset import table_utils
from leap_utils.common import logger
from leap_utils.common.validation import gx_validation
from great_expectations.dataset import SparkDFDataset

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Widgets and environments

# COMMAND ----------

dbutils.widgets.removeAll()  # must be the first widget call
dbutils.widgets.text("pipeline_write_env", "dev")
dbutils.widgets.text("pipeline_read_env", "dev")
dbutils.widgets.text("reference_read_env", "prod")
dbutils.widgets.text("debug", "False")
dbutils.widgets.dropdown("log_level", defaultValue="info",
                         choices=["debug", "info", "warning", "error", "critical"])

PIPELINE_WRITE_ENV = dbutils.widgets.get("pipeline_write_env")
PIPELINE_READ_ENV = dbutils.widgets.get("pipeline_read_env")
REFERENCE_READ_ENV = dbutils.widgets.get("reference_read_env")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Logger

# COMMAND ----------

# TODO: configure `log` exactly as in the closest sibling notebook (leap_utils.common.logger)
log = ...

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. INPUTS
# MAGIC Each source table is read **once**, here only.
# MAGIC ## 4.1 <table> (Bronze)

# COMMAND ----------

table = "<table>"
domain = "<domain>"

df_source_raw = spark.read.table(f"{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.{table}")
df_header = spark.read.table(f"{REFERENCE_READ_ENV}_bronze.headers_sap.headers_sap_{table}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. INPUT QUALITY CHECKS
# MAGIC #N/A (TBD — freshness / completeness checks not yet standardised)

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. DATA PREPARATION
# MAGIC ### 6.1 Prefix columns (before the SAP header rename, for later join disambiguation)

# COMMAND ----------

df = table_utils.add_prefix_to_columns(df_source_raw, "<SOURCE_PREFIX>")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Rename with SAP business header

# COMMAND ----------

df = table_utils.rename_columns_with_sap_business_header(df, df_header)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 SAP trailing minus on every float/double column (between rename and cast)

# COMMAND ----------

# TODO: apply table_utils.transform_float_column() to every float/double column

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Cast types (try_cast / safe_to_date semantics — never a raw cast)

# COMMAND ----------

df = table_utils.cast_columns_with_sap_business_headers(df, df_header)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.5 Strip leading zeros from document / master-data identifiers

# COMMAND ----------

# TODO: strip leading zeros (both sides of any planned join)

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. DATA TRANSFORMATIONS
# MAGIC Silver keeps every source column: no `.select()` / `.drop()`.
# MAGIC ### 7.1 Primary key

# COMMAND ----------

# Composite key example — {table}_ID, first column. Skip if a natural column is already unique.
pk_col = f"{table}_ID"
df = df.withColumn(pk_col, f.concat_ws("-", f.col("<col1>"), f.col("<col2>")))
columns = df.columns
columns.remove(pk_col)
df_silver = df.select([pk_col] + columns)

# COMMAND ----------

# MAGIC %md
# MAGIC # 8. QUALITY CHECKS
# MAGIC All checks grouped here, none inline. RED = halt, AMBER = warn.

# COMMAND ----------

dfgx = SparkDFDataset(df_silver, persist=False)

amber_results = [
    # dfgx.expect_column_values_to_not_be_null(column="<key_dimension>"),
]
gx_validation.validate_and_log_gx_results(
    quality_check_results=amber_results, validation_level="AMBER",
    info_message="<what is expected>", error_message="<what to look at>",
)

red_results = [
    dfgx.expect_column_values_to_not_be_null(column=pk_col),
    dfgx.expect_column_values_to_be_unique(column=pk_col),
]
gx_validation.validate_and_log_gx_results(
    quality_check_results=red_results, validation_level="RED",
    info_message=f"{pk_col} unique and not null", error_message="PK check failed",
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 9. OUTPUTS
# MAGIC ## 9.1 Save table + PK constraint

# COMMAND ----------

table_utils.save_table(dest_table=f"{PIPELINE_WRITE_ENV}_silver.{domain}.{table}",
                       df=df_silver, mode="overwrite", overwrite_schema=True)

# TODO: table_utils.set_table_column_not_null(...) then table_utils.add_table_primary_key(...)
#       constraint name: silver_{table}_PK
