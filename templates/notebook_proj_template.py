# Databricks notebook source
# MAGIC %md
# MAGIC # 1. SCRIPT OVERVIEW
# MAGIC **Script:** `create_proj_<asset_name>.py` — Layer: Proj (use case UC<number>)
# MAGIC
# MAGIC **Purpose:** <KPIs / flags pre-computed for the PowerBI report>
# MAGIC
# MAGIC **Inputs** (Gold only, via PIPELINE_WRITE_ENV)
# MAGIC - `{PIPELINE_WRITE_ENV}_gold.<domain>.<table>`
# MAGIC
# MAGIC **Outputs**
# MAGIC - `{PIPELINE_WRITE_ENV}_proj.<domain>.uc<number>_<table_name>`
# MAGIC
# MAGIC **Calculated columns (business meaning)**
# MAGIC - `is_<flag>`: <meaning>
# MAGIC - `<weighted_numerator>`: <meaning>

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. TECHNICAL DEBT
# MAGIC #N/A

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. CONFIGURATION
# MAGIC Same imports / widgets / logger as the Silver template.

# COMMAND ----------

import pyspark.sql.functions as f
from leap_utils.data_asset import table_utils
from leap_utils.common import logger
from leap_utils.common.validation import gx_validation
from great_expectations.dataset import SparkDFDataset

dbutils.widgets.removeAll()
dbutils.widgets.text("pipeline_write_env", "dev")
dbutils.widgets.text("pipeline_read_env", "dev")
dbutils.widgets.text("reference_read_env", "prod")
dbutils.widgets.text("debug", "False")
dbutils.widgets.dropdown("log_level", defaultValue="info",
                         choices=["debug", "info", "warning", "error", "critical"])

PIPELINE_WRITE_ENV = dbutils.widgets.get("pipeline_write_env")
PIPELINE_READ_ENV = dbutils.widgets.get("pipeline_read_env")
REFERENCE_READ_ENV = dbutils.widgets.get("reference_read_env")

# TODO: configure `log` as in the closest sibling notebook
log = ...

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. INPUTS
# MAGIC ## 4.1 Gold table — never Silver or Bronze

# COMMAND ----------

domain = "<domain>"
df_gold = spark.read.table(f"{PIPELINE_WRITE_ENV}_gold.{domain}.<table>")

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. INPUT QUALITY CHECKS
# MAGIC #N/A

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. DATA PREPARATION
# MAGIC ### 6.1 Use-case scope filter

# COMMAND ----------

df_scope = df_gold  # TODO: .filter(...) inclusion lists specific to the use case

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. DATA TRANSFORMATIONS
# MAGIC ### 7.1 Flags and weighted numerators (integer-cast for summable KPIs)

# COMMAND ----------

df_proj = df_scope  # TODO: flags, weighted numerators, optional coarser aggregation

# COMMAND ----------

# MAGIC %md
# MAGIC # 8. QUALITY CHECKS

# COMMAND ----------

pk_col = "<pk_column>"
dfgx = SparkDFDataset(df_proj, persist=False)
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

# COMMAND ----------

table_utils.save_table(dest_table=f"{PIPELINE_WRITE_ENV}_proj.{domain}.uc<number>_<table_name>",
                       df=df_proj, mode="overwrite", overwrite_schema=True)
