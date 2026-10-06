# Databricks notebook source
# MAGIC %md
# MAGIC # 1. SCRIPT OVERVIEW
# MAGIC **Script:** `create_gold_<asset_name>.py` — Layer: Gold
# MAGIC
# MAGIC **Purpose:** <business purpose>
# MAGIC
# MAGIC **Pipeline:** <job / data asset>
# MAGIC
# MAGIC **Inputs**
# MAGIC - `{PIPELINE_WRITE_ENV}_silver.<domain>.<table>` (own Silver)
# MAGIC - `{REFERENCE_READ_ENV}_gold.<domain>.<reference_table>` (reference / master data)
# MAGIC
# MAGIC **Outputs**
# MAGIC - `{PIPELINE_WRITE_ENV}_gold.<domain>.<table>`
# MAGIC - `{PIPELINE_WRITE_ENV}_gold.<domain>.<table>_exposed` (view)
# MAGIC
# MAGIC **Calculated columns (business meaning)**
# MAGIC - `<column>`: <meaning>

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. TECHNICAL DEBT
# MAGIC #N/A

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. CONFIGURATION
# MAGIC Same imports / widgets / logger as the Silver template (see `notebook_silver_template.py`).

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
# MAGIC ## 4.1 Own Silver table (PIPELINE_WRITE_ENV)

# COMMAND ----------

table = "<table>"
domain = "<domain>"
df_silver = spark.read.table(f"{PIPELINE_WRITE_ENV}_silver.{domain}.{table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Reference table (REFERENCE_READ_ENV)

# COMMAND ----------

df_ref = spark.read.table(f"{REFERENCE_READ_ENV}_gold.<domain>.<reference_table>")

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. INPUT QUALITY CHECKS
# MAGIC #N/A

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. DATA PREPARATION
# MAGIC ### 6.1 Pre-filter and dedup the reference table (guards against join fan-out)

# COMMAND ----------

df_ref_prep = (
    df_ref
    # .filter(f.col("spras") == "E")
    .select("<join_key>", "<enrichment_col>")
    .dropDuplicates(["<join_key>"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. DATA TRANSFORMATIONS
# MAGIC ### 7.1 Business logic (calculated columns / flags)

# COMMAND ----------

df_gold = df_silver  # TODO: f.when(...).otherwise(...), f.coalesce(...)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Enrichment join (LEFT) — risk logged, checked in section 8

# COMMAND ----------

df_gold = df_gold.join(df_ref_prep, ["<join_key>"], "left")
log.info("Joined <reference_table> on <join_key> — fan-out (RED) and null rate (AMBER) checked in Quality Checks below")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Amount correction and currency conversion (only if amounts exist)
# MAGIC Order: `amount_correction.create_corrected_amounts` → currency coverage risk logged →
# MAGIC `table_utils.currency_conversion` (Consolidation + Budget + Spot). Prefer document date over `current_date()`.
# MAGIC #N/A

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.4 Primary key (first column)

# COMMAND ----------

pk_col = f"{table}_ID"  # or keep the natural column if already unique
# TODO: build with f.concat_ws("-", ...) and reorder to first column

# COMMAND ----------

# MAGIC %md
# MAGIC # 8. QUALITY CHECKS

# COMMAND ----------

dfgx = SparkDFDataset(df_gold, persist=False)

amber_results = [
    # dfgx.expect_column_values_to_not_be_null(column="<optional_or_joined_col>"),
]
gx_validation.validate_and_log_gx_results(
    quality_check_results=amber_results, validation_level="AMBER",
    info_message="<expected>", error_message="<what to check>",
)

red_results = [
    dfgx.expect_column_values_to_not_be_null(column=pk_col),
    dfgx.expect_column_values_to_be_unique(column=pk_col),  # also catches join fan-out
]
gx_validation.validate_and_log_gx_results(
    quality_check_results=red_results, validation_level="RED",
    info_message=f"{pk_col} unique and not null", error_message="PK / fan-out check failed",
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 9. OUTPUTS
# MAGIC ## 9.1 Save table + PK constraint `gold_{table}_PK`

# COMMAND ----------

table_utils.save_table(dest_table=f"{PIPELINE_WRITE_ENV}_gold.{domain}.{table}",
                       df=df_gold, mode="overwrite", overwrite_schema=True)
# TODO: register NOT NULL + gold_{table}_PK

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.2 Exposed view

# COMMAND ----------

excluded_prefixes = ["<RAW_PREFIX>"]  # raw source-table prefixes, technical only
selected_columns = [c for c in df_gold.columns if not any(c.startswith(p) for p in excluded_prefixes)]
table_utils.create_table_view(
    src_full_tablename=f"{PIPELINE_WRITE_ENV}_gold.{domain}.{table}",
    dest_full_tablename=f"{PIPELINE_WRITE_ENV}_gold.{domain}.{table}_exposed",
    select_statement=", ".join(selected_columns),
    replace_view=True,
)
