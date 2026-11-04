# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # As design BOM Bronze to Silver
# MAGIC
# MAGIC
# MAGIC **Description:**  
# MAGIC In this notebook, we aim to create a data asset in Databricks to obtain the complete list of active Bills of Materials (BOMs).
# MAGIC
# MAGIC A Bill of Materials (BOM) is a comprehensive list of raw materials, components, and assemblies required to construct, manufacture, or repair a product or service. 
# MAGIC The BOM includes the quantity of each item and descriptions. 
# MAGIC It is essential for production management and inventory control, ensuring that all necessary materials are available for manufacturing a product.
# MAGIC
# MAGIC In this first notebook, we are directly accessing multiple tables from SAP. We perform data cleaning and renaming of the columns to standardize the dataset. After this, we join these cleaned tables to obtain a comprehensive list of active Bills of Materials (BOMs).
# MAGIC
# MAGIC
# MAGIC **Intended Pipeline**
# MAGIC - As_design_BOM_DataAsset 
# MAGIC
# MAGIC **Inputs Reference Data**
# MAGIC - {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.stpo_latest
# MAGIC - {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.stko_latest
# MAGIC - {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.mast_latest
# MAGIC - {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.stas_latest 
# MAGIC
# MAGIC **Output Tables (Pipeline)**
# MAGIC - {PIPELINE_WRITE_ENV}_silver.production.BOM
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Standard Package Imports
# MAGIC

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### Config LEAP Function Imports

# COMMAND ----------


from leap_utils.data_asset import table_utils

from leap_utils.common import logger


# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Widgets

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Pipeline Environments
# MAGIC
# MAGIC - PIPELINE_WRITE_ENV -> Used for specifying which catalog environment the script should write data to, NOTE by definition any writing table would mean it is part of the same "pipeline"
# MAGIC - REFERENCE_READ_ENV -> Used for specifying which catalog environment that non-pipeline "reference" data is in (generally used in enrichments).

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("pipeline_write_env", "dev")
PIPELINE_WRITE_ENV = dbutils.widgets.get("pipeline_write_env")

dbutils.widgets.text("reference_read_env", "prod")
REFERENCE_READ_ENV = dbutils.widgets.get("reference_read_env")


# COMMAND ----------

# MAGIC %md
# MAGIC # Inputs
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inputs - Manual declarations

# COMMAND ----------

# COLUMN NAME
MATERIAL_NUMBER="material_number"
PLANT="plant"
BOM_NUMBER="BOM_number"
ALTERNATIVE="alternative"
BOM_STATUS="BOM_status"
BOM_TYPE="BOM_type"
CREATION_DATE="creation_date"
QTY_BASE="quantity_base"
NODE="node"
CANCEL="cancel"
ITEM_NUMBER="item_number"
COMPONENT="component"
QUANTITY="quantity"
UNIT="unit"
ITEM_TYPE="item_type"
LAST_MODIF="last_modif"
ALTERNATIVE_ITEM="alternative_item"
PROBABILITY="probability"
STORAGE_LOCATION="storage_location"


# HARD CODED FILTER
FILTER_BOM_TYPE = "M"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inputs - LEAP tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### SAP MAST (link BOM/Mat)
# MAGIC
# MAGIC **df_mast** give all BOM existing with their alternative for couple Material Number / Plant

# COMMAND ----------

df_mast = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.mast_latest"
)
df_mast_headers_sap = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.headers_sap.headers_sap_business_mast"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SAP STKO (Header BOM)
# MAGIC
# MAGIC **df_bom_header** We use STKO Header BOM to exclude BOM inactive (through BOM Status)

# COMMAND ----------

df_bom_header = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.stko_latest"
)
df_bom_header_headers_sap = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.headers_sap.headers_sap_business_stko"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SAP STAS (BOM Item Selection)
# MAGIC
# MAGIC **df_bom_select_noeud** We use STAS table BOM to make link between STKO and STPO using stlkn field

# COMMAND ----------

df_bom_select_noeud = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.stas_latest"
)
df_bom_select_noeud_headers_sap = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.headers_sap.headers_sap_business_stas"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SAP STPO (Item BOM)
# MAGIC
# MAGIC **df_bom_item:** We use STPO Item BOM to obtain all information requested by business

# COMMAND ----------

df_bom_item = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.stpo_latest"
)
df_bom_item_headers_sap = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.headers_sap.header_sap_business_stpo"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Preparation
# MAGIC  
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparation dataAsset MAST
# MAGIC
# MAGIC Rules : 
# MAGIC - keep only  Bom Usage = Fabrication (stlan = 1)
# MAGIC - exclude article with wrong configuration (cslty="X")

# COMMAND ----------

df_mast_prefix = table_utils.add_prefix_to_columns(df_mast, "MAST")
table_utils.check_schema_of_the_header_file(df_mast_headers_sap)
df_column_names = table_utils.rename_columns_with_sap_business_header(
    df_mast_prefix, df_mast_headers_sap
)
df_production_version_renamed = table_utils.cast_columns_with_sap_business_headers(
    df_column_names, df_mast_headers_sap
)
df_mast_cleaned = table_utils.remove_leading_zeros(
    df=df_production_version_renamed, column_names=["material_number"]
)
df_mast_prepared = df_mast_cleaned.filter(
    (f.col("stlan") == "1") & ((f.col("cslty") != "X") | (f.col("cslty").isNull()))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparation dataAsset STKO
# MAGIC
# MAGIC STKO Header is used to exclude BOM inactive (through BOM Status)
# MAGIC rules:
# MAGIC - BOM Type must be "Material BOM" (BOM_type="X")
# MAGIC - Choose last creation_date from a unique BOM_number, alternative, BOM_status

# COMMAND ----------

window_spec = Window.partitionBy(BOM_NUMBER, ALTERNATIVE).orderBy(
    f.col(CREATION_DATE).desc()
)

df_bom_header_prefix = table_utils.add_prefix_to_columns(df_bom_header, "STKO")
table_utils.check_schema_of_the_header_file(df_bom_header_headers_sap)
df_column_names = table_utils.rename_columns_with_sap_business_header(
    df_bom_header_prefix, df_bom_header_headers_sap
)
df_bom_header_renamed = table_utils.cast_columns_with_sap_business_headers(
    df_column_names, df_bom_header_headers_sap
)
df_bom_header_prepared = (
    df_bom_header_renamed.filter(
        (f.col(BOM_STATUS) != "02")
        & (f.col(BOM_STATUS) != "")
        & (f.col(BOM_TYPE) == FILTER_BOM_TYPE)
    )
    .withColumn("row_number", f.row_number().over(window_spec))
    .filter(f.col("row_number") == 1)
    .drop("row_number")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparation dataAsset STAS
# MAGIC
# MAGIC STAS table is used to make link between STKO and STPO using stlkn field.

# COMMAND ----------

df_bom_select_noeud_prefix = table_utils.add_prefix_to_columns(
    df_bom_select_noeud, "STAS"
)
table_utils.check_schema_of_the_header_file(df_bom_select_noeud_headers_sap)
df_column_names = table_utils.rename_columns_with_sap_business_header(
    df_bom_select_noeud_prefix, df_bom_select_noeud_headers_sap
)
df_bom_select_noeud_prefix_renamed = table_utils.cast_columns_with_sap_business_headers(
    df_column_names, df_bom_select_noeud_headers_sap
)
df_bom_select_noeud_prefix_filtered = df_bom_select_noeud_prefix_renamed.filter(
    f.col("BOM_type") == FILTER_BOM_TYPE
)

df_bom_cancel_row = df_bom_select_noeud_prefix_filtered.filter(
    f.col(CANCEL) == "X"
).select(CANCEL, BOM_NUMBER, ALTERNATIVE, NODE)
df_bom_select_noeud = df_bom_select_noeud_prefix_filtered.drop(CANCEL)
df_bom_select_noeud = df_bom_select_noeud.join(
    df_bom_cancel_row, [BOM_NUMBER, ALTERNATIVE, NODE], "left"
)
df_bom_select_noeud_prepared = df_bom_select_noeud.filter(f.col(CANCEL).isNull()).drop(
    CANCEL, BOM_TYPE
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparation dataAsset STPO
# MAGIC STPO Item is used to obtain all information requested by business

# COMMAND ----------

df_bom_item_prefix = table_utils.add_prefix_to_columns(df_bom_item, "STPO")
table_utils.check_schema_of_the_header_file(df_bom_item_headers_sap)
df_column_names = table_utils.rename_columns_with_sap_business_header(
    df_bom_item_prefix, df_bom_item_headers_sap
)
df_bom_item_renamed = table_utils.cast_columns_with_sap_business_headers(
    df_column_names, df_bom_item_headers_sap
)
df_bom_item_cleaned = table_utils.remove_leading_zeros(
    df=df_bom_item_renamed, column_names=["component", "item_number"]
)
df_bom_item_prepared = df_bom_item_cleaned.filter(
    (f.col(BOM_TYPE) == FILTER_BOM_TYPE) & (f.col(COMPONENT).isNotNull())
).drop(BOM_TYPE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparation - Join dataAsset

# COMMAND ----------

# Join MAST with Header BOM
df_bom_standard = df_mast_prepared.join(
    df_bom_header_prepared,
    [BOM_NUMBER, ALTERNATIVE],
    "inner",
)

# Join Header and Item BOM
df_bom_standard_tmp = df_bom_standard.join(
    df_bom_select_noeud_prepared,
    [BOM_NUMBER, ALTERNATIVE],
    "inner",
)
df_bom_standard_joined = df_bom_standard_tmp.join(
    df_bom_item_prepared,
    [BOM_NUMBER, NODE],
    "inner",
)

df_bom_standard_end = (
    df_bom_standard_joined.withColumn(LAST_MODIF, f.to_date(f.col(LAST_MODIF), "yyyyMMdd"))
    .withColumn(CREATION_DATE, f.to_date(f.col(CREATION_DATE), "yyyyMMdd"))
    .withColumn(QUANTITY, f.col(QUANTITY).cast("double"))
    .withColumn(QTY_BASE, f.col(QTY_BASE).cast("double"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Outputs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output - LEAP Tables

# COMMAND ----------

catalog = f"{PIPELINE_WRITE_ENV}_silver"
domain = "production"
table = "BOM"

table_utils.save_table(
    dest_table=f"{catalog}.{domain}.{table}", df=df_bom_standard_end, mode="overwrite", overwrite_schema=True
)
