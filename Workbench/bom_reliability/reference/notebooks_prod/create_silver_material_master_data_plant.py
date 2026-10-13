# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Purpose
# MAGIC
# MAGIC **Description:**  
# MAGIC The aim is to create a silver table for Material Master Data by plant and Domain Prices  
# MAGIC This Table will contain all Parameters linked with plant details (MRP, Procurement,...)
# MAGIC
# MAGIC **Output table(s):**
# MAGIC   - {WRITE_ENV}_silver.master_data.material_master_data_plant  
# MAGIC **Input :**
# MAGIC   - {READ_ENV}_bronze.sap_latecoere_ecc6.marc_latest
# MAGIC   - {READ_ENV}_bronze.sap_latecoere_ecc6.mbew_latest
# MAGIC
# MAGIC **Triggered by:**
# MAGIC  - SAP Bronze Ingestion
# MAGIC
# MAGIC **Things performed in this NoteBook**
# MAGIC
# MAGIC - Join Plant Material with prices by Domain
# MAGIC - Renaming Headers
# MAGIC - Cleaning raw data

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql import DataFrame

from leap_utils.data_asset import table_utils

dbutils.widgets.removeAll()
# Get read environment variable
dbutils.widgets.text("READ_ENV", "prod")
READ_ENV = dbutils.widgets.get("READ_ENV")
# Get write environment variable
dbutils.widgets.text("WRITE_ENV", "dev")
WRITE_ENV = dbutils.widgets.get("WRITE_ENV")

# COMMAND ----------

# MAGIC %md
# MAGIC #Functions

# COMMAND ----------

# Check Header from Manual Input file
def check_schema_of_the_file(df_names: DataFrame):
    """Check if we have the right columns"""
    if df_names.columns == ["Table_name", "Field", "Business_name", "Format"]:
        print("Correct schema")
    else:
        raise Exception(
            f"Bad Table schema. Expecting 4 columns, Table_name, Field, Business_name and Format"
        )


# --------------------------------------------------------------------------------------------------------------

# Delete rows when field is null
def delete_null_rows(df_names: DataFrame) -> DataFrame:
    """Delete rows where the field is null"""
    df_names_new = df_names.filter(f.col("Field").isNotNull() == True)
    return df_names_new


# COMMAND ----------

# MAGIC %md
# MAGIC #Upload Table and Renaming

# COMMAND ----------

# Upload MARC <- Master Data material by Plant from SAP
df_marc= spark.read.table(f"{READ_ENV}_bronze.sap_latecoere_ecc6.marc_latest")
df_marc = table_utils.add_prefix_to_columns(df_marc, "MARC")  # <- Renaming // MARC_XXXX
#Exclusion Temporary of SPLIT management
df_marc = df_marc.filter(f.col("MARC_matnr")!="SPLIT")

# Upload MBEW <- Price Master Data material by Plant from SAP
df_mbew= spark.read.table(f"{READ_ENV}_bronze.sap_latecoere_ecc6.mbew_latest")
df_mbew = table_utils.add_prefix_to_columns(df_mbew, "MBEW")  # <- Renaming // MBEW_XXXX


# COMMAND ----------

# MAGIC %md
# MAGIC #Join Material

# COMMAND ----------

df_material = (df_marc.alias("A")
               .join(df_mbew.alias("B"),(f.col("A.MARC_matnr")==f.col("B.MBEW_matnr"))&(f.col("A.MARC_werks")==f.col("B.MBEW_bwkey")),"left")
            .drop("MBEW_matnr","MBEW_bwkey")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Filter on Master Data Material

# COMMAND ----------

# Exclusion Material cancelled
df_material = df_material.filter(f.col("MARC_lvorm").isNull())
# Exclusion Plant 1500 created by mistake
df_material = df_material.filter(f.col("MARC_werks")!="1500")
# Exclusion Valorisation Test
df_material = df_material.filter(f.col("MBEW_bwtar").isNull())

# COMMAND ----------

# MAGIC %md
# MAGIC #Renaming according Header files

# COMMAND ----------

# -----------------------------------------------------------------------------------
## Renaming MAKT - Description Material from SAP
df_name_marc = spark.read.table("prod_bronze.headers_sap.headers_sap_business_marc")
# Call function Check Header files
check_schema_of_the_file(df_name_marc)
# Call Function Delete rows null
df_name_marc = delete_null_rows(df_name_marc)

# -----------------------------------------------------------------------------------
## Renaming MBEW - Price Material from SAP
df_name_mbew = spark.read.table("prod_bronze.headers_sap.headers_sap_business_mbew")
# Call function Check Header files
check_schema_of_the_file(df_name_mbew)
# Call Function Delete rows null
df_name_mbew = delete_null_rows(df_name_mbew)

# Union Marc-MBEW
df_names_all = df_name_marc.unionByName(df_name_mbew)

# Call Function Rename Columns according header file
df_column_names = table_utils.rename_columns_with_sap_business_header(
    df_material, df_names_all
)
# Call Funtion Create Columns on Date format
df_material_renamed = table_utils.cast_columns_with_sap_business_headers(
    df_column_names, df_names_all
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Cleasing raw data from material data asset
# MAGIC

# COMMAND ----------

# Delete leading zeros on Material_Number
df_material_end = df_material_renamed.withColumn(
    "Material_Number", f.regexp_replace(df_material_renamed.Material_Number, "^0+", "")
)
# Delete leading zeros on Profir_Center
df_material_end = df_material_end.withColumn(
    "Profit_Center", f.regexp_replace(df_material_end.Profit_Center, "^0+", "")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Save Silver table

# COMMAND ----------

catalog = f"{WRITE_ENV}_silver"
domain = "master_data"
table = "material_master_data_plant"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{domain}")

table_utils.save_table(
    dest_table=f"{catalog}.{domain}.{table}",
    df=df_material_end,
    mode="overwrite",
    overwrite_schema=True
)