# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # As design BOM Silver to gold
# MAGIC
# MAGIC
# MAGIC **Description:**
# MAGIC In this notebook, we aim to create a data asset in Databricks to obtain the complete list of active Bills of Materials (BOMs).
# MAGIC
# MAGIC A Bill of Materials (BOM) is a comprehensive list of raw materials, components, and assemblies required to construct, manufacture, or repair a product or service.
# MAGIC The BOM includes the quantity of each item and descriptions.
# MAGIC It is essential for production management and inventory control, ensuring that all necessary materials are available for manufacturing a product.
# MAGIC
# MAGIC In this notebook, we focus on performing quality checks on the dataset obtained from the silver steps. This involves verifying the integrity, consistency, and accuracy of the data.
# MAGIC
# MAGIC
# MAGIC **Intended Pipeline**
# MAGIC - As_design_BOM_DataAsset
# MAGIC
# MAGIC **Input Tables Pipeline**
# MAGIC - {PIPELINE_READ_ENV}_silver.production.BOM
# MAGIC
# MAGIC **Output Tables (Pipeline)**
# MAGIC - {PIPELINE_WRITE_ENV}_gold.production.BOM
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
import great_expectations as gx

# COMMAND ----------

# MAGIC %md
# MAGIC ### Config LEAP Function Imports

# COMMAND ----------


from leap_utils.data_asset import table_utils

from leap_utils.common.validation import gx_validation


# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Widgets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Environments
# MAGIC
# MAGIC - PIPELINE_WRITE_ENV -> Used for specifying which catalog environment the script should write data to, NOTE by definition any writing table would mean it is part of the same "pipeline"
# MAGIC - PIPELINE_READ_ENV -> Used for specifying which catalog environment the upstream pipeline data is in.

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
# MAGIC # Inputs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inputs - LEAP tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### DF BOM silver

# COMMAND ----------

df_silver = spark.read.table(f"{PIPELINE_READ_ENV}_silver.production.BOM")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Material Master data
# MAGIC

# COMMAND ----------

df_material_master_component = (
    spark.read.table(
        f"{REFERENCE_READ_ENV}_gold.master_data.material_master_data_plant"
    )
    .withColumnsRenamed(
        {
            "Material_Number": "component",
            "MARC_rgekz": "backflush",
            "Storage_Location": "Storage_Location_MD",
            "Material_Plant_Status": "Component_Plant_Status",
        }
    )
    .select(
        "component",
        "plant",
        "Storage_Location_MD",
        "Component_Plant_Status",
        "backflush",
        "MRP_Unit",
    )
)

df_material_master_material = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.master_data.material_master_data_plant"
).select("Material_Number", "plant", "Material_Plant_Status", "MRP_Unit")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data transformation
# MAGIC - Join with material master data to add useful info
# MAGIC - Use the probability of usage to determine the quantity of each component (IS used probability to determine quantity on each components).
# MAGIC - Divide the quantity by the base quantity to obtain a Unit BOM (Bill of Materials).
# MAGIC - Create a business key that includes the BOM number, item number, and material number.

# COMMAND ----------

df_bom_std = (
    df_silver.alias("a")
    .join(
        df_material_master_component.alias("b"),
        on=(f.col("b.MRP_Unit") == f.col("a.plant"))
        & (f.col("b.component") == f.col("a.component"))
        & (f.col("b.plant") == f.col("a.plant")),
        how="left",
    )
    .drop(f.col("b.component"), f.col("b.plant"), f.col("b.MRP_Unit"))
)

df_bom_std = (
    df_bom_std.alias("a")
    .join(
        df_material_master_material.alias("b"),
        on=(f.col("b.MRP_Unit") == f.col("a.plant"))
        & (f.col("b.Material_Number") == f.col("a.Material_Number"))
        & (f.col("b.plant") == f.col("a.plant")),
        how="left",
    )
    .drop(f.col("b.Material_Number"), f.col("b.plant"), f.col("b.MRP_Unit"))
)

df_bom_std_info = df_bom_std.withColumn(
    "Storage_Location",
    f.when(f.col("storage_location").isNull(), f.col("Storage_Location_MD")).otherwise(
        f.col("Storage_Location")
    ),
).drop("Storage_Location_MD")

# COMMAND ----------

df_bom_standard_end = df_bom_std_info.withColumn(
    "quantity",
    f.when(
        f.col("alternative_item") == "X", f.col("quantity") * f.col("probability") / 100
    ).otherwise(f.col("quantity")),
)
df_bom_standard_end = df_bom_standard_end.withColumn(
    "quantity", f.col("quantity") / f.col("quantity_base")
).drop("quantity_base", "alternative_item")

# COMMAND ----------

df_silver_business = df_bom_standard_end.withColumn(
    "business_id",
    f.concat_ws(
        "_",
        f.col("BOM_number"),
        f.col("plant"),
        f.col("item_number"),
        f.col("material_number"),
        f.col("alternative"),
        f.col("node"),
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Quality Checks
# MAGIC
# MAGIC **Data Type Check:**
# MAGIC
# MAGIC creation_date and last_modif Columns: We checked that these columns are of the date type to ensure that date-related operations can be performed accurately.
# MAGIC quantity Column: We checked that this column is of the double (floating-point) type to ensure precise numerical operations.
# MAGIC
# MAGIC
# MAGIC **Data Format Check:**
# MAGIC
# MAGIC plant Column: We verified that the values in this column are exactly 4 digits long, ensuring consistency in plant codes.
# MAGIC BOM_number Column: We checked that the values in this column are exactly 8 digits long, ensuring proper format for BOM identification.
# MAGIC node Column: We verified that the values in this column are exactly 8 digits long, ensuring uniformity in node identification.
# MAGIC
# MAGIC **Unicity Check**
# MAGIC
# MAGIC business_id needs to be unique

# COMMAND ----------

# Wrap the dataframe with the great expectations object, use persist=True just if it increases the speed of execution
dfgx = gx.dataset.SparkDFDataset(df_silver_business, persist=False)

# Define the red quality checks here
red_quality_check_results = [
    dfgx.expect_column_values_to_be_of_type(column="creation_date", type_="DateType"),
    dfgx.expect_column_values_to_be_of_type(column="last_modif", type_="DateType"),
    dfgx.expect_column_values_to_be_of_type(column="quantity", type_="DoubleType"),
    dfgx.expect_column_value_lengths_to_equal(column="plant", value=4),
    dfgx.expect_column_value_lengths_to_equal(column="BOM_number", value=8),
    dfgx.expect_column_value_lengths_to_equal(column="node", value=8),
    dfgx.expect_column_values_to_be_unique(column="business_id"),
]

# Validate all red quality checks, if at least one fails the job stops
gx_validation.validate_and_log_gx_results(
    quality_check_results=red_quality_check_results,
    validation_level="RED",
    info_message="Quality Checks passed for BOM table - RED",
    error_message="Quality checks failed for BOM table, please check log messages",
    pipeline_write_env=PIPELINE_WRITE_ENV,
    table_name="as_design_BOM_gold",
    log_to_event_table=True,
    log_successes=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Outputs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output - LEAP Tables

# COMMAND ----------

catalog = f"{PIPELINE_WRITE_ENV}_gold"
domain = "production"
table = "BOM"

table_utils.save_table(
    dest_table=f"{catalog}.{domain}.{table}", df=df_silver_business, mode="overwrite"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output - LEAP Tables BOM_exposed

# COMMAND ----------

select_exposed_statement = ", ".join(
    [
        col
        for col in df_silver_business.columns
        if not (
            col.startswith("STPO")
            or col.startswith("MAST")
            or col.startswith("STKO")
            or col.startswith("STAS")
        )
    ]
)

table_utils.create_table_view(
    src_full_tablename=f"{catalog}.{domain}.{table}",
    dest_full_tablename=f"{catalog}.{domain}.{table}_exposed",
    select_statement=select_exposed_statement,
    replace_view=True,
)
