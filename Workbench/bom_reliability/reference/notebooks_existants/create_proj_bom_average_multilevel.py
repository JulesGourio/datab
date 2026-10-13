# Databricks notebook source
# MAGIC %md
# MAGIC # Multilevel average BOM
# MAGIC
# MAGIC **Description**
# MAGIC
# MAGIC 1. Build the multilevel average BOM (1 per material number, based on latest data)
# MAGIC 2. Build the multilevel average BOM per material number and configuration (based on the MSN to set the configuration)
# MAGIC
# MAGIC **Output table:**  
# MAGIC   - {PIPELINE_WRITE_ENV}_proj.supply_chain.current_average_bom
# MAGIC
# MAGIC **Input tables:**  
# MAGIC   - {REFERENCE_READ_ENV}_gold.production.bom
# MAGIC   - {REFERENCE_READ_ENV}_gold.production.production_version_exposed
# MAGIC   - {REFERENCE_READ_ENV}_gold.master_data.material_plant_exposed
# MAGIC   - {REFERENCE_READ_ENV}_proj.manuf_engineering.mod_effectivity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### Config LEAP function import

# COMMAND ----------

from leap_utils.data_asset.table_utils import save_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Config Widgets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Config Pipeline environments

# COMMAND ----------

# Get read reference environment variable
dbutils.widgets.text("reference_read_env", "prod")
REFERENCE_READ_ENV = dbutils.widgets.get("reference_read_env")
# Get read pipeline environment variable
dbutils.widgets.text("pipeline_read_env", "prod")
PIPELINE_READ_ENV = dbutils.widgets.get("pipeline_read_env")
# Get write environment variable
dbutils.widgets.text("pipeline_write_env", "dev")
PIPELINE_WRITE_ENV = dbutils.widgets.get("pipeline_write_env")

# COMMAND ----------

# MAGIC %md
# MAGIC # Inputs

# COMMAND ----------

df_bom = spark.read.table(f"{REFERENCE_READ_ENV}_gold.production.bom")

df_prod_version = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.production.production_version_exposed"
)

df_material = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.master_data.material_plant_exposed"
)

df_a320 = spark.read.table(
    f"{REFERENCE_READ_ENV}_proj.manuf_engineering.mod_effectivity"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Tr 1. Build active BOM for MRP
# MAGIC ## Tr 1.1 Get active BOM data

# COMMAND ----------

# Load BOM
df_bom_all = df_bom.select(
    "BOM_status",
    "material_number",
    "plant",
    "component",
    "quantity",
    "alternative",
    "bulk_material",
    f.col("STPO_fmeng").alias("fixed_item"),
)

# Active production version
w = Window.partitionBy("material_number", "plant").orderBy("alternative")

df_prod_version = (
    df_prod_version.filter(
        (f.col("is_locked") == "false")
        & (f.col("start_validity") <= f.current_date())
        & (f.col("end_validity") >= f.current_date())
    )
    .withColumn("rn", f.row_number().over(w))
    .filter(f.col("rn") == 1)
    .drop("rn")
)

# Keep single plant (most recent)
w0 = Window.partitionBy("material_number","plant").orderBy(f.desc("start_validity"))

df_prod_version = (
    df_prod_version.withColumn("date_rank", f.dense_rank().over(w0))
    .filter(f.col("date_rank") == 1)
    .drop("date_rank")
)

df_prod_version_end = df_prod_version.select("material_number", "plant", "alternative")

df_bom_active = df_bom_all.join(
    df_prod_version_end, ["material_number", "plant", "alternative"], "inner"
).filter(f.col("material_number") != f.col("component"))

# Keep single alternative
w0 = Window.partitionBy("material_number", "plant").orderBy("alternative")

df_bom_active_end = (
    df_bom_active.withColumn("rk", f.dense_rank().over(w0))
    .filter(f.col("rk") == 1)
    .drop("rk")
)

# COMMAND ----------

# Add plant switch
df_mat_switch = df_material.select(
    f.col("material_number").alias("component"), "plant", "plant_buy"
)

df_bom_active_end = df_bom_active_end.join(
    df_mat_switch, ["component", "plant"], "left"
).withColumn("plant_buy", f.coalesce("plant_buy", "plant"))

# Light material join (for flag logic)
df_material_flag = df_material.select(
    f.col("material_number").alias("component"),
    f.col("plant").alias("plant_buy"),
    "procurement_type",
    "procurement_special_type",
    f.col("bulk_material").alias("bulk_material_2"),
)

df_bom_flag_ready = df_bom_active_end.join(
    df_material_flag, ["component", "plant_buy"], "left"
)

df_bom_flag_ready = df_bom_flag_ready.withColumn(
    "bulk_material", f.coalesce(f.col("bulk_material"), f.col("bulk_material_2"))
).drop("bulk_material_2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr 1.2 Build active BOM

# COMMAND ----------

# Root items
root_items = (
    df_a320.select("material_number","plant")
    .distinct()
    .join(
        df_prod_version_end.select("material_number", "plant").distinct(),
        on=["material_number","plant"],
        how="inner",
    )
)

# Level 0
exploded = root_items.select(
    f.col("material_number").alias("root"),
    f.col("material_number").alias("parent"),
    f.col("plant"),
    f.col("plant").alias("plant_buy"),
    f.lit(None).cast("string").alias("component"),
    f.lit(None).cast("string").alias("bulk_material"),
    f.lit(1.0).alias("initial_qty"),
    f.lit(1.0).alias("total_qty"),
    f.lit(0).alias("level"),
    f.lit(None).cast("string").alias("procurement_type"),
    f.lit(None).cast("string").alias("procurement_special_type"),
    f.lit(None).cast("string").alias("branch_flag"),
    f.lit(None).cast("string").alias("fixed_item"),
    f.array(f.col("material_number")).alias("path"),
)

# Level 1
current_level = (
    df_bom_flag_ready.alias("bom")
    .join(
        root_items.alias("r"),
        (f.col("r.material_number") == f.col("bom.material_number"))
        & (f.col("r.plant") == f.col("bom.plant")),
        how="inner",
    )
    .select(
        f.col("r.material_number").alias("root"),
        f.col("bom.material_number").alias("parent"),
        f.col("bom.component"),
        f.col("bom.procurement_type"),
        f.col("bom.procurement_special_type"),
        f.col("bom.bulk_material"),
        f.col("bom.plant"),
        f.col("bom.plant_buy"),
        f.col("bom.quantity").cast("double").alias("initial_qty"),
        f.col("bom.quantity").cast("double").alias("total_qty"),
        f.col("bom.fixed_item"),
        f.lit(1).alias("level"),
        f.lit(None).alias("branch_flag"),
        f.array(f.col("r.material_number"), f.col("bom.component")).alias("path"),
    )
)
all_levels = exploded.unionByName(current_level)

# COMMAND ----------

# Explosion loop with propagation
for i in range(20):
    print(i)
    next_level = (
        current_level.alias("cur")
        .join(
            df_bom_flag_ready.alias("bom"),
            (f.col("cur.component") == f.col("bom.material_number"))
            & (f.col("cur.plant_buy") == f.col("bom.plant")),
            "inner",
        )
        .select(
            f.col("cur.root"),
            f.col("cur.component").alias("parent"),
            f.col("bom.component"),
            f.col("bom.procurement_type"),
            f.col("bom.procurement_special_type"),
            f.col("bom.bulk_material"),
            f.col("cur.plant"),
            f.col("bom.plant_buy"),
            f.col("bom.quantity").alias("initial_qty"),
            (f.col("cur.total_qty") * f.col("bom.quantity")).alias("total_qty"),
            f.col("bom.fixed_item"),
            (f.col("cur.level") + 1).alias("level"),
            f.when(f.col("cur.branch_flag") == "X", f.lit("X"))
            .when(
                (f.col("cur.procurement_type") == "F")
                & (f.col("cur.procurement_special_type") == "20"),
                f.lit("X"),
            )
            .alias("branch_flag"),
            f.array_append(f.col("cur.path"), f.col("bom.component")).alias("path"),
        )
    )
    if not next_level.take(1):
        break

    all_levels = all_levels.unionByName(next_level).distinct()
    current_level = next_level

# Adapt quantity, add type (PO for PO and interco, WO for WO not interco only)
df_bom_exploded = (
    all_levels.withColumnRenamed("root", "material_number")
    .withColumn(
        "quantity",
        f.when(f.col("fixed_item") == "X", f.col("initial_qty")).otherwise(
            f.col("total_qty")
        ),
    )
    .withColumn(
        "type",
        f.when(
            (f.col("component").startswith("IS"))
            & (f.col("component").endswith("M")),
            "WO",
        ).otherwise("PO"),
    )
    .drop("initial_qty", "total_qty", "fixed_item")
    .distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr 1.3. Enrich data

# COMMAND ----------

# Full material enrichment (after explosion)
df_material_full = df_material.select(
    f.col("material_number").alias("component"),
    f.col("plant").alias("plant_buy"),
    "MRP_unit",
    "material_description",
    "external_lead_time",
    "internal_lead_time",
    f.col("standard_price_eur_budget").alias("unit_price"),
    "supplier_code",
    "supplier_name",
    "abc_indicator",
).filter(f.col("plant") == f.col("MRP_unit")).drop("MRP_unit")

df_bom_final = df_bom_exploded.join(
    df_material_full, ["component", "plant_buy"], "left"
).withColumn(
    "lead_time",
    f.when(f.col("type") == "PO", f.col("external_lead_time")).otherwise(f.col("internal_lead_time"))
).distinct()

# ISM profit center (from material master on ISM material_number + plant)
df_profit_center = (
    df_material
    .filter(f.col("plant") == f.col("MRP_unit"))
    .select("material_number", "plant", "profit_center")
    .distinct()
)
df_bom_final = df_bom_final.join(df_profit_center, ["material_number", "plant"], "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr 1.4 Save average BOM

# COMMAND ----------

CATALOG = f"{PIPELINE_WRITE_ENV}_proj"
SCHEMA = "supply_chain"
DESTINATION_TABLE = "current_average_bom"
DESTINATION_TABLE_FULL_PATH = f"{CATALOG}.{SCHEMA}.{DESTINATION_TABLE}"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{SCHEMA}")

result = save_table(
    dest_table=DESTINATION_TABLE_FULL_PATH,
    df=df_bom_final,
    mode="overwrite",
    overwrite_schema=True,
)
