# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Average BOM
# MAGIC
# MAGIC
# MAGIC **Description:**  
# MAGIC
# MAGIC POC AVERAGE BOM  
# MAGIC 1- Extraction de tous les ordres de fabrications de la branche IS (exclusion Rush Order) + Horizon 1 an  "OF TERMINES"  
# MAGIC 2- Extraction des nomenclatures séries des articles issus des OF etape 1  
# MAGIC 3- Extraction des réservations liés aux OF étape 1 (As Planned)  
# MAGIC 4- Extraction des mouvements de stocks réels sur les OF de l'étape 1 (As build)  
# MAGIC 5- Calcul de la nomenclature moyenne (via As planned - etape 3)  
# MAGIC 6- Ajout Information As Build pour prise de décision  
# MAGIC 7- Tester cas particulier Article Fantôme dans BOM Standard   
# MAGIC
# MAGIC **Intended Pipeline**
# MAGIC - Name of pipeline(s) the script was built for.
# MAGIC
# MAGIC **Inputs Reference Data**
# MAGIC - {REFERENCE_READ_ENV}_gold.production.sap_work_order_basics_latest_exposed
# MAGIC - {REFERENCE_READ_ENV}_gold.mrp.sap_mrp_reservation_latest
# MAGIC - {REFERENCE_READ_ENV}_gold.production.bom_exposed
# MAGIC - {REFERENCE_READ_ENV}_gold.production.quotas_arrangement_exposed
# MAGIC - {REFERENCE_READ_ENV}_gold.production.production_version_exposed
# MAGIC - {REFERENCE_READ_ENV}_gold.master_data.material_master_data_plant_exposed
# MAGIC
# MAGIC
# MAGIC **Output Tables (Pipeline)**
# MAGIC - {PIPELINE_WRITE_ENV}_proj.manuf_engineering.average_bom 
# MAGIC - {PIPELINE_WRITE_ENV}_proj.manuf_engineering.average_bom_wo_details  
# MAGIC - {PIPELINE_WRITE_ENV}_proj.manuf_engineering.bom_phantom  
# MAGIC
# MAGIC **TODO:**
# MAGIC - split notebook into smaller one with intermediate part
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Standard Package Imports

# COMMAND ----------

from graphframes import *
from pyspark.sql import functions as f
from pyspark.sql.window import Window

from leap_utils.data_asset import table_utils

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
# Get read reference environment variable
dbutils.widgets.text("reference_read_env", "prod")
REFERENCE_READ_ENV = dbutils.widgets.get("reference_read_env")
# Get write environment variable
dbutils.widgets.text("pipeline_write_env", "dev")
PIPELINE_WRITE_ENV = dbutils.widgets.get("pipeline_write_env")
# Get read pipeline environment variable
dbutils.widgets.text("pipeline_read_env", "dev")
PIPELINE_READ_ENV = dbutils.widgets.get("pipeline_read_env")

dbutils.widgets.text("horizon", "12")
HORIZON = int(dbutils.widgets.get("horizon"))

spark.conf.set("spark.sql.shuffle.partitions", "auto")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC # Inputs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inputs - LEAP tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### LEAP Table - Work Order
# MAGIC
# MAGIC Detailed instruction for manufacturing a product or performing a specific task in the production process.

# COMMAND ----------

df_work_order = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.production.sap_work_order_basics_latest_exposed"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### LEAP Table - MRP reservations
# MAGIC
# MAGIC Reservations created by the Material Requirements Planning (MRP) system to ensure that the necessary materials are available for production.

# COMMAND ----------

df_mrp_reservation = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.mrp.sap_mrp_reservation_latest"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### LEAP Table - BOM As design

# COMMAND ----------

df_bom_standard = (
    spark.read.table(f"{REFERENCE_READ_ENV}_gold.production.bom_exposed")
    .filter((f.col("Bulk_Material").isNull()))
    .drop("BOM_type", "Bulk_Material")
).drop("business_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LEAP Table - Quotas arrangement
# MAGIC
# MAGIC A mechanism to allocate production among multiple sources or suppliers according to predefined quotas.

# COMMAND ----------

df_quotas_arrangement = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.production.quotas_arrangement_exposed"
).drop("business_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LEAP Table - Production Version
# MAGIC
# MAGIC A specific variant of the production process for a product, including details like the BOM and the manufacturing route used.

# COMMAND ----------

df_production_version = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.production.production_version_exposed"
).drop("business_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LEAP Table - Material master data

# COMMAND ----------

df_material_master = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.master_data.material_master_data_plant_exposed"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### LEAP Table - Profit Center

# COMMAND ----------

df_profit_center = (
    spark.read.table(f"{REFERENCE_READ_ENV}_gold.finance.profit_center_exposed")
    .select(
        "Profit_Center", f.col("Short_Description").alias("Profit_Center_Description")
    )
    .dropDuplicates()
)
df_profit_center = table_utils.remove_leading_zeros(df_profit_center, ["Profit_Center"])

# COMMAND ----------

# MAGIC %md
# MAGIC # Data preparation

# COMMAND ----------

# MAGIC %md
# MAGIC ##Creation Dataframe With As Planned & As Build

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upload Gold Table Work Order
# MAGIC Target : Use WO production on the previous year in order to determine BOM Standard to Analyse

# COMMAND ----------

# #############################################################################
# Business Rules :
# - Keep Only Work Order where production is terminated on the previous 6 month
# - Exclusion Rush Order (Production without Material Number)
# - Focus On IS Branch
# -----------------------------------------------------------------------------
# Output :
# 1 Dataframe with All Work Order details Information
# 1 Dataframe with List of Material Plant where BOM analysis will be performed
# #############################################################################
List_RU = ["ZP09", "ZP10", "ZP11", "YP09", "YP10", "YP11"]

df_work_order = (
    df_work_order.filter(
        (f.col("WO_Status") == "WO_Delivered")
        & (f.col("Material_Number").isNotNull())
        & ~(f.col("Order_Type").isin(List_RU))
        & (f.col("Branch") == "IS")
    )
    .select(
        "Plant",
        "Material_Number",
        "WO_Number",
        "WO_Description",
        "Quantity_Ordered",
        "Start_Planned_Date",
        "End_Planned_Date",
        "Real_End_Date",
        "Mrp_Controller",
        "Storage_Location",
        "Production_Version",
        "Unit",
        "Unit_planif",
        "WBS_Code",
        "Profit_Center",
        "Production_Site",
        "Branch",
        "Customers_Orders",
        "Customers_Orders_Item"
    )
    .withColumnRenamed("Wo_Number", "Work_Order_Origin")
    .withColumnRenamed("WO_Description", "Material_Number_Description")
    .withColumnRenamed("Quantity_Ordered", "Quantity")
    .withColumn("months_diff", f.months_between(f.current_date(), f.col("real_end_date")))
).filter(f.col("months_diff") < HORIZON) # Filter WO depending on horizon
# -----------------------------------------------------------------------------
# Remove leading Zero on Work Order Number
df_work_order_end = table_utils.remove_leading_zeros(df_work_order, ["Work_Order_Origin"])
# -----------------------------------------------------------------------------
# List of Material Plant where BOM analysis will be performed
df_work_orders_list = df_work_order_end.groupBy("Plant", "Material_Number").agg(
    f.count("Work_Order_Origin").alias("Nb_WO_List")
)
# -----------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ###Upload Table Gold MRP RESERVATION  
# MAGIC Target : Use MRP Reservation in order to obtain list of Components planned and Built according Wo List from previous Step  
# MAGIC - TECHNICAL DEBT REVOIR la TABLE EXPOSED pour ajout colonne Manquantes 'RESB_postp' & 'RESB_posnr

# COMMAND ----------

# #############################################################################
# Business Rules :
# - Keep Only Reservation on Components (ie : Exclusion Rows without Material)
# - Exclusion Phantom and Bulk Material (ie: field dumps / schgt)
# - Exclusion reservations Cancelled  TBC
# -----------------------------------------------------------------------------
# Output :
# 1 Dataframe with all mrp reservation
# #############################################################################
df_reservations = df_mrp_reservation.filter(
    (f.col("Material_Number").isNotNull())
    & (f.col("RESB_dumps").isNull())
    # & (f.col("Status") != "Res. Cancel")
    & (f.col("RESB_schgt").isNull())
).select(
    f.col("Reservation_Number"),
    f.col("Reservation_Item_Number"),
    f.col("Plant").alias("Plant_C"),
    f.col("Material_Number").alias("Component"),
    f.col("Material_Number_Description").alias("Component_Description"),
    f.col("Storage_Location").alias("Component_Storage_Location"),
    f.col("Need_Date"),
    f.col("Quantity_Needed"),
    f.col("Unit").alias("Component_Unit"),
    f.col("Quantity_Consumed"),
    f.col("Planned_Order_Origin"),
    f.col("Work_Order_Origin"),
    f.col("Sales_Order"),
    f.col("Sales_Order_item"),
    f.col("WBS_Code").alias("Component_WBS_Code"),
    f.col("Profit_Center").alias("Component_Profit_Center"),
    f.col("Status"),
    f.col("RESB_postp").alias("Item_Type"),
    f.col("RESB_posnr").alias("Item_Number"),
    f.col("Material_Number_Origin"),
    f.col("RESB_ausch").alias("scrap_pourcentage")
)
# -----------------------------------------------------------------------------
# Remove leading Zero on Work Order Number / Components / Item Number
df_reservations_clean = table_utils.remove_leading_zeros(
    df_reservations, ["Work_Order_Origin"]
)
df_reservations_clean = table_utils.remove_leading_zeros(
    df_reservations_clean, ["Component"]
)
df_reservations_clean = table_utils.remove_leading_zeros(
    df_reservations_clean, ["Item_Number"]
)
# -----------------------------------------------------------------------------
# Creation Item Category according Item Type on Work Order
df_reservations_end = df_reservations_clean.withColumn(
    "Item_Category",
    f.when((f.col("Item_Type") == "L") | (f.col("Item_Type") == "0"), f.lit("Standard"))
    .when((f.col("Item_Type") == "1"), f.lit("Missing_BOM"))
    .when(
        (f.col("Item_Type") == "2")
        | (f.col("Item_Type") == "3")
        | (f.col("Item_Type") == "4")
        | (f.col("Item_Type") == "5"),
        f.lit("Over_Consumption"),
    )
    .otherwise(f.lit("TBD")),
)
# -----------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Work Order and Reservations
# MAGIC Target : Cross Work Orders List with MRP Reservations to Analyse with BOM Standard

# COMMAND ----------

# #############################################################################
# Business Rules :
# - Calculate Base Quantity for 1U in order to compare with BOM Standard Unit
# - Exclusion Phantom and Bulk Material (ie: field dumps / schgt)
# -----------------------------------------------------------------------------
# Output :
# 1 Dataframe for standard consumption to compare with Standard BOM (df_wo_average_std)
# 1 Dataframe for missing consumption (df_wo_average_miss)
# 1 Dataframe for over consumption (df_wo_average_over)
# #############################################################################

df_join_needs = df_work_order_end.join(
    df_reservations_end, ["Work_Order_Origin"], "left"
)
# -----------------------------------------------------------------------------
# Calculate Base Quantity for 1 (Quantity_Needed / Quantity_Consumed)
df_join_mrp = (
    df_join_needs.withColumn("Quantity_Global_Needed", f.col("Quantity_Needed"))
    .withColumn("Quantity_Global_Consumed", f.col("Quantity_Consumed"))
    .withColumn("Quantity_Needed", f.col("Quantity_Needed") / f.col("Quantity"))
    .withColumn("Quantity_Consumed", f.col("Quantity_Consumed") / f.col("Quantity"))
    .withColumn("horizon", f.lit(HORIZON))
)
df_join_mrp_w_scrap = (
    df_join_mrp.withColumn(
        "Quantity_Global_Needed_wo_scrap",
        f.col("Quantity_Global_Needed")
        - (f.col("Quantity_Global_Needed") * (f.col("scrap_pourcentage") / 100)),
    )
    .withColumn(
        "Quantity_Global_Consumed_wo_scrap",
        f.col("Quantity_Global_Consumed")
        - (f.col("Quantity_Global_Consumed") * (f.col("scrap_pourcentage") / 100)),
    )
    .withColumn(
        "Quantity_Needed_wo_scrap",
        f.col("Quantity_Needed")
        - (f.col("Quantity_Needed") * (f.col("scrap_pourcentage") / 100)),
    )
    .withColumn(
        "Quantity_Consumed_wo_scrap",
        f.col("Quantity_Consumed")
        - (f.col("Quantity_Consumed") * (f.col("scrap_pourcentage") / 100)),
    )
    .withColumn(
        "Quantity_wo_scrap",
        f.col("Quantity") - (f.col("Quantity") * (f.col("scrap_pourcentage") / 100)),
    )
)
# -----------------------------------------------------------------------------
# Creation Dataframe for standard consumption to compare with Standard BOM
df_wo_average_std = df_join_mrp_w_scrap.filter(f.col("Item_Category") == "Standard")
df_wo_average_std = df_wo_average_std.groupBy(
    "Plant",
    "Material_Number",
    "Component",
    "Item_Category",
    "Item_Type",
    "Material_Number_Origin",
).agg(
    f.sum(f.col("Quantity_Needed")).alias("Quantity_Needed"),
    f.sum(f.col("Quantity_Consumed")).alias("Quantity_Consumed"),
    f.sum(f.col("Quantity_Global_Needed")).alias("Quantity_Global_Needed"),
    f.sum(f.col("Quantity_Global_Consumed")).alias("Quantity_Global_Consumed"),
    f.count_distinct(f.col("Work_Order_Origin")).alias("NB_As_Planned"),
    f.sum(f.col("Quantity_Global_Needed_wo_scrap")).alias(
        "Quantity_Global_Needed_wo_scrap"
    ),
    f.sum(f.col("Quantity_Global_Consumed_wo_scrap")).alias(
        "Quantity_Global_Consumed_wo_scrap"
    ),
    f.sum(f.col("Quantity_Needed_wo_scrap")).alias("Quantity_Needed_wo_scrap"),
    f.sum(f.col("Quantity_Consumed_wo_scrap")).alias("Quantity_Consumed_wo_scrap"),
)
# ----------------------------------------------------------------------
# Creation Dataframe for missing and over consumption
df_wo_average_temp = df_join_mrp_w_scrap.filter(f.col("Item_Category") != "Standard")
df_wo_average_temp = df_wo_average_temp.groupBy(
    "Plant",
    "Material_Number",
    "Component",
    "Item_Category",
).agg(
    f.sum(f.col("Quantity_Needed")).alias("Quantity_Needed"),
    f.sum(f.col("Quantity_Consumed")).alias("Quantity_Consumed"),
    f.sum(f.col("Quantity_Global_Needed")).alias("Quantity_Global_Needed"),
    f.sum(f.col("Quantity_Global_Consumed")).alias("Quantity_Global_Consumed"),
    f.count_distinct(f.col("Work_Order_Origin")).alias("NB_As_Planned"),
)
df_wo_average_miss = df_wo_average_temp.filter(
    f.col("Item_Category") == "Missing_BOM"
).drop("Item_Category")

df_wo_average_over = df_wo_average_temp.filter(
    f.col("Item_Category") == "Over_Consumption"
).drop("Item_Category")
# ----------------------------------------------------------------------

# COMMAND ----------

path_coverage_wo = f"{PIPELINE_WRITE_ENV}_proj.manuf_engineering.average_bom_wo_details"
table_utils.save_table(dest_table=path_coverage_wo, df=df_join_mrp_w_scrap, mode="overwrite", replace_where=f"horizon = {HORIZON}", merge_schema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data preparation - production version

# COMMAND ----------

# #############################################################################
# Construction Data Asset Production Version :
# - Use MAKL (link Prod Vers/Mat) to obtain active Production Version and BOM linked
# -----------------------------------------------------------------------------
# Business Rules
# - Keep Only Production Version active (Validity Date / Unblocked-mksp-)
# mksp: 	Production version is locked  -> get unlocked production version
# - Use STPO (Item BOM) to link BOM Header and BOM Item
# -----------------------------------------------------------------------------
# Output :
# 1 Dataframe contain Material with only one Production Version Active
# 1 Dataframe contain Material with severals Production Version Active
# #############################################################################
df_production_version_unlocked = df_production_version.filter(~f.col("is_locked"))
# -----------------------------------------------------------------------------
# Management Validity Date in MKAL table
df_production_version_date = df_production_version_unlocked.filter(
    (f.col("Start_Validity") <= f.current_date())
    & (f.col("End_Validity") > f.current_date())
)
# -----------------------------------------------------------------------------
# Check if only one Production Version is active
df_production_version_count = df_production_version_date.groupBy(
    "Plant", "Material_Number"
).agg(
    f.count(f.col("Alternative")).alias("Nb_Alternative"),
)
# -----------------------------------------------------------------------------
df_production_version_joined = df_production_version_date.join(
    df_production_version_count, ["Material_Number", "Plant"], "left"
)
df_production_version_active = df_production_version_joined.withColumn(
    "Active_BOM_MRP",
    f.when(f.col("Nb_Alternative") == 1, f.lit("X")).otherwise(f.lit("KO")),
)
# --------------------------------------------------------------------------------------
df_production_version_ok = df_production_version_active.filter(
    f.col("Active_BOM_MRP") == "X"
)
df_production_version_ko = df_production_version_active.filter(
    f.col("Active_BOM_MRP") == "KO"
)
# --------------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upload Quotas Version
# MAGIC Target : Determine which alternative of BOM is used for previsionnal needs in case of Quota Arragement is activated on SAP  
# MAGIC - TECHNICAL DEBT:  
# MAGIC -- Need to create DATA QUOTAS ARRAGMENT
# MAGIC -- Management percentage of Quotas  
# MAGIC On quick and dirty version we take production version with highest quotas  
# MAGIC Need to determine average BOM in quotas
# MAGIC

# COMMAND ----------

# #############################################################################
# Construction Data Asset Quotas Arrangement :
# - Use EQUK (Quota Header) to obtain active Quotas arrangement
# - Use EQUP (Quota Item) to obtain active Quotas arrangement details
# -----------------------------------------------------------------------------
# Business Rules
# - Keep Only Quotas arrangement active (Validity Date)
# - Keep Only max Quota Item Found
# -----------------------------------------------------------------------------
# Output :
# 1 Dataframe contain Active Quotas with Production Version  use on it
# #############################################################################
# ----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Management Validity Date in EQUK table
df_quotas_valid = df_quotas_arrangement.filter(
    (f.col("Start_Validity") <= f.current_date())
    & (f.col("End_Validity") > f.current_date())
)
# -----------------------------------------------------------------------------
# ###############################################################################
# TECH DEBT - take production version with highest quotas  (No average)
# ###############################################################################
w1 = Window.partitionBy("quota_number").orderBy(f.desc(f.col("quota")))
df_quotas_w = df_quotas_valid.withColumn("Max", f.row_number().over(w1))
df_quotas = df_quotas_w.filter(f.col("Max") == 1).drop("Max")

df_quotas = df_quotas.select(
    f.col("material_number"),
    f.col("plant"),
    f.col("production_version").alias("production_version_quotas"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Management Several Production Version active
# MAGIC Target : Keep Only one Production Version when several production version is activate on SAP  

# COMMAND ----------

#############################################################################
# Business Rules
# - Keep Production Version where quotas is activated
# - If several Production Version are still activated, pick first one created
# -----------------------------------------------------------------------------
# Output :
# 1 Dataframe contain Active Production Version used for previsionnal
# #############################################################################

# Join With Production Version with quota dataframe
df_production_version_ko_new = df_production_version_ko.join(
    df_quotas, ["material_number", "plant"], "left"
)
# Keep Production Version where quotas is activated
df_production_version_ko_ok = df_production_version_ko_new.filter(
    f.col("production_version") == f.col("production_version_quotas")
).drop("production_version_quotas")
# -----------------------------------------------------------------------------
# If we still have several Production Version active we'll pick the first one
df_production_version_ko_ko = df_production_version_ko_new.filter(
    f.col("production_version_quotas").isNull()
)
w1 = Window.partitionBy("material_number", "plant").orderBy(
    f.asc(f.col("start_validity")), f.asc(f.col("production_version"))
)
df_production_version_ko_ko = df_production_version_ko_ko.withColumn(
    "keep_first", f.row_number().over(w1)
).drop("production_version_quotas", "keep_first")
df_production_version_ko_ko_ok = df_production_version_ko_ko.filter(
    f.col("keep_first") == 1
)
# -----------------------------------------------------------------------------
# Build DataFrame for Production Version
df_production_version_end = df_production_version_ok.unionByName(
    df_production_version_ko_ok
).unionByName(df_production_version_ko_ko_ok)
# -----------------------------------------------------------------------------
# df_production_version_end.filter(f.col("Material_Number")=="IS0005292M").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Management Several BOM Active
# MAGIC Target : Selection of BOM Standard used for MRP previsionnal needs 

# COMMAND ----------

# #############################################################################
# Business Rules
# - Keep BOM where Production Version has been found and activated
# - Keep the first BOM created if Production Version is not available
# -----------------------------------------------------------------------------
# Output :
# 1 Dataframe contain BOM Standard active for MRP Previsionnal
# #############################################################################

# Keep List Of BOM where Production Version have been found
df_bom_standard_validate = df_bom_standard.join(
    df_production_version_end, ["material_number", "plant", "alternative"], "left"
)
df_bom_standard_validate_ok = df_bom_standard_validate.filter(
    f.col("Production_Version").isNotNull()
)
df_bom_list_active = df_bom_standard_validate_ok.select(
    "material_number", "plant"
).dropDuplicates()
df_bom_list_active = df_bom_list_active.withColumn("Temp", f.lit("X"))
# -----------------------------------------------------------------------------
# Management of BOM not managed through Production Version
df_bom_standard_validate_ko = df_bom_standard_validate.filter(
    f.col("Production_Version").isNull()
)
# Cancel Material already in dataframe df_bom_standard_validate_ok
df_bom_standard_validate_ko_temp = df_bom_standard_validate_ko.join(
    df_bom_list_active, ["material_number", "plant"], "left"
)
df_bom_standard_validate_ko_ko = df_bom_standard_validate_ko_temp.filter(
    f.col("Temp").isNull()
)
df_bom_list_inactive = df_bom_standard_validate_ko_ko.select(
    "material_number", "plant", "alternative", "creation_date"
).dropDuplicates()
# -----------------------------------------------------------------------------
# Ranking of BOM in order to keep the first BOM created in SAP
w1 = Window.partitionBy("material_number", "plant").orderBy(
    f.asc(f.col("creation_date"))
)
df_bom_list_inactive_rank = df_bom_list_inactive.withColumn(
    "Max", f.row_number().over(w1)
)
df_bom_list_inactive_rank = df_bom_list_inactive_rank.filter(f.col("Max") == 1)
# -----------------------------------------------------------------------------
df_bom_standard_validate_ko_ok = df_bom_standard_validate_ko_ko.join(
    df_bom_list_inactive_rank,
    ["material_number", "plant", "alternative", "creation_date"],
)
df_bom_standard_validate_ko_ok = df_bom_standard_validate_ko_ok.filter(
    f.col("Max").isNotNull()
).drop("Max", "Temp")
# -----------------------------------------------------------------------------
# Creation Dataframe Bom MRP Active
df_last_bom = df_bom_standard_validate_ok.unionByName(df_bom_standard_validate_ko_ok)
# -----------------------------------------------------------------------------
# Management of Last date of modification
df_last_bom = df_last_bom.withColumn(
    "last_modif", f.coalesce(f.col("last_modif"), f.col("creation_date"))
)

# df_last_bom.filter(f.col("material_number") == "IS0005278M").display()

# COMMAND ----------

# #############################################################################
# Business Rules
# Take information of Phantom from Material Master Data
# When Procurement_Special_Type is phantom (50), add rows from BOM Phantom
# -----------------------------------------------------------------------------
# Output :
# 1 Dataframe contain BOM Standard active for MRP Previsionnal with Phantom level
# #############################################################################
df_material_master = spark.read.table(
    f"prod_gold.master_data.material_master_data_plant_exposed"
)

# ---------------Phantom to material (Composed) -----------------
df_material_master_material = df_material_master.select(
    f.col("Material_Number"),
    f.col("Plant"),
    f.col("Procurement_Special_Type").alias("Material_Procurement_Special_Type"),
).dropDuplicates()

df_last_bom_material_proc = df_last_bom.join(
    df_material_master_material, ["Material_Number", "Plant"], "left"
)

# ---------------Phantom to Components -----------------
df_material_master_component = df_material_master.select(
    f.col("Material_Number").alias("component"),
    f.col("Plant"),
    f.col("Procurement_Special_Type").alias("Component_Procurement_Special_Type"),
).dropDuplicates()

df_last_bom_components_proc = df_last_bom_material_proc.join(
    df_material_master_component, ["component", "Plant"], "left"
)

# ------------Preprocessing to only get nodes that are phantom or father of phantom or son of phantom -------
df_phantom_end = df_last_bom_components_proc.filter(
    (f.col("Material_Procurement_Special_Type") == "50")  # Material that are phantom
    | (
        f.col("Component_Procurement_Special_Type") == "50"
    )  # Material that have son phantom
).select(
    f.col("Material_Number"), f.col("Plant"), f.col("Material_Procurement_Special_Type")
)

df_phantom = df_last_bom_components_proc.filter(
    (f.col("Material_Procurement_Special_Type") == "50")
    & (
        (f.col("Component_Procurement_Special_Type") != "50")
        | (f.col("Component_Procurement_Special_Type").isNull())
    )  # Filter phantom Material that have not son phantom
).select(
    f.col("Component").alias("Material_Number"),
    f.col("Plant"),
    f.col("Component_Procurement_Special_Type").alias(
        "Material_Procurement_Special_Type"
    ),
)

df_phantom_all = (
    df_phantom_end.unionByName(df_phantom)
    .dropDuplicates()
    .fillna("0", subset=["Material_Procurement_Special_Type"])
)


# -----------------Create graphFrame ---------------------------------------
vertices = df_phantom_all.select(
    f.concat_ws("_", "Material_Number", "Plant").alias("id"),
    "Material_Number",
    "Plant",
    "Material_Procurement_Special_Type",
).distinct()

edges = df_last_bom_components_proc.filter(
    (f.col("Material_Procurement_Special_Type") == "50")
    | (f.col("Component_Procurement_Special_Type") == "50")
).select(
    f.concat_ws("_", "Material_Number", "Plant").alias("src"),
    f.concat_ws("_", "Component", "Plant").alias("dst"),
)

g = GraphFrame(vertices, edges)


def CreateMotifJumpString(TotalJumps):
    # TotalJumps -= 1
    if TotalJumps == 1:
        return "(a)-[e1]->(b)"
    if TotalJumps > 1:
        return ";".join(
            ["(a)-[e1]->(node_1)"]
            + [f"(node_{x})-[edge_{x}]->(node_{x+1})" for x in range(1, TotalJumps - 1)]
            + [f"(node_{TotalJumps-1})-[e2]->(b)"]
        )


def CreateIntermediateFilterCondition(
    TotalJumps, fieldName="Material_Procurement_Special_Type", value=50
):
    return " AND ".join(
        [f"node_{x}.{fieldName} == '{value}'" for x in range(1, TotalJumps)]
    )


def ConcatenateIntermediateNodes(TotalJumps):
    if TotalJumps == 1:
        return "a.id"
    return (
        "concat_ws(' | ', "
        + ", ".join(["a.id"] + [f"node_{x}.id" for x in range(1, TotalJumps)])
        + ")"
    )


lev = 1
all_paths_full = (
    g.find(CreateMotifJumpString(lev))
    .filter("b.Material_Procurement_Special_Type != '50' AND a.id != b.id")
    .selectExpr("a", "b", ConcatenateIntermediateNodes(lev) + " as intermediate_path")
    .withColumn("number_of_jumps", f.lit(lev))
)

for lev in range(2, 8):
    all_paths_i = (
        g.find(CreateMotifJumpString(lev))
        .filter(
            "b.Material_Procurement_Special_Type != '50' AND a.id != b.id"
            + " AND "
            + CreateIntermediateFilterCondition(
                lev, fieldName="Material_Procurement_Special_Type", value=50
            )
        )
        .selectExpr(
            "a", "b", ConcatenateIntermediateNodes(lev) + " as intermediate_path"
        )
        .withColumn("number_of_jumps", f.lit(lev))
    )
    all_paths_full = all_paths_full.unionByName(all_paths_i)


# -----------------------Post processing ------------------
# --------------------------explode dictionnaire
df_exploded = all_paths_full
for col_name in ["a", "b"]:
    df_exploded = df_exploded.select(
        "*",
        *[
            f.col(f"{col_name}.{key}").alias(f"{col_name}_{key}")
            for key in all_paths_full.select(f"{col_name}.*").columns
        ],
    ).drop(col_name)


df_exploded.cache()


# --------------------add new path to original data
df_last_bom_material = df_last_bom.join(
    df_material_master_component, ["Component", "Plant"], "left"
)

df_last_bom_init = df_last_bom_material.filter(
    (f.col("Component_Procurement_Special_Type") != "50")
    | (f.col("Component_Procurement_Special_Type").isNull())
).withColumn("level", f.lit(1))


df_last_bom_phatom = df_last_bom_material.filter(
    f.col("Component_Procurement_Special_Type") == "50"
)
df_last_bom_tmp = (
    (
        df_last_bom_phatom.join(
            df_exploded.dropDuplicates(["a_id", "b_id", "intermediate_path"]).select(
                f.col("a_Material_Number").alias("component"),
                f.col("a_Plant").alias("plant"),
                f.col("b_Material_Number"),
                f.col("b_Material_Procurement_Special_Type"),
                f.col("number_of_jumps").alias("level"),
                f.col("intermediate_path"),
            ),
            on=["component", "plant"],
            how="left",
        )
        .withColumnsRenamed({"intermediate_path": "Phantom"})
        .withColumn("component", f.col("b_Material_Number"))
        .withColumn(
            "Component_Procurement_Special_Type",
            f.col("b_Material_Procurement_Special_Type"),
        )
        .drop("b_Material_Number", "b_Material_Procurement_Special_Type")
    )
    .select("component", "plant", "material_number", "level", "Phantom")
    .withColumn(
        "Material_Number_Origin",
        f.regexp_extract(
            f.trim(
                f.split(f.regexp_replace(f.col("Phantom"), r"\s*\|\s*", "|"), "\|")[
                    f.size(f.split(f.col("Phantom"), "\|")) - 1
                ]
            ),
            r"(.+)_\d+$",
            1,
        ),
    )
)

df_last_bom_end = (
    df_last_bom_tmp.alias("a")
    .join(
        df_last_bom.alias("b"),
        on=[
            f.col("a.component") == f.col("b.component"),
            f.col("a.plant") == f.col("b.Plant"),
            f.col("a.Material_Number_Origin") == f.col("b.Material_Number"),
        ],
        how="left",
    )
    .drop(
        f.col("b.component"),
        f.col("b.Plant"),
        f.col("a.Material_Number_Origin"),
        f.col("b.Material_Number"),
    )
)

df_last_bom_fin = df_last_bom_init.unionByName(
    df_last_bom_end, allowMissingColumns=True
)

# COMMAND ----------

# #############################################################################
# Business Rules
# Filter out BOM standard when Component_Plant_Status == 05
# SAP Status 05 is when everything is available to launch production.
# (BOM, Routing, Prod Version,...)
# -----------------------------------------------------------------------------
# Output :
# 1 Dataframe contain BOM Standard active for MRP Previsionnal with Phantom level
# #############################################################################
df_material_master = spark.read.table(
    f"prod_gold.master_data.material_master_data_plant_exposed"
)

df_material_master_material = df_material_master.select(
    f.col("Material_Number").alias("component"),
    f.col("Plant"),
    f.col("bulk_material")
).dropDuplicates()

df_last_bom_fin_filtered = (
    df_last_bom_fin.join(df_material_master_material, ["component", "Plant"], "left")
    .filter(
        (f.col("Bulk_Material").isNull()) & (f.col("Component_Plant_Status") == "05")
    )
    .drop("Bulk_Material", "Component_Plant_Status")
).withColumn("horizon", f.lit(HORIZON))

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### Management Phantom Level in BOM Active

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save BOM with phantom handled

# COMMAND ----------

df_phantom_bom_path = f"{PIPELINE_WRITE_ENV}_proj.manuf_engineering.bom_phantom"

# COMMAND ----------

table_utils.save_table(
    dest_table=df_phantom_bom_path, df=df_last_bom_fin_filtered, mode="overwrite", replace_where=f"horizon = {HORIZON}"
)

# COMMAND ----------

df= spark.read.table(df_phantom_bom_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #Transformation Average BOM

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1st Step - Cross Standard BOM with Standard Consumption

# COMMAND ----------

# #############################################################################
# Business Rules
# - Cross Standard BOM with WO production on Key Material/Plant/Item_Number
# - Specific Case Phantom Key will be Material/Plant/Item_Number/Material_Number_Origin
# -----------------------------------------------------------------------------
# Output :
# 1 Dataframe BOM Standard with Consumption planned and performed
# #############################################################################
# Dispatch dataframe As Build with and without phantom parts

df_phantom_bom = spark.read.table(df_phantom_bom_path).filter(f.col("horizon")==HORIZON)
df_last_bom_std = df_phantom_bom.filter(f.col("Phantom").isNull()).withColumn(
    "Material_Number_Origin", f.col("Material_Number")
)
df_last_bom_pht = df_phantom_bom.filter(f.col("Phantom").isNotNull()).withColumn(
    "Material_Number_Origin",
    f.regexp_extract(
        f.trim(
            f.split(f.regexp_replace(f.col("Phantom"), r"\s*\|\s*", "|"), "\|")[
                f.size(f.split(f.col("Phantom"), "\|")) - 1
            ]
        ),
        r"(.+)_\d+$",
        1,
    ),
)
# -----------------------------------------------------------------------------
# Cross Standard BOM with WO production on Key Material/Plant/Item_Number
df_join_standard = (
    df_last_bom_std.join(
        df_wo_average_std,
        [
            "Material_Number",
            "Plant",
            "Component",
            "Item_Type",
            "Material_Number_Origin",
        ],
        "left",
    )
    .withColumnRenamed("Quantity_Needed", "Qty_WO")
    .withColumnRenamed("Quantity_Consumed", "Qty_WO_real")
    .withColumnRenamed("NB_As_Planned", "NB_WO")
    .withColumnRenamed("Quantity_Global_Needed", "Qty_Global_WO")
    .withColumnRenamed("Quantity_Global_Consumed", "Qty_1")
    .withColumnRenamed("Quantity_Needed_wo_scrap", "Qty_WO_wo_scrap")
    .withColumnRenamed("Quantity_Consumed_wo_scrap", "Qty_WO_real_wo_scrap")
    .withColumnRenamed("Quantity_Global_Needed_wo_scrap", "Qty_Global_WO_wo_scrap")
    .withColumnRenamed("Quantity_Global_Consumed_wo_scrap", "Qty_1_wo_scrap")
).withColumn(
    "Material_Number_Origin",
    f.regexp_replace("Material_Number_Origin", "^0+", ""),
)
# Need to aggregate when same item appears several times in WO
df_join_standard_end = df_join_standard.groupBy(
    "Material_Number",
    "Plant",
    "Alternative",
    "BOM_Number",
    "BOM_Status",
    "Component",
    "Unit",
    "Item_Type",
    "Production_Version",
    "Material_Number_Origin",
    "level",
).agg(
    f.sum(f.col("Quantity")).alias("Quantity"),
    f.sum(f.col("Qty_WO")).alias("Qty_WO"),
    f.sum(f.col("Qty_WO_real")).alias("Qty_WO_real"),
    f.sum(f.col("NB_WO")).alias("NB_WO"),
    f.sum(f.col("Qty_Global_WO")).alias("Qty_Global_WO"),
    f.sum(f.col("Qty_1")).alias("Qty_1"),
    f.sum(f.col("Qty_WO_wo_scrap")).alias("Qty_WO_wo_scrap"),
    f.sum(f.col("Qty_WO_real_wo_scrap")).alias("Qty_WO_real_wo_scrap"),
    f.sum(f.col("Qty_Global_WO_wo_scrap")).alias("Qty_Global_WO_wo_scrap"),
    f.sum(f.col("Qty_1_wo_scrap")).alias("Qty_1_wo_scrap"),
    f.max("last_modif").alias("last_modif"),
    f.concat_ws(", ", f.collect_list("item_number")).alias("item_number"),
).dropDuplicates()

# -----------------------------------------------------------------------------
# Specific Case Phantom Key will be Material/Plant/Item_Number/Material_Number_Origin
df_join_phantom = (
    df_last_bom_pht.join(
        df_wo_average_std,
        [
            "Material_Number",
            "Plant",
            "Component",
            "Item_Type",
            "Material_Number_Origin",
        ],
        "left",
    )
    .withColumnRenamed("Quantity_Needed", "Qty_WO")
    .withColumnRenamed("Quantity_Consumed", "Qty_WO_real")
    .withColumnRenamed("NB_As_Planned", "NB_WO")
    .withColumnRenamed("Quantity_Global_Needed", "Qty_Global_WO")
    .withColumnRenamed("Quantity_Global_Consumed", "Qty_1")
    .withColumnRenamed("Quantity_Needed_wo_scrap", "Qty_WO_wo_scrap")
    .withColumnRenamed("Quantity_Consumed_wo_scrap", "Qty_WO_real_wo_scrap")
    .withColumnRenamed("Quantity_Global_Needed_wo_scrap", "Qty_Global_WO_wo_scrap")
    .withColumnRenamed("Quantity_Global_Consumed_wo_scrap", "Qty_1_wo_scrap")
)
# Need to aggregate when same item appears several times in WO
df_join_phantom_end = df_join_phantom.groupBy(
    "Material_Number",
    "Plant",
    "Alternative",
    "BOM_Number",
    "BOM_Status",
    "Component",
    "Unit",
    "Item_Type",
    "Production_Version",
    "Material_Number_Origin",
    "level",
    "Phantom",
).agg(
    f.sum(f.col("Quantity")).alias("Quantity"),
    f.sum(f.col("Qty_WO")).alias("Qty_WO"),
    f.sum(f.col("Qty_WO_real")).alias("Qty_WO_real"),
    f.sum(f.col("NB_WO")).alias("NB_WO"),
    f.sum(f.col("Qty_Global_WO")).alias("Qty_Global_WO"),
    f.sum(f.col("Qty_1")).alias("Qty_1"),
    f.sum(f.col("Qty_WO_wo_scrap")).alias("Qty_WO_wo_scrap"),
    f.sum(f.col("Qty_WO_real_wo_scrap")).alias("Qty_WO_real_wo_scrap"),
    f.sum(f.col("Qty_Global_WO_wo_scrap")).alias("Qty_Global_WO_wo_scrap"),
    f.sum(f.col("Qty_1_wo_scrap")).alias("Qty_1_wo_scrap"),
    f.max("last_modif").alias("last_modif"),
    f.concat_ws(", ", f.collect_list("item_number")).alias("item_number"),
).dropDuplicates()
# -----------------------------------------------------------------------------
# Join 2 Dataframe
df_join_standard_consumption = df_join_standard_end.unionByName(
    df_join_phantom_end, allowMissingColumns=True
)
# -----------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2nd Step - Add quantity from Manual Item (Missing & Over)

# COMMAND ----------

# #############################################################################
# Business Rules
# - Sometimes rows are manually added by business on WO directly (item_Type 1 to 5)
# - 1st add Missing rows from Production
# - 2nd add Overconsumption from production
# -----------------------------------------------------------------------------
# Output :
# 1 Dataframe BOM Standard with Consumption planned and performed
# + Over Consumption + Missing parts
# #############################################################################
# 1st - Add Item Missing Material in WO
df_join_missing = (
    df_join_standard_consumption.join(
        df_wo_average_miss,
        ["Material_Number", "Plant", "Component"],
        "left",
    )
    .withColumnRenamed("Quantity_Needed", "Qty_Miss")
    .withColumnRenamed("Quantity_Consumed", "Qty_Miss_real")
    .withColumnRenamed("NB_As_Planned", "NB_Miss")
    .withColumnRenamed("Quantity_Global_Needed", "Qty_Global_Miss")
    .withColumnRenamed("Quantity_Global_Consumed", "Qty_2")
)
# -----------------------------------------------------------------------------
# 2nd add Overconsumption from production
df_join_overconso = (
    df_join_missing.join(
        df_wo_average_over,
        ["Material_Number", "Plant", "Component"],
        "left",
    )
    .withColumnRenamed("Quantity_Needed", "Qty_Over")
    .withColumnRenamed("Quantity_Consumed", "Qty_Over_real")
    .withColumnRenamed("NB_As_Planned", "NB_Over")
    .withColumnRenamed("Quantity_Global_Needed", "Qty_Global_Over")
    .withColumnRenamed("Quantity_Global_Consumed", "Qty_3")
    
)
# -----------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3rd Step - Add manually entries in WO without rules respected (Item Missing or Item OverConsumption)

# COMMAND ----------

# get only row in BOM standard
df_join_row_bom = (
    df_wo_average_std.join(
        df_phantom_bom.select("Material_Number", "Plant"),
        ["Material_Number", "Plant"],
        "left",
    )
    .filter(df_phantom_bom["Material_Number"].isNotNull())
    .distinct()
)

df_join_missing_wo = df_join_row_bom.join(
    df_phantom_bom,
    ["Material_Number", "Plant", "Component", "Item_Type"],
    "left",
)

df_join_missing_wo_ko = df_join_missing_wo.filter(f.col("Bom_Number").isNull())

# COMMAND ----------

# #############################################################################
# Business Rules
# - Sometimes rows are manually added by business on WO directly with Bad procedure
#   Not included on Item Type (1 to 5)
# - If Material/component/Item_Number already created - add as Extra Consumption
# - If Not create a complete new line
# -----------------------------------------------------------------------------
# Output :
# 1 Dataframe BOM Standard with Consumption planned and performed
# + Over Consumption + Missing parts + Extra Consumption
# #############################################################################


df_join_row_bom = (
    df_wo_average_std.join(
        df_phantom_bom.select("Material_Number", "Plant"),
        ["Material_Number", "Plant"],
        "left",
    )
    .filter(df_phantom_bom["Material_Number"].isNotNull())
    .distinct()
)

df_join_missing_wo = df_join_row_bom.join(
    df_phantom_bom,
    ["Material_Number", "Plant", "Component", "Item_Type"],
    "left",
)

df_join_missing_wo_ko = df_join_missing_wo.filter(f.col("Bom_Number").isNull())

# COMMAND ----------

# -----------------------------------------------------------------------------
# Check if a row of this component already existing on WO
df_join_standard_tmp = df_join_overconso.select(
    "Material_Number", "Plant", "Component", "Item_Type"
).dropDuplicates()

df_join_standard_tmp = df_join_standard_tmp.withColumn("Temp", f.lit("X"))

df_join_missing_wo_end = df_join_missing_wo_ko.join(
    df_join_standard_tmp,
    ["Material_Number", "Plant", "Component", "Item_Type"],
    "left",
)
# -----------------------------------------------------------------------------
# If Yes then Add on existing lines as Extra Qty
df_join_missing_wo_yes = df_join_missing_wo_end.filter(f.col("Temp") == "X")

df_join_missing_wo_yes = df_join_missing_wo_yes.groupBy(
    "Material_Number", "Plant", "Component", "Item_Type"
).agg(
    f.sum(f.col("Quantity_Needed")).alias("Qty_Extra"),
    f.sum(f.col("Quantity_Consumed")).alias("Qty_WO_Extra"),
    f.sum(f.col("NB_As_Planned")).alias("NB_Extra"),
    f.sum(f.col("Quantity_Global_Needed")).alias("Qty_Global_Extra"),
    f.sum(f.col("Quantity_Global_Consumed")).alias("Qty_4"),
)

df_join_missing_wo_yes = df_join_overconso.join(
    df_join_missing_wo_yes,
    ["Material_Number", "Plant", "Component", "Item_Type"],
    "left",
)

# COMMAND ----------

# -----------------------------------------------------------------------------
# If  No then Create a new line without BOM Standard only
# if rows for Material/Plant/Components doesn't exist

df_join_missing_wo_no = df_join_missing_wo_end.filter(
    f.col("Temp").isNull() & (f.col("Quantity_Needed") > 0)
)

df_join_missing_wo_no = df_join_missing_wo_no.groupBy(
    "Material_Number", "Plant", "Component", "Item_Type"
).agg(
    f.sum(f.col("Quantity_Needed")).alias("Qty_Extra"),
    f.sum(f.col("Quantity_Consumed")).alias("Qty_WO_Extra"),
    f.sum(f.col("NB_As_Planned")).alias("NB_Extra"),
    f.sum(f.col("Quantity_Global_Needed")).alias("Qty_Global_Extra"),
    f.sum(f.col("Quantity_Global_Consumed")).alias("Qty_4"),
)
# Add Item Missing Material in WO without rows in BOM Standard
df_join_missing_wo_no = (
    df_join_missing_wo_no.join(
        df_wo_average_miss,
        ["Material_Number", "Plant", "Component"],
        "left",
    )
    .withColumnRenamed("Quantity_Needed", "Qty_Miss")
    .withColumnRenamed("Quantity_Consumed", "Qty_Miss_real")
    .withColumnRenamed("NB_As_Planned", "NB_Miss")
    .withColumnRenamed("Quantity_Global_Needed", "Qty_Global_Miss")
    .withColumnRenamed("Quantity_Global_Consumed", "Qty_2")
)

# -----------------------------------------------------------------------------
# Check if Material/Component/Plant existing for not taking in account for realibility
df_join_missing_wo_tmp = df_join_missing_wo_yes.select(
    "Material_Number", "Plant", "Component"
).dropDuplicates()
df_join_missing_wo_tmp = df_join_missing_wo_tmp.withColumn(
    "Realibility_temp", f.lit("X")
)

df_join_missing_wo_no = df_join_missing_wo_no.join(
    df_join_missing_wo_tmp, ["Material_Number", "Plant", "Component"], "left"
)

# -----------------------------------------------------------------------------
df_join_end = df_join_missing_wo_yes.unionByName(
    df_join_missing_wo_no, allowMissingColumns=True
)
# -----------------------------------------------------------------------------

# COMMAND ----------

# Cleansing over consomption on phantom Parts.
# As we don't know on which Phantom the overconsumption is done we mae clean up on this line

df_join_end_clean = df_join_end.withColumn(
    "Phantom_redifine",
    f.when(
        (f.col("Phantom").isNotNull())
        & ((f.col("Qty_2") > 0) | (f.col("Qty_3") > 0) | (f.col("Qty_4") > 0)),
        f.lit("X"),
    ).otherwise(f.lit("O")),
)

df_join_end_old = df_join_end_clean.filter(f.col("Phantom_redifine") == "O").drop(
    "Phantom_redifine"
)
df_join_end_temp = df_join_end_clean.filter(f.col("Phantom_redifine") == "X").drop(
    "Phantom_redifine"
)

Column_to_erase = [
    "Qty_Over",
    "Qty_Over_real",
    "Qty_Global_Over",
    "Qty_3",
    "NB_Over",
    "Qty_Miss",
    "Qty_Miss_real",
    "Qty_Global_Miss",
    "Qty_2",
    "NB_Miss",
    "Qty_Extra",
    "Qty_WO_Extra",
    "Qty_Global_Extra",
    "Qty_4",
    "NB_Extra",
]
df_join_end_temp_clean = df_join_end_temp
for column in Column_to_erase:
    df_join_end_temp_clean = df_join_end_temp_clean.withColumn(column, f.lit(None))

# -------------------------------------------------------------------------------------------------
# Add a specific line for Over Consumption
df_join_end_temp_clean2 = df_join_end_temp.dropDuplicates(
    ["Material_Number", "Plant", "Component"]
)
Column_to_erase_2 = [
    "Alternative",
    "BOM_Number",
    "BOM_Status",
    "Item_Type",
    "Production_Version",
    "Material_Number_Origin",
    "Quantity",
    "Qty_WO",
    "Qty_WO_real",
    "NB_WO",
    "Qty_Global_WO",
    "Qty_1",
    "Phantom",
    "last_modif",
]
for column in Column_to_erase_2:
    df_join_end_temp_clean2 = df_join_end_temp_clean2.withColumn(column, f.lit(None))

df_join_end_final = df_join_end_old.unionByName(df_join_end_temp_clean).unionByName(
    df_join_end_temp_clean2
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4th Step - Add Material Master Data informations

# COMMAND ----------

# #############################################################################
# Add standard Price For Impact Valorisation
# + Branch & Description & Profit Center at component and Material Level
# Business Rules :
# - Keep Only BOM where we have at least minimum 1 WO on previous year
# - Exclusion Article SPLIT for specific cutting management
# - Keep Only IS Analysis
# -----------------------------------------------------------------------------
# Output :
# 1 Dataframe BOM Standard with Consumption planned and performed
# + Over Consumption + Missing parts + Extra Consumption with material
# Master data enrichments
# #############################################################################

# Add Material Master data at Components level
df_material_master_component = df_material_master.select(
    f.col("Material_Number").alias("Component"),
    f.col("Description").alias("Component_Description"),
    f.col("Plant"),
    f.col("Standard_Price_EUR_Budget"),
    f.col("Supplier_Name"),
    f.col("PF_pourcentage_affectation")
)
df_material_master_component = df_material_master_component.dropDuplicates(
    [
        col
        for col in df_material_master_component.columns
        if col not in ["PF_pourcentage_affectation"]
    ]
)
# -----------------------------------------------------------------------------
# Add Material Master data at Material level
df_material_master_data = df_material_master.select(
    f.col("Material_Number"),
    f.col("Description").alias("Material_Description"),
    f.col("Plant"),
    f.col("Branch"),
    f.col("Profit_Center"),
).dropDuplicates()
# -----------------------------------------------------------------------------
df_last_bom_end = df_join_end_final.join(
    df_material_master_component, ["Component", "Plant"], "left"
).join(df_material_master_data, ["Material_Number", "Plant"], "left")
# -----------------------------------------------------------------------------
# Add Profit Center Description
df_last_bom_end = df_last_bom_end.join(df_profit_center, ["Profit_Center"], "left")
df_last_bom_end = df_last_bom_end.withColumn(
    "Profit_Center_con",
    f.concat_ws(
    "_",
    f.col("Profit_Center"),
    f.col("Profit_Center_Description")),
).withColumn(
    "PF_pourcentage_affectation_percent",
    f.when(
        f.element_at(f.col("PF_pourcentage_affectation"), f.col("Profit_Center")) < 0.01,
        f.lit("<1 %")
    ).otherwise(
        f.concat(
            f.format_number(
                f.element_at(f.col("PF_pourcentage_affectation"), f.col("Profit_Center")) * 100, 
                0
            ),
            f.lit(" %")
        )
    )
).drop("PF_pourcentage_affectation")
# -----------------------------------------------------------------------------
# Exclusion Material SPLIT for specific cutting management
df_last_bom_end = df_last_bom_end.filter(f.col("Component") != "SPLIT")
# -----------------------------------------------------------------------------
# Keep Only IS Analysis
df_last_bom_end = df_last_bom_end.filter(f.col("Branch") == "IS")
# -----------------------------------------------------------------------------
# Keep Only BOM where we have at least minimum 1 WO on one past year
df_last_bom_end = df_last_bom_end.join(
    df_work_orders_list, ["Material_Number", "Plant"], "left"
)
df_last_bom_end = df_last_bom_end.filter(f.col("Nb_WO_List").isNotNull()).drop(
    "Nb_WO_List"
)
# -----------------------------------------------------------------------------
# Creation of Phantom Management column
df_last_bom_end = df_last_bom_end.withColumn(
    "Phantom_check",
    f.when(f.col("Phantom").isNull(), f.lit("No")).otherwise(f.lit("Yes")),
).drop("Material_Number_Origin")
# -----------------------------------------------------------------------------
# Keep Only entries without 0
df_last_bom_end = df_last_bom_end.filter(
    (f.col("Quantity") > 0)
    | (f.col("Qty_WO") > 0)
    | (f.col("Qty_Miss") > 0)
    | (f.col("Qty_Over") > 0)
    | (f.col("Qty_Extra") > 0)
)
# -----------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md 
# MAGIC ##5th Step - Calcul Average BOM

# COMMAND ----------

# #############################################################################
# - Calcul Gap BOM Standard and Work order planification in Percentage
# - Calcul of BOM reliability (+/- 5%)
# - Calcul Impact Over Stock
# - Calcul Risk Shortage
# - Calcul Risk OverStock
# -----------------------------------------------------------------------------
# Output :
# 1 Dataframe Average BOM for project PBI
# #############################################################################
df_end = df_last_bom_end.dropDuplicates(
    [
        col
        for col in df_last_bom_end.columns
        if col not in ["PF_pourcentage_affectation_percent"]
    ]
)
# -----------------------------------------------------------------------------
# Calcul Gap BOM Standard and Work order planification in Percentage
df_end = (
    df_end.withColumn("Avg_Qty_WO", f.col("Qty_WO_wo_scrap") / f.col("NB_WO"))
    .withColumn("Avg_Qty_Real", f.col("Qty_WO_real_wo_scrap") / f.col("NB_WO"))
    .withColumn(
        "Gap_Std_Real_%",
        f.when(
            ((f.col("BOM_Number").isNull()) | (f.col("Quantity") == "0"))
            & (f.col("Realibility_temp") == "X"),
            f.lit(None),
        )
        .when((f.col("BOM_Number").isNull()) | (f.col("Quantity") == "0"), f.lit(1))
        .when(f.col("Avg_Qty_WO").isNull(), f.lit(-1))
        .otherwise((f.col("Avg_Qty_WO") - f.col("Quantity")) / f.col("Quantity")),
    )
)
# -----------------------------------------------------------------------------
# Calcul of BOM reliability
df_end = df_end.withColumn(
    "Reliability",
    f.when(
        (f.col("Gap_Std_Real_%") >= -0.05) & (f.col("Gap_Std_Real_%") <= 0.05), f.lit(1)
    )
    .when(f.col("Gap_Std_Real_%").isNull(), f.lit(None))
    .otherwise(f.lit(0)),
)
# -----------------------------------------------------------------------------
# Calcul Impact Over Stock
df_end = df_end.withColumn(
    "Impact_Stock",
    -f.col("Gap_Std_Real_%")
    * f.col("Qty_Global_WO_wo_scrap")
    * f.col("Standard_Price_EUR_Budget"),
)
df_end = df_end.withColumn(
    "Impact_Stock",
    f.when(f.col("Impact_Stock") < 0, f.lit(0)).otherwise(f.col("Impact_Stock")),
)
# -----------------------------------------------------------------------------
# Calcul Risk Shortage
df_end = df_end.withColumn(
    "Risk_shortage",
    f.when(f.col("Gap_Std_Real_%") >= 0.05, f.lit(1)).otherwise(f.lit(0)),
)
# -----------------------------------------------------------------------------
# Calcul Risk OverStock
df_end = df_end.withColumn(
    "Risk_Overstock",
    f.when(f.col("Gap_Std_Real_%") <= -0.05, f.lit(1)).otherwise(f.lit(0)),
)

# -----------------------------------------------------------------------------
# Calcul RValorisation item
df_end = df_end.withColumn(
    "Item_Valorisation",
    f.col("Qty_Global_WO_wo_scrap") * f.col("Standard_Price_EUR_Budget"),
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##6th Step - Casting Column for PBI usage

# COMMAND ----------

# #############################################################################
# - Casting of all columns with Quantity and replace null values
# - Calcul Global Quantity consummed
# -----------------------------------------------------------------------------
# Output : Final Dataframe Average BOM
# #############################################################################

columns_to_fill = [
    "Qty_WO",
    "Qty_WO_real",
    "Qty_Global_WO",
    "NB_WO",
    "Qty_Miss",
    "Qty_Miss_real",
    "Qty_Global_Miss",
    "NB_Miss",
    "Qty_Over",
    "Qty_Over_real",
    "Qty_Global_Over",
    "NB_Over",
    "Qty_Extra",
    "Qty_WO_Extra",
    "Qty_Global_Extra",
    "NB_Extra",
    "Qty_1",
    "Qty_2",
    "Qty_3",
    "Qty_4",
    "Qty_WO_wo_scrap",
    "Qty_WO_real_wo_scrap",
    "Qty_Global_WO_wo_scrap",
    "Qty_1_wo_scrap",
]
# Replace Null Values
df_end_bom = df_end.na.fill(0, subset=columns_to_fill)
# -----------------------------------------------------------------------------
# Calcul Global Quantity consummed
df_end_bom = (
    df_end_bom.withColumn(
        "Quantity_Global_Consumed",
        f.col("Qty_1_wo_scrap") + f.col("Qty_2") + f.col("Qty_3") + f.col("Qty_4"),
    )
    .drop(
        "Item_Category",
        "Qty_1",
        "Qty_2",
        "Qty_3",
        "Qty_4",
        "Realibility_temp",
        "Qty_1_wo_scrap",
    )
    .withColumn("horizon", f.lit(HORIZON))
)
# -----------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC #Save Proj Table for average BOM Project

# COMMAND ----------

df_coverage_path = f"{PIPELINE_WRITE_ENV}_proj.manuf_engineering.average_bom"
table_utils.save_table(dest_table=df_coverage_path, df=df_end_bom, mode="overwrite", replace_where=f"horizon = {HORIZON}", merge_schema=True)