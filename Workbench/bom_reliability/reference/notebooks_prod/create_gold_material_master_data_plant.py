# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Purpose
# MAGIC
# MAGIC **Description:**  
# MAGIC The aim is to create a gold table & Gold_explosed for Material Master data on plant View
# MAGIC
# MAGIC **Output table(s):**  
# MAGIC   - {WRITE_ENV}_gold.master_data.material_master_data_plant
# MAGIC   - {WRITE_ENV}_gold.master_data.material_master_data_plant_exposed
# MAGIC
# MAGIC **Input :**  
# MAGIC   - prod_gold.master_data.material_master_data_basics_exposed  
# MAGIC   - prod_gold.master_data.plant_master_data_latest_exposed  
# MAGIC   - prod_gold.finance.currency_exchange
# MAGIC   - prod_gold.mrp.list_mrp_full_exposed  
# MAGIC   - prod_bronze.sap_latecoere_ecc6.equk_latest
# MAGIC   - prod_bronze.sap_latecoere_ecc6.equp_latest  
# MAGIC   - prod_bronze.sap_latecoere_ecc6.eord_latest  
# MAGIC   - prod_bronze.sap_latecoere_ecc6.t460a_latest  
# MAGIC   - prod_bronze.sap_latecoere_ecc6.mdma_latest
# MAGIC   - prod_bronze.sap_latecoere_ecc6.ausp_latest
# MAGIC   - prod_gold.supply_chain.supplier_master_data_latest_exposed
# MAGIC
# MAGIC
# MAGIC **Triggered by:**  
# MAGIC Notebook create_silver_material_master_data_plant 
# MAGIC
# MAGIC **Things performed in this Notebook:**  
# MAGIC -  Add Material Master Data Basics  
# MAGIC -  Add Plant & Company Master Data  
# MAGIC -  Add Branch / Production Site / Inventory Site  
# MAGIC -  Management of SAP Currency  
# MAGIC -  Management of MRP Needs  
# MAGIC -  Determination of Final Assembly  
# MAGIC -  Management Supplier affectation  
# MAGIC -  Management Intra/InterCo Flows  
# MAGIC -  Management MRP Unit  
# MAGIC -  Management Price Upper Green-Line  

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import MapType, StringType, DoubleType, FloatType

from leap_utils.data_asset import amount_correction
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
# MAGIC # Upload Silver table Material

# COMMAND ----------

df_material = spark.read.table(
    f"{READ_ENV}_silver.master_data.material_master_data_plant"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Enrichment Gold

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload & Join Material Master Basics

# COMMAND ----------

# Upload Master Data Material Basic
df_material_basic = spark.read.table(
    f"prod_gold.master_data.material_master_data_basics_exposed"
).select("Material_Number", "Description", "Material_Type", "Unit_Base")

# We add Description and Material type from Material Basics
df_material_basic = df_material_basic.withColumnRenamed(
    "Material_Number", "Material_Number_"
)
df_material_basics = (
    df_material.alias("A")
    .join(
        df_material_basic.alias("B"),
        f.col("A.Material_Number") == f.col("B.Material_Number_"),
        "left",
    )
    .drop("Material_Number_")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload & join Plant Master data & Company

# COMMAND ----------

# Upload Master Data Basic
df_plant = spark.read.table(
    f"prod_gold.master_data.plant_master_data_latest_exposed"
).select("Plant", "Currency", "Plant_Description_new", "Company", "Company_Description")

df_plant = df_plant.withColumnRenamed("Plant", "Plant_").withColumnRenamed(
    "Plant_Description_new", "Production_Site"
)

df_material_plant = (
    df_material_basics.alias("A")
    .join(
        df_plant.alias("B"),
        f.col("A.Plant") == f.col("B.Plant_"),
        "left",
    )
    .drop("Plant_")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Determination Branch & Production Site & Inventory Site

# COMMAND ----------

## On AS side, we have 4 distinct plants grouped into a single plant in SAP (1000)
# and 2 distinct plants grouped into a single plan in SAP (Plant 1900)

# Here under Rules to dispatch Stock and Production activities between
# LFR-Gimont / LFR-Montredon / LFR- HUB / LFR-DES for plant 1000
# LFR-Cust / LFR-DES for plant 1900

DES_Prog = ["BCL", "HAP", "297", "298", "299", "DEW"]
# Determination Stock Site for Plant 1000 (Gimont/Montredon/HUB/DES)

df_material_storage = df_material_plant.withColumn(
    "Stock_Site",
    f.when(
        (f.col("Plant") != "1000") & (f.col("Plant") != "1900"),
        f.col("Production_Site"),
    )
    .when(
        (f.col("Profit_Center").isin(DES_Prog))
        | (f.col("Profit_Center").startswith("V"))
        | (f.col("MRP_Controller").startswith("DE")),
        f.lit("LFR-DES"),
    )
    .when(f.col("Decopling_Point").startswith("MON"), f.lit("LFR-Montredon"))
    .when(f.col("Decopling_Point").startswith("GMT"), f.lit("LFR-Gimont"))
    .when(f.col("Plant") == "1900", f.lit("LFR-Cust"))
    .otherwise(f.lit("LFR-HUB")),
)

# Dispatch Production Site for Plant 1000 (Gimont/Montredon/HUB/DES)
df_material_prod = df_material_storage.withColumn(
    "Production_Site",
    f.when(f.col("Plant") != "1000", f.col("Production_Site"))
    .when(f.col("MRP_Controller") == "DET", f.lit("LFR-DES"))
    .when(
        (f.col("Procurement_Type") == "F") & (f.col("Stock_Site") == "LFR-Gimont"),
        f.lit("LFR-Gimont"),
    )
    .when(
        (f.col("Procurement_Type") == "F") & (f.col("Stock_Site") == "LFR-DES"),
        f.lit("LFR-DES"),
    )
    .when(
        (f.col("Procurement_Type") == "F") & (f.col("Stock_Site") == "LFR-Montredon"),
        f.lit("LFR-Montredon"),
    )
    .when((f.col("Procurement_Type") == "F"), f.lit("LFR-HUB"))
    .when(f.col("Mrp_controller").startswith("O"), f.lit("LFR-Montredon"))
    .otherwise(f.lit("LFR-Gimont")),
)
# Determination Branch AS / IS
df_material_branch = df_material_prod.withColumn(
    "Branch",
    f.when(
        (f.col("Stock_Site") == "LFR-DES") | (f.col("Plant").startswith("2")),
        f.lit("IS"),
    ).otherwise(f.lit("AS")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Management Currency

# COMMAND ----------

# MAGIC %md
# MAGIC ### Management of Wrong SAP decimal for spécific currency (TND, JPY,...)

# COMMAND ----------

# Use Fonction to correct wrong amount coming from SAP
df_material_price = amount_correction.create_corrected_amounts(
    df_material_branch, "Average_Price", "Currency"
)
df_material_price = amount_correction.create_corrected_amounts(
    df_material_price, "Standard_Price", "Currency"
)
df_material_price = df_material_price.drop("Average_Price", "Standard_Price")
df_material_price = df_material_price.withColumnRenamed(
    "Average_Price_corrected", "Average_Price"
).withColumnRenamed("Standard_Price_corrected", "Standard_Price")

df_material_price = df_material_price.withColumn(
    "Currency_Date", f.date_sub(f.current_date(), 1)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert all in EUR on Budget Price

# COMMAND ----------

# Upload Gold Currency to convert all prices in EUR
df_currency = spark.read.table(f"prod_gold.finance.currency_exchange")
# Extraction Budget_Rate to EUR
df_material_price_budget = table_utils.convert_currency(
    df_material_price,
    df_currency,
    df_base_source_currency_column_name="Currency",
    target_currency_value="EUR",
    df_currency_conversion_rate_type_exp_value="Budget",
    df_base_conversion_date_column_name="Currency_Date",
    conversion_output_column_name="Rate_Budget",
)
# Extraction Spot_Rate to EUR
df_material_price_spot = table_utils.convert_currency(
    df_material_price_budget,
    df_currency,
    df_base_source_currency_column_name="Currency",
    target_currency_value="EUR",
    df_currency_conversion_rate_type_exp_value="Spot",
    df_base_conversion_date_column_name="Currency_Date",
    conversion_output_column_name="Rate_Spot",
)
# Calcul Average Price and Standard Price
df_material_price_end = df_material_price_spot.withColumn(
    "Standard_Price_EUR_Budget",
    f.col("Standard_Price") / f.col("Price_Base") * f.col("Rate_Budget"),
)
df_material_price_end = df_material_price_end.withColumn(
    "Average_Price_EUR_Budget",
    f.col("Average_Price") / f.col("Price_Base") * f.col("Rate_Budget"),
)
df_material_price_end = df_material_price_end.withColumn(
    "Standard_Price_EUR_Spot",
    f.col("Standard_Price") / f.col("Price_Base") * f.col("Rate_Spot"),
)
df_material_price_end = df_material_price_end.withColumn(
    "Average_Price_EUR_Spot",
    f.col("Average_Price") / f.col("Price_Base") * f.col("Rate_Spot"),
)
df_material_price_end = df_material_price_end.drop(
    "Rate_Budget",
    "Rate_Spot",
    "Standard_Price",
    "Average_Price",
    "Price_Base",
    "Currency_Date",
    "Currency",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Management of Needs

# COMMAND ----------

# Upload MRP_List in order to Extract needs
df_mrp = spark.read.table(f"prod_gold.mrp.list_mrp_full_exposed")
# Keep only Needs from Mrp List (-: needs / +:Resources)
df_mrp = df_mrp.filter(f.col("Code_mvt") == "-")

# Exclusion needs from PEX
# Segment 04 - Smoothing prod out MRP
# Segment 26 - Segment 26 Subcontracted Needs already take in account in segment 02
df_mrp = df_mrp.filter(
    (f.col("Segment_planif") != "04") & (f.col("Segment_planif") != "26")
)
df_mrp = df_mrp.withColumn(
    "Plant_Need",
    f.when(f.col("Plant_delivered").isNull(), f.col("Plant")).otherwise(
        f.col("Plant_delivered")
    ),
)

# Keep 1 DataFrame for All Needs for one plant
df_mrp_full = df_mrp.groupBy("Plant", "Material_Number").agg(
    f.sum("Quantity").alias("Total_Needs")
)

df_mrp_full = df_mrp_full.withColumnRenamed("Plant", "Plant_").withColumnRenamed(
    "Material_Number", "Material_Number_"
)

# Keep 1 DataFrame for Needs coming from an another Plant (Interco_Need)
df_mrp_plant = df_mrp.filter(f.col("Plant") != f.col("Plant_Need"))
df_mrp_plant = df_mrp_plant.groupBy("Plant", "Material_Number").agg(
    f.sum("Quantity").alias("Interco_Need")
)

df_mrp_plant = df_mrp_plant.withColumnRenamed("Plant", "Plant__").withColumnRenamed(
    "Material_Number", "Material_Number__"
)

# Add Full need and interco needs to Material Master Data
df_material_needs = (
    df_material_price_end.alias("A")
    .join(
        df_mrp_full.alias("B"),
        (f.col("A.Plant") == f.col("B.Plant_"))
        & (f.col("A.Material_Number") == f.col("B.Material_Number_")),
        "left",
    )
    .join(
        df_mrp_plant.alias("C"),
        (f.col("A.Plant") == f.col("C.Plant__"))
        & (f.col("A.Material_Number") == f.col("C.Material_Number__")),
        "left",
    )
    .drop("Plant_", "Material_Number_", "Plant__", "Material_Number__")
)
# Gestion Null value
columns_to_replace = ["Total_Needs", "Interco_Need"]
df_material_needs = df_material_needs.na.fill(0, columns_to_replace)

# Calcul of net needs by plant
df_material_needs = df_material_needs.withColumn(
    "Net_Needs", f.col("Total_Needs") - f.col("Interco_Need")
)
df_material_needs = df_material_needs.drop("Interco_Need")

# Management of VB Planification (Reordo Point)
df_material_needs = df_material_needs.withColumn(
    "Net_Needs",
    f.when(
        (f.col("MRP_Type") == "VB")
        & (f.col("Net_Needs") == 0)
        & (f.col("Total_Needs") == 0),
        f.col("Reorder_Point") * 3,
    ).otherwise(f.col("Net_Needs")),
)

# Add Safety Stock to Needs
df_material_needs = df_material_needs.withColumn(
    "Total_Needs", f.col("Total_Needs") + f.col("Safety_Stock")
)
df_material_needs = df_material_needs.withColumn(
    "Net_Needs", f.col("Net_Needs") + f.col("Safety_Stock")
)
# Determination of Categorisation Parts
df_material_needs = df_material_needs.withColumn(
    "Needs_Category",
    f.when(f.col("Total_Needs") == 0, f.lit("No_Need")).otherwise(f.lit("Needs_OK")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Determination Final Assembly

# COMMAND ----------

# Gestion Null value
columns_to_replace = ["Total_Needs", "Net_Needs"]
df_material_assembly = df_material_needs.na.fill(0, columns_to_replace)


# If Net Needs upper than 0 or Need_Category = No_Need
# Final Assembly is Stock Site
df_material_assembly = df_material_assembly.withColumn(
    "Assy_Final",
    f.when(
        (f.col("Needs_Category") == "No_Need") | (f.col("Net_Needs") != 0),
        f.col("Stock_Site"),
    ).otherwise(f.lit("TBD")),
)

# When Total Needs => 0 we keep the assy plant with high value of needs
window_spec = Window.partitionBy(
    "Material_Number",
).orderBy(f.col("Net_Needs").desc())
df_with_row_number = df_material_assembly.withColumn(
    "row_number", f.row_number().over(window_spec)
)
df_result = df_with_row_number.filter(f.col("row_number") == 1)
df_result = df_result.select("Material_Number", "Assy_Final")
df_result = df_result.withColumnRenamed(
    "Material_Number", "Material_Number_"
).withColumnRenamed("Assy_Final", "Assy_Final_")
df_material_join = (
    df_material_assembly.alias("A")
    .join(
        df_result.alias("B"),
        f.col("A.Material_Number") == f.col("B.Material_Number_"),
        "left",
    )
    .drop("Material_Number_")
)
# At this point if final Assembly can't be determine, we have problems
# in dataSource SAP -> Put Garbage to highlight to business that something going wrong
df_material_assembly = df_material_join.withColumn(
    "Assy_Final",
    f.when(
        (f.col("Assy_Final") == "TBD") & (f.col("Assy_Final_") == "TBD"),
        f.lit("Garbage"),
    )
    .when(f.col("Assy_Final") == "TBD", f.col("Assy_Final_"))
    .otherwise(f.col("Assy_Final")),
)
df_material_assembly = df_material_assembly.drop("Assy_Final_")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Management Supplier
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### SAP Quota Management Creation DataBase

# COMMAND ----------

## Upload Quota table from SAP
# Upload header of Quota arragement
df_equk = spark.read.table(f"prod_bronze.sap_latecoere_ecc6.equk_latest")
df_equk = table_utils.add_prefix_to_columns(df_equk, "EQUK")  # <- Renaming // EQUK_XXXX

# Upload items of Quota arragement
df_equp = spark.read.table(f"prod_bronze.sap_latecoere_ecc6.equp_latest")
df_equp = table_utils.add_prefix_to_columns(df_equp, "EQUP")  # <- Renaming // EQUP_XXXX

# Keep only active Quota.
df_equk = df_equk.withColumn(
    "EQUK_bdatu", f.to_date(f.col("EQUK_bdatu"), "yyyyMMdd")
).withColumn("EQUK_vdatu", f.to_date(f.col("EQUK_vdatu"), "yyyyMMdd"))

df_equk = df_equk.filter(
    (f.col("EQUK_vdatu") < f.current_date()) & (f.col("EQUK_bdatu") > f.current_date())
)

# Join Quota Header and Item
df_quotas = df_equk.alias("A").join(
    df_equp.alias("B"), f.col("A.EQUK_qunum") == f.col("B.EQUP_qunum"), "left"
)
# Filter on Quota not cancelled
df_quotas = df_quotas.filter(f.col("EQUP_quote") != 0)

# Upload Gold Table Supplier for description upload
df_supplier = spark.read.table(
    f"prod_gold.supply_chain.supplier_master_data_latest_exposed"
)
df_supplier1 = df_supplier.select("Supplier_Code", "Supplier_Name")
df_supplier1 = df_supplier1.dropDuplicates()

# Join with supplier table for Description supplier
df_join_quotas = (
    df_quotas.alias("A")
    .join(
        df_supplier1.alias("B"),
        f.col("A.EQUP_lifnr") == f.col("B.Supplier_Code"),
        "left",
    )
    .drop("Supplier_Code")
)
# Determine Naming for Internal Parts
df_quotas_internal = df_join_quotas.withColumn(
    "Supplier_Name",
    f.when(f.col("EQUP_beskz") == "F", f.col("Supplier_Name")).otherwise(
        f.concat(f.lit("Internal_"), f.col("EQUK_werks"))
    ),
)

df_quotas_agg = df_quotas_internal.groupBy("EQUK_matnr", "EQUK_werks").agg(
    f.concat_ws(
        ", ",
        f.collect_list(
            f.concat_ws(
                " - ", df_quotas_internal.Supplier_Name, df_quotas_internal.EQUP_quote
            )
        ),
    ).alias("SuppliersAndQuotas")
)

# Cleansing raw data
df_quotas_clean = df_quotas_agg.withColumn(
    "EQUK_matnr", f.regexp_replace(df_quotas_agg.EQUK_matnr, "^0+", "")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SAP Source List Creation DataBase

# COMMAND ----------

# Upload Source List to determinate Supplier
df_source = spark.read.table(f"prod_bronze.sap_latecoere_ecc6.eord_latest")
df_source = table_utils.add_prefix_to_columns(
    df_source, "EORD"
)  # <- Renaming // EORD_XXXX

## Keep Only Active Source List in SAP
# Actual Date between Start_Validity_Date (EORD_vdatu) and End_Validity_Date (EORD_bdatu)
# And Supplier allocated to material Number(EORD_flifn or EORD_febel)
df_source = df_source.withColumn(
    "EORD_bdatu", f.to_date(f.col("EORD_bdatu"), "yyyyMMdd")
).withColumn("EORD_vdatu", f.to_date(f.col("EORD_vdatu"), "yyyyMMdd"))

df_source = df_source.filter(
    (f.col("EORD_vdatu") < f.current_date())
    & (f.col("EORD_bdatu") > f.current_date())
    & (
        (f.col("EORD_flifn").isNotNull())
        | (f.col("EORD_febel").isNotNull())
        | (f.col("EORD_autet") == "1")
    )
)
# If we have several active entries for plant / Material_Number / Purchase Organization
# We sort by plannification flag (EORD_autet),then by Prefered source, then by Supplier affectation
# and we take the first one.
df_source = df_source.select(
    "EORD_matnr",
    "EORD_werks",
    "EORD_lifnr",
    "EORD_ekorg",
    "EORD_autet",
    "EORD_febel",
    "EORD_flifn",
    "EORD_ebeln",
    "EORD_ebelp",
)

window_spec = Window.partitionBy("EORD_matnr", "EORD_werks").orderBy(
    f.desc(f.col("EORD_autet")),
    f.desc(f.col("EORD_febel")),
    f.desc(f.col("EORD_flifn")),
    f.desc(f.col("EORD_lifnr")),
    f.desc(f.col("EORD_ebeln")),
    f.desc(f.col("EORD_ebelp"))
)
# Add row number
df_source = df_source.withColumn("row_number", f.rank().over(window_spec))
# Keep only one entry
df_source = df_source.filter(f.col("row_number") == 1).drop("row_number","EORD_ebeln","EORD_ebelp")
df_source = df_source.dropDuplicates(["EORD_matnr", "EORD_werks"])

# Take Gold Table Supplier for description
df_supplier = df_supplier.withColumnRenamed("Plant", "Plant_").withColumnRenamed(
    "Supplier_name", "Supplier_name_"
)
df_supplier = df_supplier.select(
    "Supplier_Code", "Supplier_name_", "Purch_Organization", "Plant_"
)
# Join Source List and Supplier Table on Supplier_Code and Purch_Organization
df_source_list = (
    df_source.alias("A")
    .join(
        df_supplier.alias("B"),
        (f.col("A.EORD_lifnr") == f.col("B.Supplier_Code"))
        & (f.col("A.EORD_ekorg") == f.col("B.Purch_Organization")),
        "left",
    )
    .drop("Purch_Organization", "EORD_lifnr", "EORD_ekorg", "EORD_autet")
)
# Cleansing -> Remove leading zeros on Material_Number
df_source_list = df_source_list.withColumn(
    "EORD_matnr", f.regexp_replace(df_source_list.EORD_matnr, "^0+", "")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Material DataBase with Quotas Arrangement and Source List

# COMMAND ----------

df_material_supplier_join = (
    df_material_assembly.alias("A")
    .join(
        df_quotas_clean.alias("B"),
        (f.col("A.Material_Number") == f.col("B.EQUK_matnr"))
        & (f.col("A.Plant") == f.col("B.EQUK_werks")),
        "left",
    )
    .join(
        df_source_list.alias("C"),
        (f.col("A.Material_Number") == f.col("C.EORD_matnr"))
        & (f.col("A.Plant") == f.col("C.EORD_werks")),
        "left",
    )
    .drop("EORD_matnr", "EORD_werks", "EQUK_matnr", "EQUK_werks")
)
# In SAP Quotas Arrangement take the lead on Source List
# Activation of Quota is manage in SAP through Quotas_Usage field in Master Data
# 3 or 4 value -> Activate Quota arrangement.

df_material_supplier_name = df_material_supplier_join.withColumn(
    "Supplier_Name",
    f.when(
        (f.col("Quota_Usage") == "4") | (f.col("Quota_Usage") == "3"),
        f.col("SuppliersAndQuotas"),
    ).otherwise(f.col("Supplier_name_")),
)
df_material_supplier_name = df_material_supplier_name.withColumn(
    "Supplier_Code",
    f.when(
        (f.col("Quota_Usage") == "4") | (f.col("Quota_Usage") == "3"), f.lit("Quotas")
    ).otherwise(f.col("Supplier_Code")),
)
df_material_supplier_name = df_material_supplier_name.drop(
    "Supplier_name_", "SuppliersAndQuotas"
)

# Management of Internal (not Include in Source list)
# Internal Parts are not managed by Source List
# If Procurement Type is E(Internal) We create a supplier Name
# with prefix INT_ and Plant value
df_material_supplier_int = df_material_supplier_name.withColumn(
    "Supplier_Code",
    f.when(
        f.col("Procurement_Type") == "E", f.concat(f.lit("INT_"), f.col("Plant"))
    ).otherwise(f.col("Supplier_Code")),
)
df_material_supplier_int = df_material_supplier_int.withColumn(
    "Supplier_name",
    f.when(
        f.col("Procurement_Type") == "E", f.concat(f.lit("INTERNAL_"), f.col("Plant"))
    ).otherwise(f.col("Supplier_name")),
)
df_material_supplier_int = df_material_supplier_int.withColumn(
    "Plant_",
    f.when(f.col("Procurement_Type") == "E", f.lit(None)).otherwise(f.col("Plant_")),
)
# Management of Intercompany FLows
# Plant_Buy determine where the parts are bought or produced
df_material_supplier_int = df_material_supplier_int.withColumn(
    "Plant_Buy",
    f.when(f.col("Plant_").isNull(), f.col("Plant")).otherwise(f.col("Plant_")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Management of Intraco Flows

# COMMAND ----------

# Upload Master Data for Intraco Flow
# Determine with plant is linked with Special Procurement Type
df_intraco = spark.read.table(f"prod_bronze.sap_latecoere_ecc6.t460a_latest").select(
    "werks", "sobsl", "wrk02"
)

# Join Material MAster data with Custo IntraCo Flow
df_update_intraco = (
    df_material_supplier_int.alias("A")
    .join(
        df_intraco.alias("B"),
        (f.col("A.Plant") == f.col("B.werks"))
        & (f.col("A.Procurement_Special_Type") == f.col("B.sobsl")),
        "left",
    )
    .drop("werks", "sobsl")
)
# Take priority on Plant linked to Special Procurement Type (T460A)
df_update_intraco = df_update_intraco.withColumn(
    "Plant_",
    f.when(f.col("wrk02").isNotNull(), f.col("wrk02")).otherwise(f.col("Plant_")),
)
df_update_intraco = df_update_intraco.withColumn(
    "Plant_buy",
    f.when(f.col("wrk02").isNotNull(), f.col("wrk02")).otherwise(f.col("Plant_buy")),
)
df_update_intraco = df_update_intraco.drop("wrk02")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Management of InterCompany Flows

# COMMAND ----------

## AS some plant buy or produce Part for others plant, we need to take right Supplier or Internal site
# For that we make same operation 3 times (manage 6 interco_Flow on a row )
# to be sure that we have the first Supplier and not an Interco_Supplier
# 2023 we have max 3 intercoFlow on a row
# 1st Loop
df_material_supplier_1 = df_update_intraco.select(
    "Plant",
    "Material_Number",
    "Supplier_Code",
    "Supplier_name",
    "Plant_",
    "Production_Site",
)

df_material_supplier_1 = (
    df_material_supplier_1.withColumnRenamed("Material_Number", "Material_Number1")
    .withColumnRenamed("Supplier_Code", "Supplier_Code1")
    .withColumnRenamed("Supplier_name", "Supplier_name1")
    .withColumnRenamed("Plant", "Plant1")
    .withColumnRenamed("Plant_", "Plant11")
    .withColumnRenamed("Production_Site", "Production_Site1")
)
# Join Material Master with Interco_Plant
df_material_supplier_join_1 = (
    df_update_intraco.alias("A")
    .join(
        df_material_supplier_1.alias("B"),
        (f.col("A.Material_Number") == f.col("B.Material_Number1"))
        & (f.col("A.Plant_") == f.col("B.Plant1")),
        "left",
    )
    .drop("Material_Number1", "Plant1")
)
# Management Interco rules
df_material_supplier_first = df_material_supplier_join_1.withColumn(
    "Supplier_Code", f.coalesce(f.col("Supplier_Code1"), f.col("Supplier_Code"))
)

df_material_supplier_first = df_material_supplier_first.withColumn(
    "Supplier_name", f.coalesce(f.col("Supplier_name1"), f.col("Supplier_name"))
)

df_material_supplier_first = df_material_supplier_first.withColumn(
    "Plant_", f.coalesce(f.col("Plant11"), f.col("Plant_"))
)

df_material_supplier_first = df_material_supplier_first.withColumn(
    "Plant_Buy", f.coalesce(f.col("Plant_"), f.col("Plant_Buy"))
)

df_material_supplier_first = df_material_supplier_first.withColumn(
    "Production_Site", f.coalesce(f.col("Production_Site1"), f.col("Production_Site"))
)

df_material_supplier_first = df_material_supplier_first.drop(
    "Supplier_Code1", "Supplier_name1", "Plant11", "Production_Site1"
)


# ---------------------------------------------------------------------------------------
# 2nd Loop
df_material_supplier_2 = df_material_supplier_first.select(
    "Plant",
    "Material_Number",
    "Supplier_Code",
    "Supplier_name",
    "Plant_",
    "Production_Site",
)

df_material_supplier_2 = (
    df_material_supplier_2.withColumnRenamed("Material_Number", "Material_Number2")
    .withColumnRenamed("Supplier_Code", "Supplier_Code2")
    .withColumnRenamed("Supplier_name", "Supplier_name2")
    .withColumnRenamed("Plant", "Plant2")
    .withColumnRenamed("Plant_", "Plant22")
    .withColumnRenamed("Production_Site", "Production_Site2")
)
df_material_supplier_join_2 = (
    df_material_supplier_first.alias("A")
    .join(
        df_material_supplier_2.alias("B"),
        (f.col("A.Material_Number") == f.col("B.Material_Number2"))
        & (f.col("A.Plant_") == f.col("B.Plant2")),
        "left",
    )
    .drop("Material_Number2", "Plant2")
)
# Management Interco rules
df_material_supplier_second = df_material_supplier_join_2.withColumn(
    "Supplier_Code", f.coalesce(f.col("Supplier_Code2"), f.col("Supplier_Code"))
)
df_material_supplier_second = df_material_supplier_second.withColumn(
    "Supplier_name", f.coalesce(f.col("Supplier_name2"), f.col("Supplier_name"))
)

df_material_supplier_second = df_material_supplier_second.withColumn(
    "Plant_", f.coalesce(f.col("Plant22"), f.col("Plant_"))
)
df_material_supplier_second = df_material_supplier_second.withColumn(
    "Plant_Buy", f.coalesce(f.col("Plant_"), f.col("Plant_Buy"))
)

df_material_supplier_second = df_material_supplier_second.withColumn(
    "Production_Site", f.coalesce(f.col("Production_Site2"), f.col("Production_Site"))
)

df_material_supplier_second = df_material_supplier_second.drop(
    "Supplier_Code2", "Supplier_name2", "Plant22", "Production_Site2"
)

# ---------------------------------------------------------------------------------------
# Last Loop
df_material_supplier_3 = df_material_supplier_second.select(
    "Plant",
    "Material_Number",
    "Supplier_Code",
    "Supplier_name",
    "Plant_",
    "Production_Site",
)

df_material_supplier_3 = (
    df_material_supplier_3.withColumnRenamed("Material_Number", "Material_Number3")
    .withColumnRenamed("Supplier_Code", "Supplier_Code3")
    .withColumnRenamed("Supplier_name", "Supplier_name3")
    .withColumnRenamed("Plant", "Plant3")
    .withColumnRenamed("Plant_", "Plant33")
    .withColumnRenamed("Production_Site", "Production_Site3")
)

df_material_supplier_join_3 = (
    df_material_supplier_second.alias("A")
    .join(
        df_material_supplier_3.alias("B"),
        (f.col("A.Material_Number") == f.col("B.Material_Number3"))
        & (f.col("A.Plant_") == f.col("B.Plant3")),
        "left",
    )
    .drop("Material_Number3", "Plant3")
)
# Management Interco rules

df_material_supplier_third = df_material_supplier_join_3.withColumn(
    "Supplier_Code", f.coalesce(f.col("Supplier_Code3"), f.col("Supplier_Code"))
)

df_material_supplier_third = df_material_supplier_third.withColumn(
    "Supplier_name", f.coalesce(f.col("Supplier_name3"), f.col("Supplier_name"))
)
df_material_supplier_third = df_material_supplier_third.withColumn(
    "Plant_", f.coalesce(f.col("Plant33"), f.col("Plant_"))
)

df_material_supplier_third = df_material_supplier_third.withColumn(
    "Plant_Buy", f.coalesce(f.col("Plant_"), f.col("Plant_Buy"))
)

df_material_supplier_third = df_material_supplier_third.withColumn(
    "Production_Site", f.coalesce(f.col("Production_Site3"), f.col("Production_Site"))
)

df_material_supplier_third = df_material_supplier_third.drop(
    "Supplier_Code3", "Supplier_name3", "Plant33", "Production_Site3"
)
df_material_supplier_third = df_material_supplier_third.drop("Plant_", "T460A_wrk02")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Management MRP Unit (Specific IS)

# COMMAND ----------

# Upload Master Data MRP Unit from SAP
df_mdma = spark.read.table(f"prod_bronze.sap_latecoere_ecc6.mdma_latest")
df_mdma = table_utils.add_prefix_to_columns(df_mdma, "MDMA")  # <- Renaming // MDMA_XXXX

df_material_supplier_temp = df_material_supplier_third.drop(
    "MRP_Type",
    "MRP_Controller",
    "MRP_Group",
    "Reorder_Point",
    "Batch_Size",
    "Rounding_Value",
    "Min_Lot_Size",
    "Max_Lot_Size",
    "Max_Stock_Level",
    "Procurement_Special_Type",
    "Storage_Location",
    "Storage_Location_External",
    "Safety_Stock",
    "Safety_Time",
    "Fixed_Lot_Size",
    "Safety_Time_Code",
    "External_Lead_Time",
    "MRP_Unit",
)

df_material_mrp_unit = df_mdma.alias("A").join(
    df_material_supplier_temp.alias("B"),
    (f.col("A.MDMA_matnr") == f.col("B.Material_Number"))
    & (f.col("A.MDMA_werks") == f.col("B.Plant")),
    "inner",
)
df_material_mrp_unit = (
    df_material_mrp_unit.withColumnRenamed("MDMA_dismm", "MRP_Type")
    .withColumnRenamed("MDMA_dispo", "MRP_Controller")
    .withColumnRenamed("MDMA_disgr", "MRP_Group")
    .withColumnRenamed("MDMA_minbe", "Reorder_Point")
    .withColumnRenamed("MDMA_disls", "Batch_Size")
    .withColumnRenamed("MDMA_bstrf", "Rounding_Value")
    .withColumnRenamed("MDMA_bstmi", "Min_Lot_Size")
    .withColumnRenamed("MDMA_bstma", "Max_Lot_Size")
    .withColumnRenamed("MDMA_mabst", "Max_Stock_Level")
    .withColumnRenamed("MDMA_sobsl", "Procurement_Special_Type")
    .withColumnRenamed("MDMA_lgpro", "Storage_Location")
    .withColumnRenamed("MDMA_lgfsb", "Storage_Location_External")
    .withColumnRenamed("MDMA_eisbe", "Safety_Stock")
    .withColumnRenamed("MDMA_shzet", "Safety_Time")
    .withColumnRenamed("MDMA_bstfe", "Fixed_Lot_Size")
    .withColumnRenamed("MDMA_shflg", "Safety_Time_Code")
    .withColumnRenamed("MDMA_plifz", "External_Lead_Time")
    .withColumnRenamed("MDMA_berid", "MRP_Unit")
)
Begin_By = "MDMA"
df_material_mrp_unit = df_material_mrp_unit.select(
    [col for col in df_material_mrp_unit.columns if not col.startswith(Begin_By)]
)
df_material_mrp = df_material_supplier_third.unionByName(
    df_material_mrp_unit, allowMissingColumns=True
)

df_material_mrp = df_material_mrp.withColumn(
    "MRP_Unit", f.coalesce(f.col("MRP_Unit"), f.col("Plant"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Management Price Upper Green-Line

# COMMAND ----------

# For all Material upper the Green-Line (ie :IndivColl = 1)
# Price is collected on WBS, Standard and average price need to set to 0
# Normally done in SAP but checked again here
df_material_end = df_material_mrp.withColumn(
    "Standard_Price_EUR_Budget",
    f.when(f.col("Indivcoll") == "1", f.lit(0)).otherwise(
        f.col("Standard_Price_EUR_Budget")
    ),
)
df_material_end = df_material_end.withColumn(
    "Average_Price_EUR_Budget",
    f.when(f.col("Indivcoll") == "1", f.lit(0)).otherwise(
        f.col("Average_Price_EUR_Budget")
    ),
)
df_material_end = df_material_end.withColumn(
    "Standard_Price_EUR_Spot",
    f.when(f.col("Indivcoll") == "1", f.lit(0)).otherwise(
        f.col("Standard_Price_EUR_Spot")
    ),
)
df_material_end = df_material_end.withColumn(
    "Average_Price_EUR_Spot",
    f.when(f.col("Indivcoll") == "1", f.lit(0)).otherwise(
        f.col("Average_Price_EUR_Spot")
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Management Category of Product

# COMMAND ----------

# Upload Manual Table for Category of Parts
# On AS side, Mrp Controller gives information about Product Category related to MRP Controller
# On IS side no rules clearly defined yet, so Categorization is set to OTHERS
df_cat = spark.read.table(f"prod_bronze.manual_input.mrp_controller_category").select(
    "Plant", "Mrp_controller", "Category"
)
df_cat = df_cat.withColumnRenamed("Plant", "Plant_").withColumnRenamed(
    "Mrp_controller", "Mrp_controller_"
)
df_join_material = (
    df_material_end.alias("A")
    .join(
        df_cat.alias("B"),
        (f.col("A.Plant") == f.col("B.Plant_"))
        & (f.col("A.MRP_Controller") == f.col("B.Mrp_controller_")),
        "left",
    )
    .drop("Mrp_controller_", "Plant_")
)

df_join_material = df_join_material.withColumn(
    "Category", f.coalesce(f.col("Category"), f.lit("Tbd"))
)
# Management of Entries not found in manual table
# IS Management also we take Production site to determine Category
df_join_material = df_join_material.withColumn(
    "Category",
    f.when(
        f.col("MRP_Controller").isNull(),
        f.concat(f.substring(f.col("Production_Site"), 1, 3), f.lit("-Others")),
    )
    .when(
        (f.col("Branch") == "IS") & (~f.col("Plant").startswith("1")),
        f.concat(f.substring(f.col("Production_Site"), 6, 6), f.lit("-Others")),
    )
    .when(
        (f.col("Branch") == "IS") & (f.col("Plant").startswith("1")),
        f.concat(f.substring(f.col("Production_Site"), 1, 3), f.lit("-Others")),
    )
    .otherwise(f.col("Category")),
)
df_material_end = df_join_material.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Material Profit Center Composition

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Table

# COMMAND ----------

# Get old master_material_data to handle null values
df_last_master_data_snapshot = spark.read.table(
    f"{READ_ENV}_gold.master_data.material_master_data_plant"
)

# Upload MRP_List in order to Extract needs
df_mrp = spark.read.table(f"{READ_ENV}_gold.mrp.list_mrp_full_exposed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Utils functions

# COMMAND ----------

def extract_needs_quantity(df_mrp: DataFrame) -> DataFrame:
    """
    Filters and processes a DataFrame containing Material Resource Planning (MRP) data to compute the sum of needs for each unique combination of material number, material number origin, profit center, and plant.

    :param df_mrp: The input DataFrame containing MRP data.
    :type df_mrp: DataFrame
    :return: A DataFrame grouped by material number, material number origin, profit center, and plant with the summed quantity of needs (negative movements) under the column "needs_quantity".
    :rtype: DataFrame
    """
    # Filter MRP DataAsset -> Keep only Needs from Mrp List (-: needs / +:Resources)
    df_mrp = df_mrp.filter(f.col("Code_mvt") == "-")
    df_mrp = df_mrp.withColumn("Quantity", f.col("Quantity").cast("float"))

    df_mrp_quantity = df_mrp.groupBy(
        "Material_Number", "Plant", "Planif_unit", "Material_Number_Origin"
    ).agg(f.sum("Quantity").alias("needs_quantity"))
    return df_mrp_quantity


def add_profit_center_origin(
    df_material_end: DataFrame, df_mrp_quantity: DataFrame
) -> DataFrame:
    """
    Joins a DataFrame containing material master data with another DataFrame containing MRP needs quantity data to associate the profit center origin of each material number Origin.

    :param df_material_end: The material master data DataFrame.
    :type df_material_end: DataFrame
    :param df_mrp_quantity: The MRP needs quantity DataFrame to be enriched with profit center origin information.
    :type df_mrp_quantity: DataFrame
    :return: A DataFrame resulting from the MRP needs quantity DataFrame with the material master data, enriched with the profit center origin for each material number origin.
    :rtype: DataFrame
    """
    filter_material_df = df_material_end.select(
        "Material_Number", "Plant", "MRP_Unit", "Profit_Center"
    )
    filter_material_df = filter_material_df.dropDuplicates(
        ["Material_Number", "Plant", "MRP_Unit", "Profit_Center"]
    )
    filter_material_df = (
        filter_material_df.withColumnRenamed("Profit_Center", "Profit_Center_Origin")
        .withColumnRenamed("Plant", "Plant_ref")
        .withColumnRenamed("Material_Number", "Material_Number_ref")
    )

    df_mrp_origin_profit_center = df_mrp_quantity.join(
        filter_material_df,
        (
            df_mrp_quantity["Material_Number_Origin"]
            == filter_material_df["Material_Number_ref"]
        )
        & (df_mrp_quantity["Plant"] == filter_material_df["Plant_ref"])
        & (df_mrp_quantity["Planif_unit"] == filter_material_df["MRP_Unit"]),
        "left",
    )
    df_mrp_origin_profit_center = df_mrp_origin_profit_center.drop(
        "Plant_ref", "Material_Number_ref", "MRP_Unit"
    )
    df_mrp_origin_profit_center = df_mrp_origin_profit_center.withColumnRenamed(
        "Planif_unit", "MRP_Unit"
    )
    return df_mrp_origin_profit_center


def add_total_quantity_needs(df_mrp_origin_profit_center: DataFrame) -> DataFrame:
    """
    Enhances a DataFrame containing MRP needs quantity data with a total needs quantity for each material number by summing up the needs quantities across all origins and plants.

    :param df_mrp_origin_profit_center: The MRP needs quantity DataFrame to be enriched with total needs quantities.
    :type df_mrp_origin_profit_center: DataFrame
    :return: The input DataFrame enriched with a new column "total_needs_quantity" representing the sum of needs quantities for each material number across all origins and plants.
    :rtype: DataFrame
    """
    # Create new df that sum stock quantity for Each Material_Number
    mrp_total_quantity_df = df_mrp_origin_profit_center.groupBy(
        "Material_Number", "Plant", "MRP_Unit"
    ).agg(f.sum("needs_quantity").alias("total_needs_quantity"))

    mrp_total_quantity_df = (
        mrp_total_quantity_df.withColumnRenamed(
            "Material_Number", "Material_Number_tot"
        )
        .withColumnRenamed("Plant", "Plant_tot")
        .withColumnRenamed("MRP_Unit", "MRP_Unit_tot")
    )

    mrp_full = df_mrp_origin_profit_center.join(
        mrp_total_quantity_df,
        (
            (
                mrp_total_quantity_df["Material_Number_tot"]
                == df_mrp_origin_profit_center["Material_Number"]
            )
            & (
                mrp_total_quantity_df["Plant_tot"]
                == df_mrp_origin_profit_center["Plant"]
            )
            & (
                mrp_total_quantity_df["MRP_Unit_tot"]
                == df_mrp_origin_profit_center["MRP_Unit"]
            )
        ),
        "left",
    )
    mrp_full = mrp_full.drop(
        "Material_Number_tot", "Plant_tot", "Material_Number_Origin"
    )
    return mrp_full


def fill_na_with_label(df: DataFrame, label: str) -> DataFrame:
    """
    Fills null values in the 'Profit_Center_Origin' column of the given DataFrame with a specified label.

    :param df: The DataFrame containing 'Profit_Center_Origin' column with null values to be filled.
    :type df: DataFrame
    :param label: The label to fill in for null values in the 'Profit_Center_Origin' column.
    :type label: str
    :return: The DataFrame with null values in the 'Profit_Center_Origin' column filled with the specified label.
    :rtype: DataFrame
    """
    mrp_full = df.na.fill(value=label, subset=["Profit_Center_Origin"])
    return mrp_full


def compute_PF_affectation(df_mrp_full: DataFrame) -> DataFrame:
    """
    Creates a new column 'PF_pourcentage' in the 'df_mrp_full' DataFrame that calculates, for each combination of 'Material_Number', the percentage of 'Profit_Center' quantity used out of the total needs quantity.

    :param df_mrp_full: The MRP DataFrame to calculate the 'Profit_Center' usage percentage.
    :type df_mrp_full: DataFrame
    :return: The 'df_mrp_full' DataFrame with a new column 'PF_pourcentage' representing the calculated percentage.
    :rtype: DataFrame
    """
    df_mrp_composition = df_mrp_full.withColumn(
        "PF_pourcentage",
        f.create_map(
            f.col("Profit_Center_Origin"),
            f.col("needs_quantity") / f.col("total_needs_quantity"),
        ),
    )
    return df_mrp_composition


def merge_dicts(dict_list):
    """
    Merges dictionaries within a list into a single dictionary, aggregating values by keys.

    :param dict_list: A list of dictionaries to be merged. Each dictionary represents
                      a 'Profit_Center' to its percentage allocation for a 'Material_Number'.
    :type dict_list: list
    :return: A single dictionary with keys from the input dictionaries and aggregated values.
             If initial values for a key are None, they contribute to an equal distribution
             based on the total sum of non-None values.
    :rtype: dict
    """
    result = {}
    nb_null_value = 0
    tot_value = 0
    PF_composition = {}
    for dictionary in dict_list:
        for key, value in dictionary.items():
            if value is None:
                nb_null_value += 1
            else:
                tot_value += value

    for dictionary in dict_list:
        for key, value in dictionary.items():
            if key in result:
                if value is None:
                    if tot_value == 0:
                        result[key] += 1 / nb_null_value
                else:
                    result[key] += value
            else:
                if value is None:
                    if tot_value == 0:
                        result[key] = 1 / nb_null_value
                else:
                    result[key] = value
    return result


def compute_global_PF_affectation(df_mrp_composition: DataFrame) -> DataFrame:
    """
    Computes the global 'Profit Center' (PF) affectation percentages for each 'Material_Number'
    by merging individual 'PF_pourcentage' dictionaries into a single dictionary per 'Material_Number'.

    :param df_mrp_composition: A DataFrame containing 'Material_Number' and 'PF_pourcentage'.
    :type df_mrp_composition: DataFrame
    :return: DataFrame
    :rtype: DataFrame
    """
    merge_dicts_udf = udf(merge_dicts, MapType(StringType(), DoubleType()))
    df_material_composition = df_mrp_composition.groupBy(
        "Material_Number", "Plant", "MRP_Unit"
    ).agg(
        merge_dicts_udf(f.collect_list("PF_pourcentage")).alias(
            "PF_pourcentage_affectation"
        )
    )
    return df_material_composition


def set_na_w_last_snapshot(
    df_last_master_data_snapshot: DataFrame, df_material_composition: DataFrame
) -> DataFrame:
    """
    Set Null Values of PF_pourcentage_affectation with last snapshot.

    :param df_last_master_data_snapshot: The DataFrame containing the last master data snapshot.
    :type df_last_master_data_snapshot: DataFrame
    :param df_material_composition: The DataFrame containing material composition data.
    :type df_material_composition: DataFrame
    :return: The DataFrame with replaced unknown values in the 'PF_pourcentage_affectation' column.
    :rtype: DataFrame
    """
    affectation_column_name = "PF_pourcentage_affectation"
    if affectation_column_name in df_last_master_data_snapshot.columns:
        df_last_master_data_snapshot = df_last_master_data_snapshot.select(
            "Material_Number", "Plant", "PF_pourcentage_affectation", "MRP_Unit"
        )
        df_last_master_data_snapshot = df_last_master_data_snapshot.dropDuplicates(
            ["Material_Number", "Plant", "MRP_Unit"]
        )
        df_last_master_data_snapshot = (
            df_last_master_data_snapshot.withColumnRenamed("Plant", "Plant_last")
            .withColumnRenamed("Material_Number", "Material_Number_last")
            .withColumnRenamed("MRP_Unit", "MRP_Unit_last")
            .withColumnRenamed(
                "PF_pourcentage_affectation", "PF_pourcentage_affectation_last"
            )
        )
        df_material_composition_tot = df_material_composition.join(
            df_last_master_data_snapshot,
            (
                df_last_master_data_snapshot["Material_Number_last"]
                == df_material_composition["Material_Number"]
            )
            & (
                df_last_master_data_snapshot["Plant_last"]
                == df_material_composition["Plant"]
            )
            & (
                df_last_master_data_snapshot["MRP_Unit_last"]
                == df_material_composition["MRP_Unit"]
            ),
            "left",
        )
        df_material_composition_without_null_values = (
            df_material_composition_tot.withColumn(
                "PF_pourcentage_affectation",
                f.when(
                    f.col("PF_pourcentage_affectation").isNull(),
                    f.col("PF_pourcentage_affectation_last"),
                ).otherwise(
                    f.col("PF_pourcentage_affectation"),
                ),
            )
        )
        df_material_composition = df_material_composition_without_null_values.drop(
            "Plant_last",
            "Material_Number_last",
            "MRP_Unit_last",
            "PF_pourcentage_affectation_last",
        )
    return df_material_composition


def handle_unknown_values_from_affectation(df_material: DataFrame) -> DataFrame:
    """
    Handle affectation flagged as "unknown" and Null. 

    :param df_material: material master data
    :type df_material: DataFrame
    :return: _description_
    :rtype: DataFrame
    """
    allocate_profit_center_base_condition = (
        (f.col("Profit_Center_Affectation") == "Unknown")
        | f.isnull(f.col("Profit_Center_Affectation"))
    ) & (
        (~f.col("Profit_Center").startswith("98"))
        | (~f.col("Profit_Center").endswith("W"))
    )

    allocate_others_condition = (
        (f.col("Profit_Center_Affectation") == "Unknown")
        | f.isnull(f.col("Profit_Center_Affectation"))
    ) & (
        (f.col("Profit_Center").startswith("98"))
        | (f.col("Profit_Center").endswith("W"))
    )

    df_exploded = df_material.select(
        f.col("*"),
        f.explode_outer(f.col("PF_pourcentage_affectation")).alias(
            "Profit_Center_Affectation", "value"
        ),
    ).withColumn(
        "value",
        f.when(f.col("PF_pourcentage_affectation").isNull(), f.lit(1)).otherwise(f.col("value"))
    )

    df_transformed = df_exploded.withColumn(
        "Profit_Center_Affectation",
        f.when(
            allocate_profit_center_base_condition,
            df_exploded["Profit_Center"],
        )
        .when(
            allocate_others_condition,
            f.lit("Others"),
        )
        .when(
            (f.col("Profit_Center").isNull()) & (f.col("Profit_Center_Affectation").isNull()),
            f.lit("Unknown")
        )
        .otherwise(df_exploded["Profit_Center_Affectation"]),
    )
    df_grouped_by_PF_affectation = df_transformed.groupBy(
        "Material_Number", "Plant", "MRP_Unit", "Profit_Center_Affectation"
    ).agg(f.sum("value").alias("value"))
    df_grouped = (
        df_grouped_by_PF_affectation.groupBy("Material_Number", "Plant", "MRP_Unit")
        .agg(
            f.collect_list(f.expr("struct(Profit_Center_Affectation, value)")).alias(
                "key_value_pairs"
            )
        )
        .withColumn("PF_pourcentage_affectation", f.map_from_entries("key_value_pairs"))
        .select("Material_Number", "Plant", "MRP_Unit", "PF_pourcentage_affectation")
    )
    return df_grouped


def join_with_material_master_data_base(
    material_master_data: DataFrame, df_right: DataFrame
) -> DataFrame:
    """
    Join sub dataframe of material master data into material master data on "Material_Number", "Plant", "MRP_Unit".

    :param material_master_data: material master data reference
    :type material_master_data: DataFrame
    :param df_right: Sub dataframe of material master data
    :type df_right: DataFrame
    :return: Joined dataframe
    :rtype: DataFrame
    """
    df_material_composition = (
        df_right.withColumnRenamed("Material_Number", "Material_Number_Composition")
        .withColumnRenamed("Plant", "Plant_composition")
        .withColumnRenamed("MRP_Unit", "MRP_Unit_composition")
    )
    df_material_end = material_master_data.join(
        df_material_composition,
        (
            material_master_data["Material_Number"]
            == df_material_composition["Material_Number_Composition"]
        )
        & (
            material_master_data["Plant"]
            == df_material_composition["Plant_composition"]
        )
        & (
            material_master_data["MRP_Unit"]
            == df_material_composition["MRP_Unit_composition"]
        ),
        "left",
    )
    df_material_end = df_material_end.drop(
        "Material_Number_Composition", "Plant_composition", "MRP_Unit_composition"
    )
    return df_material_end

def transform_map(map_col):
    if map_col is None:
        return None
    return {
        k: round(1.0 if v > 1 else v, 2) if v is not None else None
        for k, v in map_col.items()
    }

transform_map_udf = f.udf(transform_map, MapType(StringType(), DoubleType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compute

# COMMAND ----------

df_mrp_quantity = extract_needs_quantity(df_mrp=df_mrp)
df_mrp_origin_profit_center = add_profit_center_origin(
    df_material_end=df_material_end, df_mrp_quantity=df_mrp_quantity
)
df_final_mrp = add_total_quantity_needs(
    df_mrp_origin_profit_center=df_mrp_origin_profit_center
)
df_final_mrp = fill_na_with_label(df=df_final_mrp, label="Unknown")
df_mrp_composition = compute_PF_affectation(df_mrp_full=df_final_mrp)
df_material_composition_compute = compute_global_PF_affectation(
    df_mrp_composition=df_mrp_composition
)
df_material_joined = join_with_material_master_data_base(
    material_master_data=df_material_end, df_right=df_material_composition_compute
)
df_material_composition = set_na_w_last_snapshot(
    df_last_master_data_snapshot=df_last_master_data_snapshot,
    df_material_composition=df_material_joined,
)
df_material_complete = handle_unknown_values_from_affectation(
    df_material=df_material_composition
)
df_material_end = join_with_material_master_data_base(
    material_master_data=df_material_end, df_right=df_material_complete
).withColumn(
    "PF_pourcentage_affectation", transform_map_udf(f.col("PF_pourcentage_affectation"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Management MEKKO Classification  
# MAGIC Creation of a special classification of part for MEKKO PBI Visualisation

# COMMAND ----------

#  Upload Caracteristic Part from Bronze table
df_carac_part = spark.read.table(f"{READ_ENV}_bronze.sap_latecoere_ecc6.ausp_latest")
df_carac_part = df_carac_part.select(
    f.col("objek").alias("Material_Number"),
    f.col("atinn"),
    f.col("klart"),
    f.col("atwrt"),
)
df_carac_part = df_carac_part.withColumn(
    "Material_Number", f.regexp_replace(df_carac_part.Material_Number, "^0+", "")
)
# -------------------------------------------------------------------------------------
# According with business rules
# We keep only material Classification (klart->001)
# and special group "Assembly & Standard"(255,266,311)
classif_atinn = ["0000000255", "0000000266", "0000000331"]
df_carac_part = df_carac_part.filter(
    (f.col("klart") == "001") & (f.col("atinn").isin(classif_atinn))
)
# -------------------------------------------------------------------------------------
# Translation business for atwrt SAP Field
df_carac_part = (
    df_carac_part.withColumn(
        "Mekko_classif",
        f.when(
            (f.col("atinn") == "0000000266") & (f.col("atwrt") == "02"),
            f.lit("6-RAW_MATERIAL"),
        )
        .when(
            (f.col("atinn") == "0000000255") & (f.col("atwrt") == "BZ"),
            f.lit("6-RAW_MATERIAL"),
        )
        .when(
            (f.col("atinn") == "0000000266") & (f.col("atwrt") != "02"),
            f.lit("5-STANDARD_PARTS"),
        )
        .when(
            (f.col("atinn") == "0000000255") & (f.col("atwrt") == "AA"),
            f.lit("2-SUB_ASSY"),
        )
        .when(
            (
                (f.col("atinn") == "0000000255")
                & ((f.col("atwrt") == "AB") | (f.col("atwrt") == "AC"))
            ),
            f.lit("1-ASSY"),
        )
        .when(
            (f.col("atinn") == "0000000331") & (f.col("atwrt") == "01"),
            f.lit("1-ASSY"),
        )
        .otherwise(f.lit("DETAIL PART")),
    )
    .drop("atinn", "klart", "atwrt")
    .dropDuplicates(["Material_Number"])
)
# -------------------------------------------------------------------------------------
df_material_end_join = df_material_end.join(df_carac_part, ["Material_Number"], "left")
# -------------------------------------------------------------------------------------
# Management Assy Sub Assy according individual Category for AS
df_material_end_join = df_material_end_join.withColumn(
    "Mekko_classif",
    f.when(
        (
            (f.col("Mekko_classif") == "1-ASSY")
            | (f.col("Mekko_classif") == "2-SUB_ASSY")
        )
        & (f.col("Indivcoll") == "1")
        & (f.col("Branch") == "AS"),
        f.lit("1-ASSY"),
    )
    .when(
        (
            (f.col("Mekko_classif") == "1-ASSY")
            | (f.col("Mekko_classif") == "2-SUB_ASSY")
        )
        & (f.col("Indivcoll") == "2")
        & (f.col("Branch") == "AS"),
        f.lit("2-SUB_ASSY"),
    )
    .otherwise(f.col("Mekko_classif")),
)
# -------------------------------------------------------------------------------------
# Management of Internal or External details parts
df_material_end = df_material_end_join.withColumn(
    "Mekko_classif",
    f.when(
        (f.col("Mekko_classif") == "DETAIL PART")
        & (f.col("Supplier_Code").startswith("INT")),
        f.lit("3-INTERNAL_DP"),
    )
    .when(f.col("Mekko_classif") == "DETAIL PART", f.lit("4-PURCHASED_DP"))
    .when(f.col("Mekko_classif").isNull(), f.lit("9-OTHERS"))
    .otherwise(f.col("Mekko_classif")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Save Gold table

# COMMAND ----------

catalog = f"{WRITE_ENV}_gold"
domain = "master_data"
table = "material_master_data_plant"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{domain}")

table_utils.save_table(
    dest_table=f"{catalog}.{domain}.{table}",
    df=df_material_end,
    mode="overwrite",
    overwrite_schema=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Save Gold exposed table

# COMMAND ----------

catalog = "gold"
domain = "master_data"
table = "material_master_data_plant"
src_full_tablename = f"{WRITE_ENV}_{catalog}.{domain}.{table}"
dest_full_tablename = f"{src_full_tablename}_exposed"
Begin_By = "MARC"
Begin_By2 = "MBEW"
selected_columns = [
    col
    for col in df_material_end.columns
    if not col.startswith(Begin_By) and not col.startswith(Begin_By2)
]
select_statement = ", ".join(selected_columns)

table_utils.create_table_view(
    src_full_tablename, dest_full_tablename, select_statement, replace_view=True
)