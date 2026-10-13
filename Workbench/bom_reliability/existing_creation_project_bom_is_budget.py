# Databricks notebook source
# MAGIC %md
# MAGIC # Purpose
# MAGIC
# MAGIC **Description:**  
# MAGIC This notebook aims to build a database to support a Power BI tool for Budget Purchasing management.  
# MAGIC It consolidates data from various SAP and internal sources, processes parts movement, and outputs a table for reporting and analysis.  
# MAGIC Final dataframe gives for one Sales Order, the as build production with all work order and conumption done  
# MAGIC   
# MAGIC
# MAGIC **Output table(s):**  
# MAGIC   - `{PIPELINE_WRITE_ENV}_proj.bom_budget.bom_budget_is`   
# MAGIC
# MAGIC **Input:**  
# MAGIC   - `{REFERENCE_READ_ENV}_gold.supply_chain_logistic.part_movement`  
# MAGIC   - `{REFERENCE_READ_ENV}_gold.mrp.sap_mrp_reservation_latest`  
# MAGIC   - `{REFERENCE_READ_ENV}_gold.production.sap_work_order_basics_latest_exposed`  
# MAGIC   - `{REFERENCE_READ_ENV}_gold.supply_chain_logistic.stock_by_storage_location`  
# MAGIC   - `{REFERENCE_READ_ENV}_gold.finance.profit_center_exposed`  
# MAGIC   - `{REFERENCE_READ_ENV}_gold.master_data.material_master_data_plant_exposed`  
# MAGIC   - `{REFERENCE_READ_ENV}_proj.bom_budget.msn_std_mapping` 
# MAGIC   - `{REFERENCE_READ_ENV}_gold.supply_chain_procurement.purchase_orders_exposed`  
# MAGIC   - `{REFERENCE_READ_ENV}_gold.finance.currency_exchange`  

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from pyspark.sql import Window
from leap_utils.data_asset import table_utils

dbutils.widgets.removeAll()
# Get read environment variable
dbutils.widgets.text("REFERENCE_READ_ENV", "prod")
REFERENCE_READ_ENV = dbutils.widgets.get("REFERENCE_READ_ENV")
# Get pipeline environment variable
dbutils.widgets.text("PIPELINE_READ_ENV", "dev")
PIPELINE_READ_ENV = dbutils.widgets.get("PIPELINE_READ_ENV")
# Get write environment variable
dbutils.widgets.text("PIPELINE_WRITE_ENV", "dev")
PIPELINE_WRITE_ENV = dbutils.widgets.get("PIPELINE_WRITE_ENV")

# COMMAND ----------

# MAGIC %md 
# MAGIC #1 - Creation Dataframe with parts movement clean  
# MAGIC The goal of this section is to obtain a dataframe of stock movements (entry and consumption) cleared of any cancellation movements.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 - Upload gold parts movement from SAP

# COMMAND ----------

# Upload Gold table for parts Movements
df_mvt = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.supply_chain_logistic.part_movement"
)
# -----------------------------------------------------------------------
# According Business rules , we keep only Entries (101,102), Consumptions (261,262),
# Interco Deliveries (643,644), Intraco Deliveries (643,644), Customers Deliveries (601,602)
List_mvt_to_keep = [
    "101",
    "102",
    "261",
    "262",
    "641",
    "642",
    "643",
    "644",
    "601",
    "602",
]
df_mvt = df_mvt.filter(f.col("Movement_type").isin(List_mvt_to_keep))
# #-----------------------------------------------------------------------
# # As is a specific case for IS plant we keep only Plant start with 2
df_mvt = df_mvt.filter(f.col("Plant").startswith("2"))
# -----------------------------------------------------------------------

# COMMAND ----------

# Select Data + Cleaning Base
df_mvt = df_mvt.select(
    "Mat_doc_year",
    "Material_doc",
    "Material_doc_item",
    "Plant",
    "Material_number",
    "Material_Description",
    "Document_date",
    "Posting_date",
    "Received_date",
    "Received_hour",
    "Reference",
    "Movement_type",
    "Work_order",
    "Purchase_order",
    "Purchase_item",
    "Quantity",
    "Unit",
    "Quantity_in_unit_buy",
    "Unit_buy",
    "Material_doc_reference",
    "Material_doc_item_reference",
    "Material_doc_origin",
    "Material_doc_item_origin",
    "DC_indicator",
    "Sales_Order",
    f.col("MSEG_rsnum").alias("Reservation_Number"),
    f.col("MSEG_rspos").alias("Reservation_Item_Number"),
)
# -----------------------------------------------------------------------
# Cast date columns to format yyyy-mm-dd
date_columns = ["Document_date", "Posting_date", "Received_date"]
for col in date_columns:
    df_mvt = df_mvt.withColumn(col, f.to_date(f.col(col), "yyyyMMdd"))
# -----------------------------------------------------------------------
# Cast Quantity columns to format double
qty_columns = ["Quantity", "Quantity_in_unit_buy"]
for col in qty_columns:
    df_mvt = df_mvt.withColumn(col, f.col(col).cast("double"))
# -----------------------------------------------------------------------
# Remove leading zeros
df_mvt_clean = table_utils.remove_leading_zeros(
    df_mvt,
    [
        "Work_order",
        "Reference",
        "Purchase_order",
        "Purchase_item",
        "Material_doc",
        "Material_doc_item",
        "Material_doc_reference",
        "Material_doc_item_reference",
        "Material_doc_origin",
        "Material_doc_item_origin",
        "Material_number",
        "Sales_Order",
        "Reservation_Number",
        "Reservation_Item_Number",
    ],
)
# -----------------------------------------------------------------------


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 - Management of documents cancellation  
# MAGIC In this section, we handle the cancellation movements of item documents in the ERP.  
# MAGIC Specifically, movement 102 is the cancellation of movement 101, 262 of 261

# COMMAND ----------

# MAGIC %md
# MAGIC ###1.2.1 - Management of Entries Cancelled

# COMMAND ----------

# ----------------------------------------------------------------
# Keep Only entries with Material Doc Different from Material Doc reference
# Standard process for cancellation 101 movement.
df_mvt_entries_a = df_mvt_clean.filter(
    (f.col("Material_doc_reference") != f.col("Material_doc"))
    & (f.col("Material_doc_reference").isNotNull())
)
df_mvt_entries_a1 = df_mvt_entries_a.select(
    "Mat_doc_year",
    "Material_number",
    "Plant",
    f.col("Material_doc_reference").alias("Material_doc"),
    f.col("Material_doc_item_reference").alias("Material_doc_item"),
    "Movement_type",
)
df_mvt_entries_a2 = df_mvt_entries_a.select(
    "Material_doc",
    "Material_doc_item",
)
# ----------------------------------------------------------------
# Exclusion Entries Cancelled 2 process
# Exclusion by new Document Article
df_mvt_entries = df_mvt_clean.join(
    df_mvt_entries_a1,
    ["Material_number", "Plant", "Material_doc", "Material_doc_item"],
    "left_anti",
)
# Exclusion by cancellation old Document Article
df_mvt_cleaning = df_mvt_entries.join(
    df_mvt_entries_a2,
    ["Material_doc", "Material_doc_item"],
    "left_anti",
)
# ----------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2.2 - Management of consumptions cancelled
# MAGIC **1st** : SAP standard functionnality- Used Material Doc in order to make link between original Material Doc and Cancellation material Doc  
# MAGIC This information is stored in view 'Material_doc_origin'  
# MAGIC **2nd** : Some cancellation movement could be made from scratch with no reference with original doc.  
# MAGIC We'll use Work order number and timestamp of consumption in order to treat them.  

# COMMAND ----------

# -----------------------------------------------------------------------
# 1st : SAP Standard Flows:
# Keep Only entries with Material Doc Different from Material Doc reference
df_mvt_conso_a = df_mvt_clean.filter(
    (f.col("Material_doc_origin") != f.col("Material_doc"))
    & (f.col("Material_doc_origin").isNotNull())
)
df_mvt_conso_a1 = df_mvt_conso_a.select(
    "Mat_doc_year",
    "Material_number",
    "Plant",
    f.col("Material_doc_origin").alias("Material_doc"),
    f.col("Material_doc_item_origin").alias("Material_doc_item"),
    "Movement_type",
)
df_mvt_conso_a2 = df_mvt_conso_a.select(
    "Material_doc",
    "Material_doc_item",
)
# -----------------------------------------------------------------------
# Exclusion consumption Cancelled 2 process
# Exclusion by new Document Article
df_mvt_conso = df_mvt_clean.join(
    df_mvt_conso_a1,
    ["Material_number", "Plant", "Material_doc", "Material_doc_item"],
    "left_anti",
)
# Exclusion by cancellation old Document Article
df_mvt_clean_end = df_mvt_conso.join(
    df_mvt_conso_a2,
    ["Material_doc", "Material_doc_item"],
    "left_anti",
)

# COMMAND ----------

# -----------------------------------------------------------------------
# 2nd : Scratch process
# -----------------------------------------------------------------------
def process_sap_exception_flows(
    df: DataFrame, movement_code_pos: str, movement_code_neg: str
) -> DataFrame:
    """
    Processes SAP movement records to cancel positive material movement lines
    when corresponding negative movement (cancellation) has occurred.

    This function:
    1. Filters for positive movement lines and ranks them by received date/hour/doc.
    2. Computes a cumulative quantity per material/work order/plant for positive movements.
    3. Aggregates negative movement quantities (e.g., cancellations) by material/work order/plant.
    4. Joins both datasets to calculate net material entries.
    5. Adjusts or removes positive lines where the cancellation fully offsets the movement.
    6. Appends unprocessed movement types back to the filtered result.

    Parameters:
        df (DataFrame): Input DataFrame containing movement records with at least the following columns:
            - "Material_number", "Plant", "Work_order", "Movement_type",
              "Received_date", "Received_hour", "Material_doc", "Material_doc_item",
              "Quantity"
        movement_code_pos (str): SAP movement type code for positive/creation movements.
        movement_code_neg (str): SAP movement type code for negative/cancellation movements.

    Returns:
        DataFrame: Filtered and adjusted DataFrame with net movement entries.
    """
    # -------------------------------------------------------------------
    # Filter on movement code positive - creation
    df_code_pos = df.filter(f.col("Movement_type") == movement_code_pos)

    # Ranking of mvt positive by Work order and date
    window_spec = Window.partitionBy("Material_number", "Plant", "Work_order").orderBy(
        f.desc("Received_date"),
        f.desc("Received_hour"),
        f.desc("Material_doc"),
        f.desc("Material_doc_item"),
    )
    df_f0 = df_code_pos.withColumn("Need_Rank", f.row_number().over(window_spec))
    f1 = Window.partitionBy("Material_number", "Plant", "Work_order").orderBy(
        "Need_Rank"
    )
    df_conso_end = df_f0.withColumn("CumNeed", f.sum(f.col("Quantity")).over(f1))

    # -------------------------------------------------------------------
    # Extraction Of Movement negative - cancelation
    df_code_neg = df.filter(f.col("Movement_type") == movement_code_neg)

    # Calcul Cumul Conso by Work_Order
    df_code_neg = df_code_neg.groupBy("material_number", "plant", "work_order").agg(
        f.sum("quantity").alias("Cum_Quantity")
    )
    # -------------------------------------------------------------------
    # Join lines with lines on id columns
    df_joined = df_conso_end.join(
        df_code_neg,
        on=["material_number", "plant", "work_order"],
        how="left",
    )
    df_joined = df_joined.fillna(0, subset=["Cum_Quantity"])
    # -------------------------------------------------------------------
    # Suppress lines 'positive' where movement 'negative' have been done
    df_joined = df_joined.withColumn(
        "NetCumEntries", f.col("CumNeed") - f.col("Cum_Quantity")
    )
    df_joined = df_joined.withColumn(
        "test",
        f.when(f.col("NetCumEntries") <= 0, f.lit("X"))
        .when(f.col("NetCumEntries") >= f.col("Quantity"), f.lit("O"))
        .otherwise(f.col("Quantity") - f.col("NetCumEntries")),
    )
    df_joined = df_joined.filter(f.col("test") != "X")
    df_result = df_joined.withColumn(
        "Quantity",
        f.when(f.col("test") == "O", f.col("Quantity")).otherwise(f.col("test")),
    ).drop("test", "Need_Rank", "CumNeed", "Cum_Quantity", "NetCumEntries")
    # -------------------------------------------------------------------
    df_init = df.filter(
        (f.col("Movement_type") != movement_code_pos)
        & (f.col("Movement_type") != movement_code_neg)
    )
    df_end = df_init.unionByName(df_result)
    return df_end


# ------------------------------------------------------------------------
# Wo Consumptions
df_mvt_clean_end = process_sap_exception_flows(df_mvt_clean_end, "261", "262")
# Interco deliveries
df_mvt_clean_end = process_sap_exception_flows(df_mvt_clean_end, "641", "642")
# Interco deliveries
df_mvt_clean_end = process_sap_exception_flows(df_mvt_clean_end, "643", "644")
# Customer deliveries
df_mvt_clean_end = process_sap_exception_flows(df_mvt_clean_end, "601", "602")

# ----------------------------------------------------------------
# Cleaning Cancellation mvt not treated,
# due to starting horizon in 2020 (aquisition in SAP)
List_mvt_to_excluded = ["102", "262", "641", "642", "643", "644", "601", "602"]
df_mvt_std = df_mvt_clean_end.filter(~f.col("Movement_type").isin(List_mvt_to_excluded))
# ----------------------------------------------------------------
# Drop Material document
df_mvt_std = df_mvt_std.drop("Material_doc", "Material_doc_item")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 - Management of multi consumption on same reservation
# MAGIC We have to aggregate multi consumption on a same reservation Number

# COMMAND ----------

# Filter and aggregate based on the most recent received_date and sum the quantity for movement code 261
window_spec = Window.partitionBy(
    "Reservation_Number", "Reservation_Item_Number"
).orderBy(f.desc("Received_date"))

df_mvt_std = df_mvt_std.withColumn(
    "Quantity",
    f.when(f.col("Movement_type") == "101", f.col("Quantity")).otherwise(f.lit(0)),
).dropDuplicates()

df_mvt_261 = df_mvt_std.filter(f.col("Movement_type") == 261)
df_mvt_261 = df_mvt_261.withColumn("row_num", f.row_number().over(window_spec))
df_mvt_261 = df_mvt_261.filter(f.col("row_num") == 1).drop("row_num")

df_mvt_261 = df_mvt_261.groupBy("Reservation_Number", "Reservation_Item_Number").agg(
    f.first("Received_date").alias("Received_date"),
    f.sum("Quantity").alias("Quantity"),
    *[
        f.first(col).alias(col)
        for col in df_mvt_261.columns
        if col
        not in [
            "Received_date",
            "Quantity",
            "Reservation_Number",
            "Reservation_Item_Number",
        ]
    ]
)
# Append the other movements
df_mvt_other = df_mvt_std.filter(f.col("Movement_type") != 261)
df_mvt_std_reservation = df_mvt_261.unionByName(df_mvt_other)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 - Management of over consumption
# MAGIC According Business rules, we have to exclude overconsumption (item include between 2 to 5)

# COMMAND ----------

# Upload Gold table for reservation info
df_resb = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.mrp.sap_mrp_reservation_latest"
).select(
    "Reservation_Number",
    "Reservation_Item_Number",
    "Quantity_Needed",
    "Quantity_Consumed",
    f.col("RESB_postp").alias("Item_type"),
    "Material_Number_Origin",

)
# Remove leading zeros
df_resb_clean = table_utils.remove_leading_zeros(
    df_resb,
    [
        "Reservation_Number",
        "Reservation_Item_Number",
    ],
)
# Join Mouvement with Reservation information
df_join = df_mvt_std_reservation.join(
    df_resb_clean, ["Reservation_Number", "Reservation_Item_Number"], "left"
)
# We keep only Reservation without item 2 to 5
List_Exclusion_item = ["2", "3", "4", "5"]
df_mvt_clean_item = df_join.filter(
    (~f.col("Item_type").isin(List_Exclusion_item)) | (f.col("DC_indicator") == "S")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##1.5 - Save temporary table parts movement

# COMMAND ----------

# For better performance, we save this table in a temporary table
# It will be cancel at the end of this notebook
catalog = f"{PIPELINE_WRITE_ENV}_proj"
domain = "bom_budget"
table = "bom_budget_mvt_clean_tmp"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{domain}")

table_utils.save_table(
    dest_table=f"{catalog}.{domain}.{table}",
    df=df_mvt_clean_item,
    mode="overwrite",
    overwrite_schema=True,
)

# COMMAND ----------

# df_mvt_clean_item = spark.read.table(
#     f"{PIPELINE_READ_ENV}_proj.bom_budget.bom_budget_mvt_clean_tmp"
# )
# df_mvt_clean_item.filter(f.col("material_number")=="IS0002405S").display()


# COMMAND ----------

# MAGIC %md
# MAGIC # 2 - Spreading Sales Information on Consumptions movements
# MAGIC The goal of this section is to spread information on Sales Order to the components of work order

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.1 - Management Consumption affected to a Sales Orders
# MAGIC

# COMMAND ----------

# Upload Part movement temporary table
df_mvt_clean_item = spark.read.table(
    f"{PIPELINE_READ_ENV}_proj.bom_budget.bom_budget_mvt_clean_tmp"
)
# ----------------------------------------------------------------
# Creation temporary dataframe with link Work order/Sales Orders
df_so_temp = df_mvt_clean_item.select(
    "Work_order", f.col("Sales_Order").alias("key_so")
).dropDuplicates()

df_so_temp = df_so_temp.filter(
    f.col("key_so").isNotNull() & f.col("Work_order").isNotNull()
)
# ----------------------------------------------------------------
df_mvt_clean_end_new = df_mvt_clean_item.join(df_so_temp, ["Work_order"], "left")
df_mvt_clean_end_new = df_mvt_clean_end_new.withColumn(
    "key_so",
    f.when(f.col("key_so").isNull(), f.col("Sales_Order")).otherwise(f.col("key_so")),
)
# ----------------------------------------------------------------
# Add Sales Order from Header of work order
# Upload Gold table for reservation info
df_work = (
    spark.read.table(
        f"{REFERENCE_READ_ENV}_gold.production.sap_work_order_basics_latest_exposed"
    )
    .select(
        f.col("Customers_Orders").alias("Sales_Order_temp"),
        f.col("WO_Number").alias("Work_order"),
    )
    .dropDuplicates()
)
df_mvt_clean_end_new = df_mvt_clean_end_new.join(df_work, ["Work_order"], "left")
df_mvt_clean_end_new = df_mvt_clean_end_new.withColumn(
    "key_so",
    f.when(f.col("key_so").isNull(), f.col("Sales_Order_temp")).otherwise(
        f.col("key_so")
    ),
).drop("Sales_Order_temp")

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.2 - Management Consumption not affected to a Sales Orders  
# MAGIC The goal is to create a traceability for each component and affect this one to consumptions

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.2.1 - Creation DataFrame for Entries  
# MAGIC Standard entries DataFrame SAP

# COMMAND ----------

df_entries = (
    df_mvt_clean_end_new.filter(f.col("DC_indicator") == "S")
    .orderBy(f.desc("Received_date"), f.desc("Received_hour"))
    .withColumn("DC_indicator", f.lit("1-entries"))
    .withColumn("Quantity", f.col("Quantity").cast("double"))
    .withColumn(
        "PO_item_ID", f.concat_ws("-", f.col("Purchase_order"), f.col("Purchase_item"))
    )
)
# Exclusion PO interco and intra-co
df_purchase_orders = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.supply_chain_ordering.purchase_orders_sap_exposed"
).select("intercompany", "PO_Item_ID")
df_entries_interco_out = (
    df_entries.join(df_purchase_orders, ["PO_Item_ID"], "left")
    .filter((f.col("intercompany").isNull()) | (f.col("intercompany") == "External"))
    .drop("intercompany", "PO_Item_ID")
)

# ---------------------------------------------------------------
# Here we create our own traceability keys so that we can propagate them on consumptions
df_entries = df_entries_interco_out.withColumn(
    "Key_Doc",
    f.when(
        f.col("Work_Order").isNull(),
        f.concat_ws(
            "_",
            f.lit("PO"),
            "Mat_doc_year",
            "Purchase_order",
            "Purchase_item",
            "Quantity",
        ),
    ).otherwise(
        f.concat_ws(
            "_",
            f.lit("WO"),
            "Mat_doc_year",
            "Work_order",
            "Quantity",
        )
    ),
)
df_entries_end = df_entries.select(
    "Material_number",
    "Plant",
    "DC_indicator",
    "Received_date",
    "Received_hour",
    "Quantity",
    "Key_Doc",
    "Movement_type",
    "key_so",
)
# --------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.2.2 - Creation DateFrame for Consumptions  
# MAGIC Standard consumption DataFrame SAP

# COMMAND ----------

df_conso_temp = (
    df_mvt_clean_end_new.filter(f.col("DC_indicator") == "H")
    .withColumn("DC_indicator", f.lit("2-conso"))
    .withColumn("Quantity_Needed", f.col("Quantity_Needed").cast("float"))
    .withColumn("Quantity_Consumed", f.col("Quantity_Consumed").cast("float"))
    .drop(
        "Material_doc_reference",
        "Material_doc_item_reference",
        "Material_doc_origin",
        "Material_doc_item_origin",
        "Quantity",
    )
)

# COMMAND ----------

# As we don"t have all reception we initialize the dataframe with a batch of 100000 for each material number
df_new_rows = df_conso_temp.select("Material_number", "Plant").distinct().withColumn(
    "Quantity", f.lit(100000.0)
).withColumn(
    "Key_Doc", f.lit("batch init")
).withColumn(
    "Received_date", f.lit("1900-01-01")
).withColumn(
    "Received_hour", f.lit("151515")
).withColumn(
    "DC_indicator", f.lit("1-entries")
).withColumn(
    "Movement_type", f.lit(None)
).withColumn(
    "key_so", f.lit(None)
)
df_entries_end = df_entries_end.unionByName(df_new_rows, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2.3 - Add Stock value for traceability

# COMMAND ----------

# We need to add stock on hand for each material number
# in order to integrate traceability on stock before consumption
df_stk = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.supply_chain_logistic.stock_by_storage_location"
).filter(f.col("Plant").startswith("2"))

df_stk = df_stk.withColumn(
    "Quantity",
    f.col("Free_Stock")
    + f.col("Transfert_Stock")
    + f.col("Quality_Stock")
    + f.col("UnFree_Stock")
    + f.col("Block_Stock")
    + f.col("Return_Stock"),
)
df_stk = df_stk.groupBy("Material_Number", "Plant").agg(
    f.sum("Quantity").alias("Quantity_Consumed")
)

df_stk = (
    df_stk.withColumn("DC_indicator", f.lit("0-Stk"))
    .withColumn("Received_date", f.lit("2999-01-01"))
    .withColumn("Quantity_Consumed", f.col("Quantity_Consumed").cast("float"))
)

# ---------------------------------------------------------------------------------
# Add Stock values to consumption dataframe
df_conso_end = df_conso_temp.unionByName(df_stk, allowMissingColumns=True)
# ---------------------------------------------------------------------------------

# COMMAND ----------

# Determination of plant buying/produce the parts
df_material = (
    spark.read.table(f"{REFERENCE_READ_ENV}_gold.master_data.material_plant_exposed")
    .select(
        f.col("material_number").alias("Material_number"),
        f.col("plant").alias("Plant"),
        "plant_buy",
    )
    .dropDuplicates()
)

df_conso_end = df_conso_end.join(df_material, ["Material_number", "Plant"], "left")
df_conso_end = df_conso_end.withColumn(
    "plant_buy", f.coalesce(f.col("plant_buy"), f.col("plant"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2.4 - Traceability Assignment to Consumptions  
# MAGIC The goal is to assign each consumption the associated batches with their respective quantities used

# COMMAND ----------

# ---------------------------------------------------------------------------------
# Calculate cumulative quantities of consumptions and entries
# Add a rank for each consumption in chronological order
window_conso_order = Window.partitionBy("Material_number", "Plant").orderBy(
    f.desc("Received_date"),
    f.desc("Received_hour"),
)
df_conso_end_rk = df_conso_end.withColumn(
    "Conso_Rank", f.row_number().over(window_conso_order)
)
# ---------------------------------------------------------------------------------
# Add a rank for each entry in FIFO order
window_entry_order = Window.partitionBy("Material_number", "Plant").orderBy(
    f.desc("Received_date"),
    f.desc("Received_hour"),
)
df_entries_end_rk = df_entries_end.withColumn(
    "Entry_Rank", f.row_number().over(window_entry_order)
)
# ---------------------------------------------------------------------------------
# Step 1: Create the validity range for each consumption
window_consumption = Window.partitionBy("Material_number", "Plant").orderBy(
    "Conso_Rank"
)
df_consumption_with_ranges = (
    df_conso_end_rk.withColumn(
        "start_idx",
        f.sum("Quantity_Consumed").over(
            window_consumption.rowsBetween(Window.unboundedPreceding, -1)
        ),
    )
    .withColumn("start_idx", f.coalesce(f.col("start_idx"), f.lit(0)))
    .withColumn("end_idx", f.col("start_idx") + f.col("Quantity_Consumed") - 1)
)
# ---------------------------------------------------------------------------------
# Step 2: Create the validity range for each entry
window_entry = Window.partitionBy("Material_number", "Plant").orderBy("Entry_Rank")
df_stock_entry_with_ranges = (
    df_entries_end_rk.withColumn(
        "start_idx",
        f.sum("Quantity").over(window_entry.rowsBetween(Window.unboundedPreceding, -1)),
    )
    .withColumn("start_idx", f.coalesce(f.col("start_idx"), f.lit(0)))
    .withColumn("end_idx", f.col("start_idx") + f.col("Quantity") - 1)
)
# ---------------------------------------------------------------------------------
# Step 3: Select the values to keep for the cross join
df_consumption_expanded = df_consumption_with_ranges
df_stock_entry_expanded = df_stock_entry_with_ranges.select(
    f.col("Material_number").alias("entry_material"),
    f.col("Plant").alias("entry_plant"),
    "Key_Doc",
    "Entry_Rank",
    "Quantity",
    f.col("start_idx").alias("entry_start_idx"),
    f.col("end_idx").alias("entry_end_idx"),
)
# ---------------------------------------------------------------------------------
# Step 4: Create all possible combinations between consumptions and stock entries
# The calculation of Qty_entry_used aims to determine the amount of stock used
# for each combination of consumption and stock entry.
df_final_result = (
    df_consumption_expanded.crossJoin(df_stock_entry_expanded)
    .filter(
        (f.col("Material_number") == f.col("entry_material"))
        & (f.col("plant_buy") == f.col("entry_plant"))
    )
    .withColumn(
        "Qty_entry_used",
        f.greatest(
            f.lit(0),
            f.least(f.col("end_idx"), f.col("entry_end_idx"))
            - f.greatest(f.col("start_idx"), f.col("entry_start_idx"))
            + 1,
        ),
    )
    .filter(f.col("Qty_entry_used") > 0)
    .drop(
         "entry_material",
         "entry_plant",
         "start_idx",
         "end_idx",
         "entry_start_idx",
         "entry_end_idx",
     )
)
# ---------------------------------------------------------------------------------

# COMMAND ----------

# Step 1 : Identifiy consumption with no key_so
df_matched_conso = df_final_result.select(
    "Material_number", "Plant", "Conso_Rank"
).distinct()
df_unmatched_conso = df_consumption_expanded.join(
    df_matched_conso, on=["Material_number", "Plant", "Conso_Rank"], how="left_anti"
)
# Step 2 : Do new join without Plant information
df_stock_entry_expanded_material_only = df_stock_entry_with_ranges.select(
    f.col("Material_number").alias("entry_material"),
    "Key_Doc",
    "Entry_Rank",
    "Quantity",
    f.col("start_idx").alias("entry_start_idx"),
    f.col("end_idx").alias("entry_end_idx"),
    f.col("Plant").alias("entry_plant"),
)
# --------------------------------------------------------------------------------
df_fallback_result = (
    df_unmatched_conso.crossJoin(df_stock_entry_expanded_material_only)
    .filter(f.col("Material_number") == f.col("entry_material"))
    .withColumn(
        "Qty_entry_used",
        f.greatest(
            f.lit(0),
            f.least(f.col("end_idx"), f.col("entry_end_idx"))
            - f.greatest(f.col("start_idx"), f.col("entry_start_idx"))
            + 1,
        ),
    )
    .filter(f.col("Qty_entry_used") > 0)
    .drop(
        "entry_material",
        "entry_start_idx",
        "entry_end_idx",
        "start_idx",
        "end_idx",
    )
)
df_fallback_result = df_fallback_result.withColumn(
    "KEY",
    f.concat_ws(
        "_",
        "Material_number",
        "Plant",
        "Work_order",
        "Reservation_Number",
        "Reservation_Item_Number",
    ),
)
window_entry_plant = Window.partitionBy("KEY").orderBy("entry_plant")
df_fallback_result = (
    df_fallback_result.withColumn("rank", f.rank().over(window_entry_plant))
    .filter(f.col("rank") == 1)
    .drop("rank", "start_idx", "end_idx", "entry_plant", "KEY")
)
# --------------------------------------------------------------------------------
df_final_result_with_fallback = df_final_result.unionByName(df_fallback_result)
# --------------------------------------------------------------------------------
# Add entries to consumptions
df_traceability = df_final_result_with_fallback.unionByName(
    df_entries, allowMissingColumns=True
)
# --------------------------------------------------------------------------------
# Select Columns for the final table
df_traceability = df_traceability.select(
    "Mat_doc_year",
    "Plant",
    "Material_number",
    "Material_Description",
    "Document_date",
    "Posting_date",
    "Received_date",
    "Received_hour",
    "Reference",
    "Movement_type",
    "DC_indicator",
    "Quantity_Consumed",
    "Quantity_Needed",
    "Work_order",
    "Purchase_order",
    "Purchase_item",
    "Quantity",
    "Qty_entry_used",
    "Unit",
    "Sales_Order",
    "Key_Doc",
    "key_so",
    "Item_type",
    "Material_Number_Origin",
)
# --------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2.3 - Spread traceability to consumptions

# COMMAND ----------

# ---------------------------------------------------------------------------------
# Extraction Of WO from Key_Doc
split_col = f.split(f.col("Key_Doc"), "_")
df_traceability_delivery_orders = df_traceability.withColumn(
    "Work_order_link",
    f.when(
        (f.col("Key_Doc").startswith("WO"))
        & (f.size(split_col) > 2),
        split_col[2],
    ).otherwise(f.lit(None)),
)
# ---------------------------------------------------------------------------------
# Generation of downward traceability key according key_so
df_traceability_delivery = df_traceability_delivery_orders.withColumn(
    "KLX",
    f.when(
        f.col("key_so").isNotNull(),
        f.concat_ws(
            "_",
            "key_so",
            "Material_number",
        ),
    ).otherwise(f.lit(None)),
).withColumn(
    "Type",
    f.when(f.col("DC_indicator") == "1-entries", f.lit("Entries"))
    .when(f.col("Key_Doc").startswith("WO"), f.lit("WO"))
    .when(f.col("Key_Doc").startswith("PO"), f.lit("PO"))
    .otherwise(f.lit("DL")),
)
# ---------------------------------------------------------------------------------
# Initialisation of line where downward traceability is waiting for extension
df_traceability_end = (
    df_traceability_delivery.withColumn(
        "To_extend",
        f.when(
            (
                f.col("Sales_Order").isNull()
                & (f.col("key_so").isNotNull())
                & (f.col("Type") == "WO")
            ),
            f.lit("extend"),
        ).otherwise(f.lit(None)),
    )
    .withColumn("Init", f.lit("Init"))
    .withColumn("Work_order_origin", f.lit(1))
).withColumn("unique_id", f.monotonically_increasing_id())
# ---------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 - Save temporary table traceability 

# COMMAND ----------

# For performance reasons, we temporarily save the table for Traceability
# It will be cancel at the end of this notebook
catalog = f"{PIPELINE_WRITE_ENV}_proj"
domain = "bom_budget"
table = "bom_budget_traceability_tmp"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{domain}")

table_utils.save_table(
    dest_table=f"{catalog}.{domain}.{table}",
    df=df_traceability_end,
    mode="overwrite",
    overwrite_schema=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 - Process spread Traceability  

# COMMAND ----------

# This function allows the propagation of traceability from a delivery note during an export
# and identifies all the components used and their associated traceability
# ---------------------------------------------------------------------------------
def propagate_traceability(df_updt: DataFrame, df_init: DataFrame) -> DataFrame:
    """
    Propagates traceability information across work orders by extending KLX traceability
    from parent to child work orders based on identified links.

    This function is typically used in a manufacturing or supply chain context where
    traceability of components and their origin (e.g., via KLX codes) needs to be extended
    from one work order to the work orders it feeds.

    Steps:
    1. Detects "extend" flags in the update DataFrame.
    2. Builds a mapping between parent and child work orders.
    3. Applies KLX traceability to downstream consumption records lacking KLX.
    4. Computes proportional quantities if traceability is propagated.
    5. Generates a reference ID `WO_Ref` combining child and origin work orders.
    6. Merges updated data with initial dataset, ensuring deduplication.

    Parameters:

        df_updt (DataFrame): The DataFrame with updated information and candidate
            traceability extensions. Must contain columns:
            - "Work_order", "Work_order_link", "To_extend", "KLX", "DC_indicator",
              "Qty_entry_used", "Work_order_origin"
        df_init (DataFrame): The original DataFrame to merge with at the end.

    Returns:
        DataFrame: Updated DataFrame with propagated traceability and consistent trace links.
    """
    if df_updt.filter((f.col("To_extend") == "extend")).count() > 0:
        # Determine the lines where traceability needs to be propagated
        # Need to build strong link between work order who calls another work order
        # in order to keep all lines after dropduplicates (Work_order_origin proscess)
        df_traca_temp = (
            (df_updt.filter((f.col("To_extend") == "extend")))
            .select(
                f.col("Work_order").alias("Work_order_origin_temp"),
                f.col("Work_order_link").alias("Work_order"),
                f.col("KLX").alias("KLX_GOD"),
            )
            .dropDuplicates()
        )
        # ---------------------------------------------------------------------------------------
        # Propagation of traceability on consumptions
        df_conso = df_updt.filter(f.col("DC_indicator") == "2-conso")
        df_traca_join = df_conso.join(df_traca_temp, ["Work_Order"], "left")
        # ---------------------------------------------------------------------------------------
        # Update Traceability to extend
        df_traca_update = df_traca_join.withColumn(
            "To_extend",
            f.when(
                (f.col("KLX_GOD").isNotNull())
                & (f.col("KLX").isNull())
                & (f.col("work_order_link").isNotNull()),
                f.lit("extend"),
            ).otherwise(f.lit(None)),
        )
        df_traca_update = (
            df_traca_update.withColumn(
                "KLX", f.coalesce(f.col("KLX"), f.col("KLX_GOD"))
            )
            .withColumn(
                "Work_order_origin",
                f.coalesce(f.col("Work_order_origin_temp"), f.col("Work_order_origin")),
            )
            .drop("KLX_GOD", "Work_order_origin_temp")
        )
        # ---------------------------------------------------------------------------------------
        # Filter and select unique work orders and their quantities
        df_entry = df_updt.filter(f.col("DC_indicator") == "1-entries")
        df_order = df_entry.groupBy("Work_Order").agg(
            f.max("Quantity").alias("Qty_init")
        )
        # -------------------------------------------------------------------------------------
        # Adjust consumed quantities based on the quantity required for product delivery
        df_traca_end = df_traca_update.join(df_order, ["Work_order"], "left")
        df_traca_end = df_traca_end.withColumn(
            "Quantity_New",
            f.when(
                f.col("KLX").isNotNull(),
                f.coalesce(f.col("Qty_entry_used") / f.col("Qty_init"), f.lit(1)),
            ).otherwise(f.lit(None)),
        ).drop("Qty_init")
        
        df_traca_end = df_traca_end.withColumn(
            "WO_Ref",
            f.when(
                f.col("Work_order_origin").isNotNull(),
                f.concat_ws("_", f.col("Work_order"), f.col("Work_order_origin")),
            ).otherwise(f.lit(None)),
        )
        df_traca_final = df_traca_end.unionByName(
            df_init, allowMissingColumns=True
        ).dropDuplicates()
    else:
        df_traca_final = df_updt

    return df_traca_final

# COMMAND ----------

# Launch the process of downward traceability with a maximum of 20 attempts
max_attempts = 20
for _ in range(max_attempts):
    # Read the latest temporary traceability table
    df_final = spark.read.table(f"{PIPELINE_READ_ENV}_proj.bom_budget.bom_budget_traceability_tmp")
    # -------------------------------------------------------------------------------------
    # Management of base initialisation
    df_init = df_final.filter(f.col("Init") == "Init").withColumn(
        "To_extend", f.lit(None)
    )
    if df_final.filter(f.col("Init") != "Init").count() == 0:
        df_updt = df_final.withColumn("Init", f.lit(None))
    else:
        df_updt = df_final.filter(f.col("Init") != "Init")
    # -------------------------------------------------------------------------------------
    # If there are no rows to extend, break the loop
    if df_final.filter((f.col("To_extend") == "extend")).count() == 0:
        break
    else:
        # Process the dataframe to propagate traceability
        df_final = propagate_traceability(df_updt, df_init)
        print(f"Iteration {_}: {df_final.count()} rows")

        # Save the updated dataframe back to the temporary traceability table
        catalog = f"{PIPELINE_WRITE_ENV}_proj"
        domain = "bom_budget"
        table = "bom_budget_traceability_tmp"

        spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{domain}")

        table_utils.save_table(
            dest_table=f"{catalog}.{domain}.{table}",
            df=df_final,
            mode="overwrite",
            overwrite_schema=True,
        )

# COMMAND ----------

# MAGIC %md
# MAGIC #4 - Add Specific views for business

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 - Add Program and MSN

# COMMAND ----------

df_final = spark.read.table(
    f"{PIPELINE_READ_ENV}_proj.bom_budget.bom_budget_traceability_tmp"
)
# Erase Init base
df_final = df_final.filter(f.col("Init").isNull())
# Update Key SO
split_col = f.split(f.col("KLX"), "_")
df_final = df_final.withColumn("key_so", split_col[0])
df_final = df_final.withColumn("key_so", f.coalesce(f.col("key_so")))
# --------------------------------------------------------------------
# Cleaning Table - Keep only entries with sales order information found
df_filtered = df_final.filter(
    (f.col("DC_indicator") == "2-conso") & (f.col("KLX").isNotNull())
)
# --------------------------------------------------------------------
# Extract Program and MSN from KLX for PBI report display
df_prog_msn = df_filtered.withColumn("program", f.substring(f.col("KLX"), 3, 3))
df_prog_msn = df_prog_msn.withColumn("msn", f.substring(f.col("KLX"), 6, 5))
# --------------------------------------------------------------------
df_prog_msn = df_prog_msn.drop(
    "Quantity_conso",
    "Quantity",
    "Qty_entry_used",
    "Work_order_link",
    "To_extend",
    "Init",
    "Work_order_origin",
    "WO_Ref",
).withColumnRenamed("Quantity_New", "Quantity")
# --------------------------------------------------------------------
# Remove leading zeros
df_prog_msn = table_utils.remove_leading_zeros(
    df_prog_msn,
    ["msn"],
)
# --------------------------------------------------------------------
# Add Profit center description
df_pc = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.finance.profit_center_exposed"
).select(
    f.col("Profit_Center_short").alias("program"),
    f.col("Short_Description").alias("program_description"),
)
df_prog_msn = df_prog_msn.join(df_pc, ["program"], "left")
df_prog_msn = df_prog_msn.withColumn(
    "prog_desc", f.concat_ws(" - ", f.col("program"), f.col("program_description"))
)
# --------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 - Add Material Master data

# COMMAND ----------

# Upload Gold table for material master data
df_material = (
    spark.read.table(
        f"{REFERENCE_READ_ENV}_gold.master_data.material_master_data_plant_exposed"
    )
    .select(
        "Material_Number",
        "Plant",
        "Procurement_Type",
        "Material_Type",
        "Standard_Price_EUR_Budget",
        "Average_Price_EUR_Budget",
        "Supplier_Name",
    )
    .dropDuplicates()
)

df_prog_msn_mat = df_prog_msn.join(df_material, ["Material_Number", "Plant"], "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ##4.3 - Mapping with Standard Configuration
# MAGIC Manuel Mapping as this information is not stored in SAP.

# COMMAND ----------

# Add Mapping MSN // STD
df_msn = spark.read.table(f"{REFERENCE_READ_ENV}_proj.bom_budget.msn_std_mapping")
df_bom_mapping = df_prog_msn_mat.join(df_msn, ["program", "msn"], "left")
df_bom_mapping = df_bom_mapping.withColumn(
    "Standard", f.coalesce(f.col("Standard"), f.lit("STD-X"))
)
df_bom_mapping = df_bom_mapping.withColumn(
    "MSN_Standard", f.concat_ws(" - ", f.col("msn"), f.col("Standard"))
)
# --------------------------------------------------------------------
# We keep only dataframe where standard is found
df_bom_mapping = df_bom_mapping.filter(f.col("Standard") != "STD-X")

# COMMAND ----------

df_bom_mapping.filter(f.col("Work_order")=="6536231").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 - Add Average price on reception of the current year

# COMMAND ----------

# We keep only Po received in the current year
df_entries_po = df_entries_interco_out.filter(
    f.year(f.col("Received_date")) == f.year(f.current_date())
)
# --------------------------------------------------------------------------------------
# Upload Gold table for purchase order data
df_po = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.supply_chain_procurement.purchase_orders_exposed"
).select(
    "Material_Number",
    f.col("Plant_delivered").alias("Plant"),
    f.col("PO_Number").alias("Purchase_order"),
    f.col("PO_Item_Number").alias("Purchase_item"),
    "Quantity_Delivered",
    "Global_price",
    "Currency_PO",
    "Intercompany_Flow",
    "Unit_of_Purchase",
    "Unit_Of_MRP",
)
# --------------------------------------------------------------------------------------
# Management of conversion Quantity
df_po = df_po.withColumn(
    "Quantity_Delivered",
    f.col("Quantity_Delivered") / f.col("Unit_of_Purchase") * f.col("Unit_Of_MRP"),
).drop("Unit_of_Purchase", "Unit_Of_MRP")
# --------------------------------------------------------------------------------------
# Convert in EUR Currency
# Upload SAP Currency
df_currency = spark.read.table(f"{REFERENCE_READ_ENV}_gold.finance.currency_exchange")
# Take the max date from Gold Currency
latest_currency_date = df_currency.agg(
    f.max("Validity_Date_DateFormat").alias("Latest currency date")
).collect()[0][0]
df_po = df_po.withColumn("Document_conversion_date", f.lit(latest_currency_date))

df_po_conv = table_utils.currency_conversion(
    df=df_po,
    document_currency_column=[f.col("Currency_PO")],
    rate_types=["Spot"],
    rate_dates=[f.col("Document_conversion_date")],
    target_currency_value="EUR",
    document_amounts=[f.col("Global_price")],
)
df_po_conv = df_po_conv.withColumnRenamed(
    "Global_price_from_Currency_PO_to_EUR_in_Spot_rate_at_Document_conversion_date",
    "Document_value",
).drop(
    "Currency_PO_to_EUR_in_Spot_rate_at_Document_conversion_date",
    "Document_conversion_date",
)
# --------------------------------------------------------------------------------------
# Management of Intercompany Flows
df_ico = df_po_conv.withColumn("Intercompany_Flow", f.trim(f.col("Intercompany_Flow")))
df_ico = df_ico.withColumnRenamed("Intercompany_Flow", "Partner")
List_no_interco = ["", "X000", "000500", "S400", "S200", "001200"]
df_ico = df_ico.withColumn(
    "Intercompany_flow",
    f.when(
        (f.col("Partner").isin(List_no_interco)) | (f.col("Partner").isNull() == True),
        f.lit("No"),
    ).otherwise(f.lit("Yes")),
).drop(f.col("Partner"))
# --------------------------------------------------------------------------------------
df_ico = df_ico.filter(
    (f.col("Intercompany_flow") == "No") & (f.col("Quantity_Delivered") > 0)
).drop("Intercompany_flow")
# --------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------
# Remove leading zeros
df_po = table_utils.remove_leading_zeros(df_ico, ["Purchase_order", "Purchase_item"])
#--------------------------------------------------------------------------------------
df_entries_valo = df_entries_po.join(
    df_po, ["Material_Number", "Plant", "Purchase_order", "Purchase_item"], "left"
)
df_entries_valo_end = df_entries_valo.groupBy("Material_Number", "Plant").agg(
    f.sum("Quantity_delivered").alias("Quantity_Po"),
    f.sum("Global_price").alias("Global_price"),
)
df_entries_valo_end = df_entries_valo_end.withColumn(
    "Unit_Price_current_year", f.col("Global_price") / f.col("Quantity_Po")
).drop("Quantity_Po", "Global_price","Currency_PO")
#--------------------------------------------------------------------------------------
df_final_end = df_bom_mapping.join(
    df_entries_valo_end, ["Material_Number", "Plant"], "left"
).drop("unique_id")

# COMMAND ----------

df_final_end.filter(f.col("Work_order")=="6536231").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #5 - Cancel all Temporary Table

# COMMAND ----------

spark.sql(
    f"DROP TABLE IF EXISTS {PIPELINE_WRITE_ENV}_proj.bom_budget.bom_budget_mvt_clean_tmp"
)
spark.sql(
    f"DROP TABLE IF EXISTS {PIPELINE_WRITE_ENV}_proj.bom_budget.bom_budget_traceability_tmp"
)


# COMMAND ----------

# MAGIC %md
# MAGIC #6 - Save Proj Table for BOM Budget IS

# COMMAND ----------

# Saving End Traceability table
catalog = f"{PIPELINE_WRITE_ENV}_proj"
domain = "bom_budget"
table = "bom_budget_is"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{domain}")

table_utils.save_table(
    dest_table=f"{catalog}.{domain}.{table}",
    df=df_final_end,
    mode="overwrite",
    overwrite_schema=True,
)
