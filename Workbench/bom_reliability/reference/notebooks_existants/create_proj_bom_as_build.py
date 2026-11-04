# Databricks notebook source
# MAGIC %md
# MAGIC # BOM As Build
# MAGIC
# MAGIC **Description:**  
# MAGIC Compute real BOM (as build) of A320 material numbers (ISM).
# MAGIC
# MAGIC **Output table:**  
# MAGIC   - {PIPELINE_WRITE_ENV}_proj.supply_chain.traceability_bom
# MAGIC   - {PIPELINE_WRITE_ENV}_proj.supply_chain.traceability_bom_temp
# MAGIC   - {PIPELINE_WRITE_ENV}_proj.supply_chain.bom_as_built
# MAGIC
# MAGIC **Input tables:**  
# MAGIC   - {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.lips_latest
# MAGIC   - {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.likp_latest
# MAGIC   - {REFERENCE_READ_ENV}_gold.supply_chain_logistic.part_movement_exposed
# MAGIC   - {REFERENCE_READ_ENV}_gold.supply_chain_logistic.stock_by_storage_location
# MAGIC   - {REFERENCE_READ_ENV}_gold.production.planned_orders_and_work_order_stack_sap
# MAGIC   - {REFERENCE_READ_ENV}_gold.supply_chain_ordering.purchase_orders_and_requests_sap_stacked
# MAGIC   - {REFERENCE_READ_ENV}_gold.master_data.material_plant_exposed
# MAGIC   - {REFERENCE_READ_ENV}_proj.manuf_engineering.mod_effectivity
# MAGIC   - {PIPELINE_READ_ENV}_proj.supply_chain.traceability_bom
# MAGIC   - {PIPELINE_WRITE_ENV}_proj.supply_chain.traceability_bom_temp

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### Config LEAP function import

# COMMAND ----------

from leap_utils.data_asset.table_utils import remove_leading_zeros, save_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Config Pipeline environments

# COMMAND ----------

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

compute_traceability = False

# COMMAND ----------

# DBTITLE 1,Inputs
# MAGIC %md
# MAGIC # Inputs

# COMMAND ----------

df_mvt = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.supply_chain_logistic.part_movement_exposed"
)

df_stk = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.supply_chain_logistic.stock_by_storage_location"
)

df_a320 = spark.read.table(
    f"{REFERENCE_READ_ENV}_proj.manuf_engineering.mod_effectivity"
)

df_delivery = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.lips_latest"
)

df_head_delivery = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.likp_latest"
)

df_po_interco_raw = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.supply_chain_ordering.purchase_orders_and_requests_sap_stacked"
)

df_wo_raw = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.production.planned_orders_and_work_order_stack_sap"
)

df_material = spark.read.table(
    f"{REFERENCE_READ_ENV}_gold.master_data.material_plant_exposed"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Utils

# COMMAND ----------

def process_sap_exception_flows(
    df: DataFrame, movement_code_pos: str, movement_code_neg: str
) -> DataFrame:
    """
    Processes SAP movement records to cancel positive material movement lines
    when corresponding negative movement (cancellation) has occurred.
    """
    df_code_pos = df.filter(f.col("movement_type") == movement_code_pos)
    window_spec = Window.partitionBy("material_number", "plant", "work_order").orderBy(
        f.desc("received_date"), f.desc("received_hour"),
        f.desc("material_doc"), f.desc("material_doc_item"),
    )
    df_f0 = df_code_pos.withColumn("Need_Rank", f.row_number().over(window_spec))
    f1 = Window.partitionBy("material_number", "plant", "work_order").orderBy("Need_Rank")
    df_conso_end = df_f0.withColumn("CumNeed", f.sum(f.col("quantity")).over(f1))

    df_code_neg = df.filter(f.col("movement_type") == movement_code_neg)
    df_code_neg = df_code_neg.groupBy("material_number", "plant", "work_order").agg(
        f.sum("quantity").alias("Cum_quantity")
    )

    df_joined = df_conso_end.join(df_code_neg, on=["material_number", "plant", "work_order"], how="left")
    df_joined = df_joined.fillna(0, subset=["Cum_quantity"])
    df_joined = df_joined.withColumn("NetCumEntries", f.col("CumNeed") - f.col("Cum_quantity"))
    df_joined = df_joined.withColumn(
        "test",
        f.when(f.col("NetCumEntries") <= 0, f.lit("X"))
        .when(f.col("NetCumEntries") >= f.col("quantity"), f.lit("O"))
        .otherwise("OK"),
    )
    df_joined = df_joined.filter(f.col("test") != "X")
    df_result = df_joined.withColumn(
        "quantity",
        f.when(f.col("test") == "O", f.col("quantity")).otherwise(
            f.col("quantity") - f.col("NetCumEntries")
        ),
    ).drop("test", "Need_Rank", "CumNeed", "Cum_quantity", "NetCumEntries")

    df_init = df.filter(
        (f.col("movement_type") != movement_code_pos)
        & (f.col("movement_type") != movement_code_neg)
    )
    return df_init.unionByName(df_result)


def propagate_traceability(df_updt: DataFrame, df_full: DataFrame) -> DataFrame:
    """
    Propagates KLX traceability downward through the WO tree.
    quantity_new = qty_entry_used / qty_init where qty_init = max(quantity) from entries per work_order.
    """
    df_traca_temp = (
        df_updt.filter(f.col("work_order_link").isNotNull())
        .select(
            f.col("work_order").try_cast("string").alias("work_order_origin_temp"),
            f.col("work_order_link").try_cast("string").alias("work_order"),
            f.col("KLX").try_cast("string").alias("KLX_GOD"),
            f.col("level").try_cast("int").alias("level"),
            f.col("path").alias("path_parent"),
        )
        .dropDuplicates(["work_order_origin_temp", "work_order", "KLX_GOD"])
        .withColumn("level", f.col("level") + 1)
    )

    df_conso = df_full.filter(f.col("DC_indicator") == "2-conso")
    df_traca_join = df_conso.join(df_traca_temp, ["work_order"], "inner")

    df_traca_update = df_traca_join.withColumn(
        "to_extend",
        f.when(
            ((f.col("KLX_GOD").isNotNull()) & (f.col("KLX").isNull()) & (f.col("work_order_link").isNotNull()))
            | ((f.col("material_number").endswith("M")) & (f.col("init").isNull()) & (f.col("type") == "PO")),
            f.lit("extend"),
        ).otherwise(f.lit(None)),
    )
    df_traca_update = (
        df_traca_update
        .withColumn("KLX", f.coalesce(f.col("KLX"), f.col("KLX_GOD")))
        .withColumn(
            "work_order_origin",
            f.coalesce(f.col("work_order_origin_temp"), f.col("work_order_origin")).try_cast("string"),
        )
        .withColumn("path", f.array_append(f.col("path_parent"), f.col("material_number")))
        .drop("KLX_GOD", "work_order_origin_temp", "path_parent")
    )

    df_entry = df_full.filter(f.col("DC_indicator") == "1-entries")
    df_order = df_entry.groupBy("work_order").agg(f.max("quantity").alias("qty_init"))

    df_traca_end = df_traca_update.join(df_order, ["work_order"], "left")
    df_traca_end = df_traca_end.withColumn(
        "quantity_new",
        f.when(f.col("KLX").isNotNull(), f.coalesce(f.col("qty_entry_used") / f.col("qty_init"), f.lit(1))),
    ).drop("qty_init")

    df_traca_end = df_traca_end.withColumn(
        "WO_Ref",
        f.when(
            f.col("work_order_origin").isNotNull(),
            f.concat_ws("_", f.col("work_order").try_cast("string"), f.col("work_order_origin").try_cast("string")),
        ).otherwise(f.lit(None)),
    )
    return df_traca_end


def resolve_interco(df_updt):
    """For interco rows (IS*M + PO + no work_order_link), resolve work_order_link
    to the WO at the sending plant whose real_end_date is closest before Posting_date."""
    _po = f.split(df_updt["key_doc"], "_")[3]

    # Join with all candidate WOs, keep only those finished before consumption
    df_joined = df_updt.join(
        df_interco_candidates,
        (_po == df_interco_candidates["_po_num"])
        & (df_updt["material_number"] == df_interco_candidates["_po_mat"])
        & df_updt["material_number"].endswith("M")
        & (df_updt["type"] == "PO")
        & df_updt["work_order_link"].isNull()
        & (df_interco_candidates["_wo_end_date"] <= df_updt["Posting_date"]),
        "left",
    )

    # Pick WO with most recent end date (closest before consumption)
    w = Window.partitionBy(
        df_joined["material_doc"], df_joined["material_doc_item"], df_joined["key_doc"]
    ).orderBy(f.desc("_wo_end_date"))
    df_joined = (
        df_joined
        .withColumn("_rn", f.row_number().over(w))
        .filter((f.col("_rn") == 1) | f.col("_entry_wo").isNull())
        .drop("_rn")
    )

    return (
        df_joined
        .withColumn("work_order_link", f.coalesce(f.col("work_order_link"), f.col("_entry_wo")))
        .drop("_po_num", "_po_mat", "_entry_wo", "_wo_end_date")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Tr 1. Theoretical traceability by FIFO rules

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr 1.1 Load and clean part movements

# COMMAND ----------

# Keep only relevant movement types
List_mvt_to_keep = [
    "101", "102", "551", "552", "261", "262",
    "643", "644", "601", "602", "221", "222",
]
df_mvt = df_mvt.filter(f.col("movement_type").isin(List_mvt_to_keep))

# COMMAND ----------

# Select columns and clean
df_mvt = df_mvt.select(
    "mat_doc_year",
    "material_doc",
    "material_doc_item",
    "plant",
    "material_number",
    "material_Description",
    "document_date",
    "Posting_date",
    "received_date",
    "received_hour",
    "Reference",
    "movement_type",
    "work_order",
    "purchase_order",
    "purchase_item",
    "quantity",
    "unit",
    "quantity_in_unit_buy",
    "unit_buy",
    "material_doc_reference",
    "material_doc_item_reference",
    "material_doc_origin",
    "material_doc_item_origin",
    "DC_indicator",
    "sales_order"
)

# Cast date columns
date_columns = ["document_date", "Posting_date", "received_date"]
for col in date_columns:
    df_mvt = df_mvt.withColumn(col, f.to_date(f.col(col), "yyyyMMdd"))

# Cast quantity columns
qty_columns = ["quantity", "quantity_in_unit_buy"]
for col in qty_columns:
    df_mvt = df_mvt.withColumn(col, f.col(col).try_cast("double"))

# Remove leading zeros
df_mvt_clean = remove_leading_zeros(
    df_mvt,
    [
        "work_order",
        "Reference",
        "purchase_order",
        "purchase_item",
        "material_doc",
        "material_doc_item",
        "material_doc_reference",
        "material_doc_item_reference",
        "material_doc_origin",
        "material_doc_item_origin",
        "material_number",
    ],
)

df_mvt_clean = df_mvt_clean.filter(f.col("document_date") > f.add_months(f.current_date(), -24))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr 1.2 Cancellation management
# MAGIC In this section, we handle the cancellation movements of item documents in the ERP.  
# MAGIC Specifically, movement 102 is the cancellation of movement 101, 262 of 261, 552 of 551, 644 of 643, and 602 of 601.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tr 1.2.1 Entry cancellations

# COMMAND ----------

# Keep only entries with material_doc different from material_doc_reference
# Standard process for cancellation of 101 movements
df_mvt_entries_a = df_mvt_clean.filter(
    (f.col("material_doc_reference") != f.col("material_doc"))
    & (f.col("material_doc_reference").isNotNull())
)
df_mvt_entries_a1 = df_mvt_entries_a.select(
    "mat_doc_year",
    "material_number",
    "plant",
    f.col("material_doc_reference").alias("material_doc"),
    f.col("material_doc_item_reference").alias("material_doc_item"),
    "movement_type",
)
df_mvt_entries_a2 = df_mvt_entries_a.select(
    "material_doc",
    "material_doc_item",
)

# Exclude cancelled entries
df_mvt_entries = df_mvt_clean.join(
    df_mvt_entries_a1,
    ["material_number", "plant", "material_doc", "material_doc_item"],
    "left_anti",
)
df_mvt_cleaning = df_mvt_entries.join(
    df_mvt_entries_a2,
    ["material_doc", "material_doc_item"],
    "left_anti",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tr 1.2.2 Consumption cancellations
# MAGIC **1st**: SAP standard functionality - use material_doc to link original and cancellation documents (stored in material_doc_origin).  
# MAGIC **2nd**: Some cancellations are made from scratch with no reference to the original doc. We use work order number and timestamp to handle them.

# COMMAND ----------

# SAP standard: link original and cancellation via material_doc_origin
df_mvt_conso_a = df_mvt_clean.filter(
    (f.col("material_doc_origin") != f.col("material_doc"))
    & (f.col("material_doc_origin").isNotNull())
)
df_mvt_conso_a1 = df_mvt_conso_a.select(
    "mat_doc_year",
    "material_number",
    "plant",
    f.col("material_doc_origin").alias("material_doc"),
    f.col("material_doc_item_origin").alias("material_doc_item"),
    "movement_type",
)
df_mvt_conso_a2 = df_mvt_conso_a.select(
    "material_doc",
    "material_doc_item",
)

# Exclude cancelled consumptions
df_mvt_conso = df_mvt_clean.join(
    df_mvt_conso_a1,
    ["material_number", "plant", "material_doc", "material_doc_item"],
    "left_anti",
)
df_mvt_clean_end = df_mvt_conso.join(
    df_mvt_conso_a2,
    ["material_doc", "material_doc_item"],
    "left_anti",
)

# COMMAND ----------

# Apply cancellation handling per movement pair
df_mvt_clean_end_flow1 = process_sap_exception_flows(df_mvt_clean_end, "261", "262")
df_mvt_clean_end_flow2 = process_sap_exception_flows(df_mvt_clean_end_flow1, "551", "552")
df_mvt_clean_end_flow3 = process_sap_exception_flows(df_mvt_clean_end_flow2, "643", "644")
df_mvt_clean_end_flow4 = process_sap_exception_flows(df_mvt_clean_end_flow3, "601", "602")
df_mvt_clean_end_flow5 = process_sap_exception_flows(df_mvt_clean_end_flow4, "221", "222")

# Exclude remaining cancellation movements
List_mvt_to_exclude = ["102", "552", "262", "644", "602", "221"]
df_mvt_clean_end = df_mvt_clean_end_flow5.filter(
    ~f.col("movement_type").isin(List_mvt_to_exclude)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr 1.3 Create entries DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tr 1.3.1 Standard entries

# COMMAND ----------

df_entries = (
    df_mvt_clean_end.filter(f.col("DC_indicator") == "S")
    .orderBy(f.desc("received_date"), f.desc("received_hour"))
    .withColumn("DC_indicator", f.lit("1-entries"))
    .withColumn("quantity", f.col("quantity").try_cast("double"))
)

# Customs coefficient for unit conversion
df_entries = df_entries.withColumn(
    "coef_customs",
    f.when(
        f.col("unit_buy") != f.col("unit"),
        f.col("quantity") / f.col("quantity_in_unit_buy"),
    ).otherwise(f.lit(1)),
)

# COMMAND ----------

# Create traceability keys for propagation on consumptions
df_entries = df_entries.withColumn(
    "key_doc",
    f.when(
        f.col("work_order").isNull(),
        f.concat_ws(
            "_",
            f.lit("PO"),
            "material_doc",
            "material_doc_item",
            "purchase_order",
            "purchase_item",
            "sales_order"
        ),
    ).otherwise(
        f.concat_ws(
            "_",
            f.lit("WO"),
            "material_doc",
            "material_doc_item",
            "work_order",
            "sales_order"
        )
    ),
)
df_entries_end = df_entries.select(
    "material_number",
    "plant",
    "DC_indicator",
    "received_date",
    "received_hour",
    "quantity",
    "key_doc",
    "movement_type",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr 1.4 Create consumptions DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tr 1.4.1 Standard consumptions

# COMMAND ----------

df_conso = (
    df_mvt_clean_end.filter(f.col("DC_indicator") == "H")
    .orderBy(f.desc("received_date"), f.desc("received_hour"))
    .withColumn("DC_indicator", f.lit("2-conso"))
    .withColumn("quantity", f.col("quantity").try_cast("double"))
    .drop(
        "material_doc_reference",
        "material_doc_item_reference",
        "material_doc_origin",
        "material_doc_item_origin",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tr 1.4.2 Add stock for traceability

# COMMAND ----------

# Add stock on hand for each material to integrate traceability on existing stock
df_stk = df_stk.withColumn(
    "quantity",
    f.col("Free_Stock")
    + f.col("Transfert_Stock")
    + f.col("Quality_Stock")
    + f.col("UnFree_Stock")
    + f.col("Block_Stock")
    + f.col("Return_Stock"),
)
df_stk = df_stk.groupBy("material_Number", "plant").agg(
    f.sum("quantity").alias("quantity")
)
df_stk = df_stk.withColumn("DC_indicator", f.lit("0-Stk")).withColumn(
    "received_date", f.lit("2999-01-01")
)

# Add stock values to consumption dataframe
df_conso_end = df_conso.unionByName(df_stk, allowMissingColumns=True)
df_conso_end = df_conso_end.withColumnRenamed("quantity", "quantity_conso")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr 1.5 Traceability assignment to consumptions
# MAGIC The goal is to assign each consumption the associated batches with their respective quantities used.

# COMMAND ----------

# Rank consumptions in chronological order
window_conso_order = Window.partitionBy("material_number", "plant").orderBy(
    f.desc("received_date"),
    f.desc("received_hour"),
    f.desc("material_doc"),
    f.desc("material_doc_item"),
)
df_conso_end_rk = df_conso_end.withColumn(
    "Conso_Rank", f.row_number().over(window_conso_order)
)

# Rank entries in FIFO order
window_entry_order = Window.partitionBy("material_number", "plant").orderBy(
    f.desc("received_date"),
    f.desc("received_hour"),
)
df_entries_end_rk = df_entries_end.withColumn(
    "Entry_Rank", f.row_number().over(window_entry_order)
)

# COMMAND ----------

if compute_traceability:
    # Step 1: Validity range for each consumption
    window_consumption = Window.partitionBy("material_number", "plant").orderBy(
        "Conso_Rank"
    )
    df_consumption_with_ranges = (
        df_conso_end_rk.withColumn(
            "start_idx",
            f.sum("quantity_conso").over(
                window_consumption.rowsBetween(Window.unboundedPreceding, -1)
            ),
        )
        .withColumn("start_idx", f.coalesce(f.col("start_idx"), f.lit(0)))
        .withColumn("end_idx", f.col("start_idx") + f.col("quantity_conso") - 1)
    )

    # Step 2: Validity range for each entry
    window_entry = Window.partitionBy("material_number", "plant").orderBy("Entry_Rank")
    df_stock_entry_with_ranges = (
        df_entries_end_rk.withColumn(
            "start_idx",
            f.sum("quantity").over(window_entry.rowsBetween(Window.unboundedPreceding, -1)),
        )
        .withColumn("start_idx", f.coalesce(f.col("start_idx"), f.lit(0)))
        .withColumn("end_idx", f.col("start_idx") + f.col("quantity") - 1)
    )

    # Step 3: Prepare for cross join
    df_consumption_expanded = df_consumption_with_ranges
    df_stock_entry_expanded = df_stock_entry_with_ranges.select(
        f.col("material_number").alias("entry_material"),
        f.col("plant").alias("entry_plant"),
        "key_doc",
        "Entry_Rank",
        "quantity",
        f.col("start_idx").alias("entry_start_idx"),
        f.col("end_idx").alias("entry_end_idx"),
    )

    # Step 4: Cross join to determine stock used per consumption
    df_final_result = (
        df_consumption_expanded.crossJoin(df_stock_entry_expanded)
        .filter(
            (f.col("material_number") == f.col("entry_material"))
            & (f.col("plant") == f.col("entry_plant"))
        )
        .withColumn(
            "qty_entry_used",
            f.greatest(
                f.lit(0),
                f.least(f.col("end_idx"), f.col("entry_end_idx"))
                - f.greatest(f.col("start_idx"), f.col("entry_start_idx"))
                + 1,
            ),
        )
        .filter(f.col("qty_entry_used") > 0)
        .drop(
            "entry_material",
            "entry_plant",
            "start_idx",
            "end_idx",
            "entry_start_idx",
            "entry_end_idx",
        )
    )

    # Add entries to consumptions
    df_traceability = df_final_result.unionByName(df_entries, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr 1.6 Save traceability temp table

# COMMAND ----------

# Save temporary project table for traceability
if compute_traceability:
    catalog = f"{PIPELINE_WRITE_ENV}_proj"
    domain = "supply_chain"
    table = "traceability_bom"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{domain}")

    save_table(
        dest_table=f"{catalog}.{domain}.{table}",
        df=df_traceability,
        mode="overwrite",
        overwrite_schema=True,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Tr 2. Initialize A320 data
# MAGIC This data initializes the material numbers whose BOM must be computed. We only want those that have a delivery note, and only past deliveries (future BOM are not known in advance).

# COMMAND ----------

# Read A320 data (ISM, MSN, sales order)
df_a320_prepared = df_a320.select(
    f.col("material_number"),
    f.col("plant"),
    f.col("sales_order"),
    f.col("sales_order_item"),
).distinct()
df_a320_prepared = df_a320_prepared.withColumn("init", f.lit("init"))

# Read delivery notes to link with sales order
df_delivery_prepared = df_delivery.select(
    f.col("vgbel").alias("sales_order"),
    f.col("vgpos").alias("sales_order_item"),
    f.col("vbeln").alias("reference"),
).distinct()
df_delivery_prepared = remove_leading_zeros(
    df_delivery_prepared, ["sales_order", "sales_order_item", "reference"]
)

# Get delivery date
df_head_delivery_prepared = df_head_delivery.select(
    f.col("vbeln").alias("reference"),
    f.col("lfdat").alias("delivery_date"),
).distinct()
df_head_delivery_prepared = remove_leading_zeros(
    df_head_delivery_prepared, ["reference"]
)

# Build A320 data with delivery notes
df_a320_end = df_a320_prepared.join(
    df_delivery_prepared, on=["sales_order", "sales_order_item"], how="inner"
).join(df_head_delivery_prepared, on=["reference"], how="inner")

df_a320_end = (
    df_a320_end.withColumn(
        "delivery_date", f.try_to_date(f.col("delivery_date"), "yyyyMMdd")
    ).withColumnRenamed(
        "sales_order", "sales_order_root"
    ).withColumnRenamed(
        "sales_order_item", "sales_order_item_root"
    ).filter(
        (f.col("delivery_date") < f.current_date())
        & (f.col("delivery_date") > f.add_months(f.current_date(), -12))
    )
    .distinct()
)
df_a320_end.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Tr 3. Downstream traceability

# COMMAND ----------

df_traceability = (
    spark.read.table(f"{PIPELINE_READ_ENV}_proj.supply_chain.traceability_bom")
    .select(
        "mat_doc_year",
        "material_doc",
        "material_doc_item",
        "sales_order",
        "plant",
        "material_number",
        "material_Description",
        "document_date",
        "Posting_date",
        "received_date",
        "received_hour",
        "Reference",
        "movement_type",
        "DC_indicator",
        "work_order",
        "purchase_order",
        "purchase_item",
        "quantity",
        "unit",
        "key_doc",
        "qty_entry_used"
    )
    .distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr 3.1 Filter deliveries and create KLX keys

# COMMAND ----------

# Filter: only last 1.5 years deliveries, exclude plants 1900/2010
df_traceability_filtered = df_traceability.withColumn(
    "to_cancel",
    f.when(
        ((f.col("movement_type") == "601") | (f.col("movement_type") == "643"))
        & (f.col("document_date") < f.add_months(f.current_date(), -18)),
        f.lit("X"),
    ).when(
        ((f.col("movement_type") == "601") | (f.col("movement_type") == "643"))
        & (f.col("plant").isin(["1900", "2010"])),
        f.lit("X"),
    ),
).filter(f.col("posting_date") > f.add_months(f.current_date(), -24))

df_traceability_filtered = df_traceability_filtered.filter(
    f.col("to_cancel").isNull()
).drop("to_cancel")

# Generate KLX key for delivery notes
df_traceability_delivery = df_traceability_filtered.withColumn(
    "KLX",
    f.when(
        (f.col("movement_type") == "601") | (f.col("movement_type") == "643"),
        f.concat_ws(
            "_",
            "Reference",
            "material_number",
            "mat_doc_year",
            "material_doc",
            "material_doc_item",
        ),
    ),
).withColumn(
    "Type",
    f.when(f.col("DC_indicator") == "1-entries", f.lit("Entries"))
    .when(f.col("key_doc").startswith("WO"), f.lit("WO"))
    .when(f.col("KLX").isNotNull(), f.lit("DL"))
    .otherwise(f.lit("PO")),
)

# Extract WO from key_doc
split_col = f.split(f.col("key_doc"), "_")
df_traceability_delivery_orders = df_traceability_delivery.withColumn(
    "work_order_link",
    f.when(
        (f.col("key_doc").startswith("WO"))
        & (f.col("DC_indicator") != "1-entries")
        & (f.size(split_col) > 2),
        split_col[3],
    ).otherwise(f.lit(None)),
)

# Initialize lines where downward traceability needs extension
df_traceability_end = df_traceability_delivery_orders.join(
    df_a320_end.select("reference", "material_number", "plant", "init", "delivery_date", "sales_order_root", "sales_order_item_root"),
    on=["reference", "material_number","plant"],
    how="left",
)
df_traceability_end = df_traceability_end.withColumn(
    "to_extend",
    f.when(f.col("init").isNotNull(), f.lit("extend")).otherwise(f.lit(None)),
).withColumn("work_order_origin", f.lit("1"))

# COMMAND ----------

# Save temp table for performance
catalog = f"{PIPELINE_WRITE_ENV}_proj"
domain = "supply_chain"
table = "traceability_bom_temp"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{domain}")

save_table(
    dest_table=f"{catalog}.{domain}.{table}",
    df=df_traceability_end,
    mode="overwrite",
    overwrite_schema=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr 3.2 Process propagation loop

# COMMAND ----------

# Launch downward traceability
df_ref = spark.read.table(
        f"{PIPELINE_READ_ENV}_proj.supply_chain.traceability_bom_temp"
    ).withColumn("level", f.lit(0)).withColumn("path", f.array(f.col("material_number")))
df_full = df_ref.withColumn("to_extend", f.lit(None)).drop("level")
df_final = df_ref.filter(f.col("init") == "init")
df_updt = df_ref.filter(f.col("to_extend") == "extend").withColumn("quantity_new", f.lit(None).cast("double"))

# Interco lookups (persist for perf)
# Step 1: PO -> plant_origin from PO/PR table
df_po_interco = (
    df_po_interco_raw
    .filter(f.col("plant_origin").isNotNull())
    .select(
        f.col("document_number").alias("_po_num"),
        f.col("material_number").alias("_po_mat"),
        f.col("plant_origin").alias("_plant_origin"),
    )
    .dropDuplicates(["_po_num", "_po_mat"])
).persist()

# Step 2: All WOs with their dates (reused later for enrichment)
df_wo_dates = (
    df_wo_raw
    .filter(f.col("document_type") == "Work Orders")
    .select(
        f.col("document_number").alias("_wo_num"),
        f.col("real_start_date").alias("_wo_start_date"),
        f.col("real_end_date").alias("_wo_end_date"),
    )
    .distinct()
)
df_wo_entries = (
    df_full.filter(
        (f.col("DC_indicator") == "1-entries") & (f.col("key_doc").startswith("WO"))
    )
    .withColumn("_entry_wo", f.split(f.col("key_doc"), "_")[3])
    .select(
        f.col("material_number").alias("_wo_mat"),
        f.col("plant").alias("_wo_plant"),
        "_entry_wo",
    )
    .distinct()
)
# Join WO entries with their end dates
df_wo_at_plant = (
    df_wo_entries
    .join(df_wo_dates, df_wo_entries["_entry_wo"] == df_wo_dates["_wo_num"], "left")
    .drop("_wo_num")
).persist()

# Step 3: Combine PO -> plant_origin -> all candidate WOs (with dates)
df_interco_candidates = (
    df_po_interco
    .join(
        df_wo_at_plant,
        (df_po_interco["_po_mat"] == df_wo_at_plant["_wo_mat"])
        & (df_po_interco["_plant_origin"] == df_wo_at_plant["_wo_plant"]),
        "inner",
    )
    .select("_po_num", "_po_mat", "_entry_wo", "_wo_end_date")
).persist()
print(f"Interco candidates size: {df_interco_candidates.count()}")

# COMMAND ----------

# Anti-cycle: track visited (child_WO, KLX) pairs
df_visited = None

max_attempts = 20
for i in range(max_attempts):
    cnt = df_updt.count()
    if cnt == 0:
        print(f"break at {i}")
        break

    # Resolve interco + drop unresolved
    df_updt = resolve_interco(df_updt).filter(f.col("work_order_link").isNotNull())
    cnt_after = df_updt.count()
    if cnt_after == 0:
        print("No rows with work_order_link after interco, stopping.")
        break

    # Track child WOs about to be explored
    new_visited = (
        df_updt
        .select(
            f.col("work_order_link").alias("visited_wo"),
            f.col("KLX").alias("visited_klx"),
        )
        .distinct()
    )
    df_visited = (
        new_visited
        if df_visited is None
        else df_visited.unionByName(new_visited).distinct()
    )

    # Propagate traceability
    df_tracebility_temp = propagate_traceability(df_updt, df_full)

    # Get next extends
    df_updt = df_tracebility_temp.filter(
        (f.col("to_extend") == "extend")
        & (f.col("init").isNull())
        & (f.col("quantity_new") > 0)
    )

    # Anti-cycle: exclude already visited
    df_updt = df_updt.join(
        df_visited,
        (df_updt["work_order_link"].eqNullSafe(df_visited["visited_wo"]))
        & (df_updt["KLX"].eqNullSafe(df_visited["visited_klx"])),
        "left_anti",
    )

    # Accumulate results
    df_final = df_tracebility_temp.unionByName(
        df_final, allowMissingColumns=True
    ).dropDuplicates()
    print(f"Iteration {i}")

# Post-loop: enrich interco IS*M rows with resolved work_order_link
df_final = resolve_interco(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr 3.3 Enrich data

# COMMAND ----------

# Cleaning Table - Keep only entries with downward traceability found
df_final_filtered = (
    df_final.filter((f.col("DC_indicator") == "2-conso") & (f.col("KLX").isNotNull()))
)

# Extraction of information from key_doc
split_col = f.split(f.col("key_doc"), "_")

df_final_clean = (
    df_final_filtered.withColumnRenamed("material_number", "component")
    .withColumn("reference", f.split(f.col("KLX"), "_")[0])
    .withColumn("material_number", f.split(f.col("KLX"), "_")[1])
    .withColumn(
        "purchase_order",
        f.when(
            (f.col("key_doc").startswith("PO")) & (f.size(split_col) > 2), split_col[3]
        ).otherwise(f.lit(None)),
    )
    .withColumn(
        "purchase_item",
        f.when(
            (f.col("key_doc").startswith("PO")) & (f.size(split_col) > 3), split_col[4]
        ).otherwise(f.lit(None)),
    ).withColumn(
        "interco",
        f.when(
            (f.col("component").endswith("M")) & (f.col("type") == "PO"), True
        ).otherwise(f.lit(False)),
    )
)

# COMMAND ----------

# ISM profit center lookup (before df_material reassignment)
df_profit_center = (
    df_material
    .filter(f.col("plant") == f.col("MRP_unit"))
    .select("material_number", f.col("plant").alias("plant_ism"), "profit_center")
    .distinct()
)

# Component material enrichment
df_material = df_material.select(
    f.col("material_number").alias("component"),
    f.col("plant"),
    "external_lead_time",
    "internal_lead_time",
    f.coalesce(f.col("standard_price_eur_budget"),f.lit(0)).alias("unit_price")
).filter(f.col("plant") == f.col("MRP_unit")).drop("MRP_unit").distinct()

df_real_bom = df_final_clean.join(df_material,on=["component","plant"], how="left").withColumn(
    "quantity", f.round(f.col("quantity_new"), 4)
).withColumn(
    "lead_time",
    f.when(f.col("type") == "PO", f.col("external_lead_time")).otherwise(f.col("internal_lead_time"))
).drop("parent")

# COMMAND ----------

# ------------------------------------------------------------------
# Enrichment: WO dates, parent material, plant correction
#   work_order      = WO that consumed this component (= parent's production WO)
#   work_order_link = WO that produced this component (used for propagation)
# ------------------------------------------------------------------

# 1. Component's own WO: dates + plant
df_wo_comp = (
    df_wo_raw
    .filter(f.col("document_type") == "Work Orders")
    .select(
        f.col("document_number").alias("work_order_link"),
        f.col("plant").alias("_plant_wo_comp"),
        f.col("real_start_date").alias("real_start_date"),
        f.col("real_end_date").alias("real_end_date"),
    )
    .dropDuplicates(["work_order_link"])
)
df_real_bom = df_real_bom.join(df_wo_comp, "work_order_link", "left")

# 2. Parent's WO: material produced + start date
df_wo_parent = (
    df_wo_raw
    .filter(f.col("document_type") == "Work Orders")
    .select(
        f.col("document_number").alias("_wo_parent"),
        f.col("material_number").alias("parent"),
        f.col("real_start_date").alias("parent_real_start_date"),
    )
    .dropDuplicates(["_wo_parent"])
)
df_real_bom = df_real_bom.join(
    df_wo_parent,
    df_real_bom["work_order"] == df_wo_parent["_wo_parent"],
    "left",
).drop("_wo_parent")

# 3. Interco: use component WO's plant instead of consumption plant
df_real_bom = df_real_bom.withColumn(
    "plant",
    f.when(f.col("interco"), f.coalesce("_plant_wo_comp", "plant")).otherwise(f.col("plant"))
).drop("_plant_wo_comp")

# 4. Rename for clarity
df_real_bom = (
    df_real_bom
    .withColumnRenamed("work_order", "work_order_parent")
    .withColumnRenamed("work_order_link", "work_order_component")
)

# 5. ISM plant + profit center via (material_number, sales_order_root, sales_order_item_root)
df_ism_plant_lookup = (
    df_a320
    .select(
        "material_number",
        f.col("plant").alias("plant_ism"),
        f.col("sales_order").alias("sales_order_root"),
        f.col("sales_order_item").alias("sales_order_item_root"),
    )
    .distinct()
)
df_real_bom = df_real_bom.join(
    df_ism_plant_lookup,
    ["material_number", "sales_order_root", "sales_order_item_root"],
    "left",
)
df_real_bom = df_real_bom.join(
    df_profit_center,
    ["material_number", "plant_ism"],
    "left",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr 3.4 Save proj table

# COMMAND ----------

catalog = f"{PIPELINE_WRITE_ENV}_proj"
domain = "supply_chain"
table = "bom_as_built"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{domain}")

save_table(
    dest_table=f"{catalog}.{domain}.{table}",
    df=df_real_bom,
    mode="overwrite",
    overwrite_schema=True,
)