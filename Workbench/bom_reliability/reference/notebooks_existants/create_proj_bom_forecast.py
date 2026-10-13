# Databricks notebook source
# MAGIC %md
# MAGIC # BOM Forecast Analysis
# MAGIC
# MAGIC **Description:**  
# MAGIC Compare future component demand based on 3 BOM scenarios:
# MAGIC - **Average BOM** — unique average BOM per ISM
# MAGIC - **Last STDCODE** — real BOM of the last past delivery with the same STDCODE
# MAGIC - **Last VERSIONCODE** — real BOM of the last past delivery with the same VERSIONCODE
# MAGIC - **Bonus : Past Data** — last year real consumptions no matter the standard or version
# MAGIC
# MAGIC Steps :
# MAGIC 1. get as-built data per ISM/plant to use it as a reference for forecasting future deliveries
# MAGIC 2. Build reference datasets : last built per version, last built per standard, real past consumption per month
# MAGIC 3. Get quantities + master data : lead time, unit price, number of deliveries mainly
# MAGIC 4. Aggregate and compare
# MAGIC
# MAGIC **Output tables:**  
# MAGIC - {PIPELINE_WRITE_ENV}_proj.supply_chain.bom_forecast_demand_detail
# MAGIC - {PIPELINE_WRITE_ENV}_proj.supply_chain.bom_forecast_demand
# MAGIC
# MAGIC **Input tables:**  
# MAGIC - {REFERENCE_READ_ENV}_proj.manuf_engineering.mod_effectivity
# MAGIC - {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.lips_latest
# MAGIC - {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.likp_latest
# MAGIC - {PIPELINE_READ_ENV}_proj.supply_chain.current_average_bom
# MAGIC - {PIPELINE_READ_ENV}_proj.supply_chain.traceability_bom

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

from leap_utils.data_asset.table_utils import save_table, remove_leading_zeros

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
dbutils.widgets.text("pipeline_read_env", "dev")
PIPELINE_READ_ENV = dbutils.widgets.get("pipeline_read_env")
# Get write environment variable
dbutils.widgets.text("pipeline_write_env", "dev")
PIPELINE_WRITE_ENV = dbutils.widgets.get("pipeline_write_env")

# COMMAND ----------

# MAGIC %md
# MAGIC # Inputs

# COMMAND ----------

# 1. MOD effectivity (material_number, sales_order, STDCODE, VERSIONCODE)
df_a320 = spark.read.table(
    f"{REFERENCE_READ_ENV}_proj.manuf_engineering.mod_effectivity"
)

# 2. Delivery notes
df_delivery = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.lips_latest"
)
df_head_delivery = spark.read.table(
    f"{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.likp_latest"
)

# 3. Average BOM (includes profit_center at material_number level)
df_avg_bom = spark.read.table(
    f"{PIPELINE_READ_ENV}_proj.supply_chain.current_average_bom"
)

# 4. Real BOM (as-built)
df_real_bom = spark.read.table(
    f"{PIPELINE_READ_ENV}_proj.supply_chain.bom_as_built"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Tr 1. Future & Past Sales Orders
# MAGIC
# MAGIC Get data from MOD Follow_up (df_a320) and split it between past sales orders (reference for version, standard and past) vs future sales order (forecast).
# MAGIC
# MAGIC We only have last year + next 2 years available but **we keep : -1/+1 year.** *(indeed data > 1 year in the future : unreliable (version unknown, standard might change, deliveries number might evolve)*

# COMMAND ----------

# Backtest reference date
BACKTEST_DATE = f.to_date(f.lit("2026-01-01"))

# 1. Get MOD follow-up data
df_a320_prepared = df_a320.select(
    "material_number",
    "plant",
    "sales_order",
    "sales_order_item",
    "STDCODE",
    "VERSIONCODE",
    "CACCCODE",
    "DESIGNATION",
    "material_description",
    f.to_date("delivery_date").alias("delivery_date"),
).distinct()

# 2. "Future" = Jan 1 to today (deliveries we'd forecast from Jan 1 perspective, now known)
df_future = df_a320_prepared.filter(
    (f.col("delivery_date") >= BACKTEST_DATE)
    & (f.col("delivery_date") < f.current_date())
).withColumn("delivery_month", f.date_trunc("month", "delivery_date"))

# 3. Enrich past data with sales order information
df_delivery_prepared = df_delivery.select(
    f.col("vgbel").alias("sales_order"),
    f.col("vgpos").alias("sales_order_item"),
    f.col("vbeln").alias("reference"),
).distinct()
df_delivery_prepared = remove_leading_zeros(
    df_delivery_prepared, ["sales_order", "sales_order_item", "reference"]
)

# 4. Past = deliveries before Jan 1 (reference known on backtest date)
df_past = (
    df_a320_prepared.filter(f.col("delivery_date") < BACKTEST_DATE)
    .join(df_delivery_prepared, on=["sales_order", "sales_order_item"], how="inner")
    .distinct()
)

# 5. Actual deliveries Jan 1 → today (with LIPS reference for real BOM lookup)
df_actual = (
    df_a320_prepared.filter(
        (f.col("delivery_date") >= BACKTEST_DATE)
        & (f.col("delivery_date") < f.current_date())
    )
    .join(df_delivery_prepared, on=["sales_order", "sales_order_item"], how="inner")
    .distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Tr 2. Handle version/standard level
# MAGIC
# MAGIC *Reminder : we want to get the last past data per material_number/plant/version & material_number/plant/standard to use it as a reference for forecasting future deliveries.*
# MAGIC
# MAGIC Versions and standards change, and there are many cases where there is no match between past and future.
# MAGIC For example, material xxx was only built with standard ST5 in the past, and all future material xxx will have standard ST9. There is no match, hence nothing to compare.
# MAGIC
# MAGIC We need to handle 3 cases for each material_number :
# MAGIC - all versions or standards match
# MAGIC - no version or standard match
# MAGIC - some match, some don't : we fill missing values with average value (warning : this is different from matching a version where a component would be missing. In that case, we would set 0 as the version was found but the component is not used).

# COMMAND ----------

# 1. Real BOM components per (material_number, reference)
df_real_components = (
    df_real_bom
    .filter(
        (f.col("component") != f.col("material_number"))
        & (f.col("DC_indicator") == "2-conso")
    )
    .groupBy("material_number", "reference", "component")
    .agg(
        f.sum("quantity_new").alias("quantity"),
        f.first("unit_price").alias("unit_price"),
        f.first("lead_time").alias("lead_time"),
        f.first("plant").alias("plant_component"),
        f.collect_set("path").alias("path_as_built"),
        f.first("plant_ism").alias("plant_ism"),
        f.first("profit_center").alias("profit_center"),
    )
)

# 2. Link past deliveries to their codes
df_past_with_codes = (
    df_past
    .select("material_number", "plant", "reference", "delivery_date", "STDCODE", "VERSIONCODE")
    .distinct()
)

# 3. Last past delivery per (material_number, plant, STDCODE)
w_std = Window.partitionBy("material_number", "plant", "STDCODE").orderBy(f.desc("delivery_date"))
df_last_stdcode = (
    df_past_with_codes
    .withColumn("_rn", f.row_number().over(w_std))
    .filter(f.col("_rn") == 1)
    .select("material_number", "plant", "STDCODE", "reference",
            f.col("delivery_date").alias("delivery_date_last_std"))
)

# 4. Last past delivery per (material_number, plant, VERSIONCODE)
w_ver = Window.partitionBy("material_number", "plant", "VERSIONCODE").orderBy(f.desc("delivery_date"))
df_last_version = (
    df_past_with_codes
    .withColumn("_rn", f.row_number().over(w_ver))
    .filter(f.col("_rn") == 1)
    .select("material_number", "plant", "VERSIONCODE", "reference",
            f.col("delivery_date").alias("delivery_date_last_ver"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Tr 3. Get BOM Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr 3.1. Get and aggregate BOMs for each case : past, avg, std, version

# COMMAND ----------

# Unit BOM per scenario (qty per component for 1 delivery)

# 1. Average BOM per (material_number, plant, component)
df_bom_avg = (
    df_avg_bom
    .groupBy("material_number", f.col("plant"), "component")
    .agg(
        f.sum("quantity").alias("qty_unit_avg"),
        f.first("unit_price").alias("price_avg"),
        f.first("lead_time").alias("lt_avg"),
        f.first("plant_buy").alias("plant_component_avg"),
        f.first("procurement_special_type").alias("procurement_special_type"),
        f.first("bulk_material").alias("bulk_material"),
        f.collect_set("path").alias("path_avg"),
        f.first("profit_center").alias("profit_center"),
    )
)

# 2. Last STDCODE BOM (last reference known before BACKTEST_DATE)
df_bom_stdcode = (
    df_real_components
    .join(df_last_stdcode, ["material_number", "reference"], "inner")
    .select("material_number", "plant", "STDCODE", "component",
            f.col("plant_component").alias("plant_component_std"),
            f.col("quantity").alias("qty_unit_std"),
            f.col("unit_price").alias("price_std"),
            f.col("lead_time").alias("lt_std"),
            "path_as_built",
            "profit_center")
)

# 3. Last VERSIONCODE BOM (last reference known before BACKTEST_DATE)
df_bom_version = (
    df_real_components
    .join(df_last_version, ["material_number", "reference"], "inner")
    .select("material_number", "plant", "VERSIONCODE", "component",
            f.col("plant_component").alias("plant_component_ver"),
            f.col("quantity").alias("qty_unit_ver"),
            f.col("unit_price").alias("price_ver"),
            f.col("lead_time").alias("lt_ver"),
            "path_as_built",
            "profit_center")
)

# 4. Real BOM: actual as-built consumption per (material_number, plant, STDCODE, VERSIONCODE, component, month)
#    Only references with BOM data (same scope as nb_deliveries)
_refs_with_bom = df_real_components.select("material_number", "reference").distinct()

df_actual_with_month = (
    df_actual
    .select("material_number", "plant", "reference", "delivery_date", "STDCODE", "VERSIONCODE")
    .join(_refs_with_bom, ["material_number", "reference"], "inner")
    .withColumn("delivery_month", f.date_trunc("month", "delivery_date"))
    .distinct()
)

# Flag at material_number/plant level: has_real if any actual delivery with BOM data in Jan-today window
df_has_real = df_actual_with_month.select("material_number", "plant").distinct()

df_bom_real = (
    df_real_components
    .join(
        df_actual_with_month.select("material_number", "plant", "reference", "delivery_month", "STDCODE", "VERSIONCODE").distinct(),
        ["material_number", "reference"], "inner",
    )
    .groupBy("material_number", "plant", "STDCODE", "VERSIONCODE", "component", "delivery_month")
    .agg(
        f.round(f.sum("quantity"), 4).alias("qty_real"),
        f.first("unit_price").alias("price_real"),
        f.first("lead_time").alias("lt_real"),
        f.first("plant_component").alias("plant_component_real"),
        f.collect_set("path_as_built").alias("path_real"),
        f.first("profit_center").alias("profit_center_real"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr 3.2. Deliveries statistics
# MAGIC
# MAGIC Count deliveries for each case and flag cases without match

# COMMAND ----------

# References with as-built BOM data
_refs_with_bom = df_real_components.select("material_number", "reference").distinct()

# Use actual deliveries as the base, only those with BOM data (ensures nb_deliveries matches qty_real scope)
df_future_base = (
    df_actual
    .select("material_number", "plant", "delivery_date", "STDCODE", "VERSIONCODE", "reference")
    .join(_refs_with_bom, ["material_number", "reference"], "inner")
    .withColumn("delivery_month", f.date_trunc("month", "delivery_date"))
)

# 1. Nb deliveries per (month, material_number, plant) — count distinct actual references
df_nb_del = (
    df_future_base
    .groupBy("delivery_month", "material_number", "plant")
    .agg(f.countDistinct("reference").alias("nb_deliveries"))
)

# 2. Nb deliveries per (month, material_number, plant, STDCODE) + delta months vs last std
df_nb_std = (
    df_future_base
    .join(df_last_stdcode.drop("reference"), ["material_number", "plant", "STDCODE"], "left")
    .withColumn("delta_months_std", f.round(f.months_between("delivery_date", "delivery_date_last_std"), 1))
    .groupBy("delivery_month", "material_number", "plant", "STDCODE")
    .agg(
        f.countDistinct("reference").alias("nb_del_std"),
        f.round(f.first("delta_months_std"), 1).alias("delta_months_std"),
    )
)

# 3. Nb deliveries per (month, material_number, plant, VERSIONCODE) + delta months vs last ver
df_nb_ver = (
    df_future_base
    .join(df_last_version.drop("reference"), ["material_number", "plant", "VERSIONCODE"], "left")
    .withColumn("delta_months_ver", f.round(f.months_between("delivery_date", "delivery_date_last_ver"), 1))
    .groupBy("delivery_month", "material_number", "plant", "VERSIONCODE")
    .agg(
        f.countDistinct("reference").alias("nb_del_ver"),
        f.round(f.first("delta_months_ver"), 1).alias("delta_months_ver"),
    )
)

# 4. Late deliveries not done: in MOD follow-up but not in LIPS (per ISM/plant/month)
df_late_not_done = (
    df_future
    .select("material_number", "plant", "delivery_month", "sales_order", "sales_order_item")
    .join(
        df_actual.select("sales_order", "sales_order_item").distinct(),
        ["sales_order", "sales_order_item"], "left_anti",
    )
    .groupBy("material_number", "plant", "delivery_month")
    .agg(f.countDistinct("sales_order", "sales_order_item").alias("nb_late_not_done"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Tr 4. Aggregate per material_number/plant/delivery month/component

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr 4.1. Detailled data : per standard and version

# COMMAND ----------

# 1. Detail groupBy keys
grp_detail = ["delivery_month", "material_number", "plant", "STDCODE", "VERSIONCODE", "component"]
grp_code = ["delivery_month", "material_number", "plant", "STDCODE", "VERSIONCODE"]

# 2. Nb deliveries per (month, material_number, plant, STDCODE, VERSIONCODE) — real counts
df_nb_detail = (
    df_future_base
    .groupBy(*grp_code)
    .agg(f.countDistinct("reference").alias("nb_deliveries"))
)

# 3. Join delta months
df_nb_detail = (
    df_nb_detail
    .join(df_nb_std.select("delivery_month", "material_number", "plant", "STDCODE", "delta_months_std"),
          ["delivery_month", "material_number", "plant", "STDCODE"], "left")
    .join(df_nb_ver.select("delivery_month", "material_number", "plant", "VERSIONCODE", "delta_months_ver"),
          ["delivery_month", "material_number", "plant", "VERSIONCODE"], "left")
)

# 4. Match flags per code
df_std_match = (
    df_bom_stdcode
    .select("material_number", "plant", "STDCODE")
    .distinct().withColumn("_std_matched", f.lit(True))
)
df_ver_match = (
    df_bom_version
    .select("material_number", "plant", "VERSIONCODE")
    .distinct().withColumn("_ver_matched", f.lit(True))
)
df_nb_detail = (
    df_nb_detail
    .join(df_std_match, ["material_number", "plant", "STDCODE"], "left")
    .join(df_ver_match, ["material_number", "plant", "VERSIONCODE"], "left")
    .fillna(False, ["_std_matched", "_ver_matched"])
)

# COMMAND ----------

# 1. Explode by component per scenario
df_detail_avg = (
    df_nb_detail
    .join(df_bom_avg, ["material_number", "plant"], "left")
    .filter(f.col("component").isNotNull())
    .withColumn("qty_avg", f.round(f.col("nb_deliveries") * f.col("qty_unit_avg"), 4))
    .select(*grp_detail, "qty_avg", "price_avg", "lt_avg", "plant_component_avg",
            "procurement_special_type", "bulk_material", "path_avg", "profit_center")
)

df_detail_std = (
    df_nb_detail
    .join(df_bom_stdcode, ["material_number", "plant", "STDCODE"], "left")
    .withColumn("qty_std",
        f.when(f.col("_std_matched") & f.col("qty_unit_std").isNotNull(),
               f.round(f.col("nb_deliveries") * f.col("qty_unit_std"), 4)))
    .select("delivery_month", "material_number", "plant", "STDCODE", "VERSIONCODE",
            "component", "qty_std", "price_std", "lt_std", "plant_component_std",
            f.col("path_as_built").alias("path_as_built_std"),
            f.col("profit_center").alias("profit_center_std"))
    .filter(f.col("component").isNotNull())
)

df_detail_ver = (
    df_nb_detail
    .join(df_bom_version, ["material_number", "plant", "VERSIONCODE"], "left")
    .withColumn("qty_ver",
        f.when(f.col("_ver_matched") & f.col("qty_unit_ver").isNotNull(),
               f.round(f.col("nb_deliveries") * f.col("qty_unit_ver"), 4)))
    .select("delivery_month", "material_number", "plant", "STDCODE", "VERSIONCODE",
            "component", "qty_ver", "price_ver", "lt_ver", "plant_component_ver",
            f.col("path_as_built").alias("path_as_built_ver"),
            f.col("profit_center").alias("profit_center_ver"))
    .filter(f.col("component").isNotNull())
)

# 2. Material attributes (CACCCODE, DESIGNATION, material_description)
df_material_attrs = (
    df_a320_prepared
    .select("material_number", "plant", "CACCCODE", "DESIGNATION", "material_description")
    .distinct()
)

# 3. Material-level profit_center fallback (from avg BOM)
df_material_profit_center = (
    df_avg_bom
    .select("material_number", "plant", "profit_center")
    .filter(f.col("profit_center").isNotNull())
    .distinct()
    .groupBy("material_number", "plant")
    .agg(f.first("profit_center", ignorenulls=True).alias("_material_profit_center"))
)

# 4. Phantom filter: proc_special_type in (50,55) and bulk_material = X
_is_phantom = f.coalesce(
    (f.col("procurement_special_type").isin("50","55")) & (f.col("bulk_material") == "X"),
    f.lit(False),
)

# COMMAND ----------

# Compute all columns
df_demand_detail = (
    df_detail_avg
    .join(df_detail_std, grp_detail, "full")
    .join(df_detail_ver, grp_detail, "full")
    .join(df_nb_detail.select(*grp_code, "nb_deliveries", "_std_matched", "_ver_matched",
                              "delta_months_std", "delta_months_ver"),
          grp_code, "left")
    # profit_center: coalesce from BOM sources, then material-level fallback
    .join(df_material_profit_center, ["material_number", "plant"], "left")
    .withColumn("profit_center", f.coalesce("profit_center", "profit_center_std", "profit_center_ver", "_material_profit_center"))
    .drop("_material_profit_center")
    # has_avg flag: component present in avg BOM (before coalesce)
    .withColumn("has_avg", f.col("qty_avg").isNotNull())
    # qty fallback: unmatched -> avg, matched + absent -> 0
    .withColumn("qty_avg", f.coalesce("qty_avg", f.lit(0)))
    .withColumn("qty_std", f.coalesce(f.col("qty_std"), f.when(~f.col("_std_matched"), f.col("qty_avg")), f.lit(0)))
    .withColumn("qty_ver", f.coalesce(f.col("qty_ver"), f.when(~f.col("_ver_matched"), f.col("qty_avg")), f.lit(0)))
    # collect distinct plant_component across scenarios
    .withColumn("plant_component", f.array_distinct(f.array("plant_component_avg", "plant_component_std", "plant_component_ver")))
    .withColumn("plant_component", f.filter("plant_component", lambda x: x.isNotNull()))
    # unit_price: filter nulls, round to 3dp, then distinct
    .withColumn("unit_price", f.filter(f.array("price_avg", "price_std", "price_ver"), lambda x: x.isNotNull()))
    .withColumn("unit_price", f.array_distinct(f.transform("unit_price", lambda x: f.round(x, 3))))
    .withColumn("lead_time", f.array_distinct(f.array("lt_avg", "lt_std", "lt_ver")))
    .withColumn("lead_time", f.filter("lead_time", lambda x: x.isNotNull()))
    # merge as-built paths from std and ver into one list
    .withColumn("path_as_built", f.array_union(
        f.coalesce("path_as_built_std", f.array()),
        f.coalesce("path_as_built_ver", f.array())))
    .withColumn("path_as_built", f.array_distinct("path_as_built"))
    .withColumn("has_std", f.col("_std_matched"))
    .withColumn("has_ver", f.col("_ver_matched"))
    .filter(~_is_phantom & ~f.col("component").startswith("49"))
    # Join real BOM (actual Jan→today) - qty at same grain as detail (per code combo)
    .join(df_bom_real.select("material_number", "plant", "STDCODE", "VERSIONCODE", "component", "delivery_month", "qty_real"),
          ["material_number", "plant", "STDCODE", "VERSIONCODE", "component", "delivery_month"], "left")
    .withColumn("qty_real", f.round(f.coalesce("qty_real", f.lit(0.0)), 4))
    .withColumn("Ratio_real_avg", f.round(f.try_divide(f.col("qty_avg") - f.col("qty_real"), f.col("qty_real")), 4))
    .withColumn("Ratio_real_std", f.round(f.try_divide(f.col("qty_std") - f.col("qty_real"), f.col("qty_real")), 4))
    .withColumn("Ratio_real_ver", f.round(f.try_divide(f.col("qty_ver") - f.col("qty_real"), f.col("qty_real")), 4))
    # has_real at material_number/plant level
    .join(df_has_real.withColumn("_has_real", f.lit(True)),
          ["material_number", "plant"], "left")
    .withColumn("has_real", f.coalesce(f.col("_has_real"), f.lit(False)))
    .drop("_has_real")
    # Late deliveries not done (per material_number/plant/month)
    .join(df_late_not_done, ["material_number", "plant", "delivery_month"], "left")
    .withColumn("nb_late_not_done", f.coalesce("nb_late_not_done", f.lit(0)))
    .withColumn("plant", f.col("plant").cast("int"))
)

# COMMAND ----------

# Rename and reorder
df_demand_detail = (
    df_demand_detail
    .join(df_material_attrs.withColumn("plant", f.col("plant").cast("int")), ["material_number", "plant"], "left")
    .select(
        f.col("material_number").alias("Material_number"),
        f.col("material_description").alias("Material_description"),
        f.col("CACCCODE"),
        f.col("DESIGNATION").alias("Designation"),
        f.col("plant").alias("Plant"),
        f.col("profit_center").alias("Profit_center"),
        f.col("delivery_month").alias("Delivery_month"),
        f.col("STDCODE"),
        f.col("VERSIONCODE"),
        f.col("component").alias("Component"),
        f.col("plant_component").alias("Component_plant"),
        f.col("unit_price").alias("Unit_price"),
        f.col("lead_time").alias("Lead_time"),
        f.col("qty_real").alias("Qty_real"),
        f.col("nb_deliveries").alias("Nb_deliveries"),
        f.col("qty_avg").alias("Qty_avg"),
        f.col("qty_std").alias("Qty_std"),
        f.col("qty_ver").alias("Qty_ver"),
        f.col("Ratio_real_avg").alias("Ratio_real_avg"),
        f.col("Ratio_real_std").alias("Ratio_real_std"),
        f.col("Ratio_real_ver").alias("Ratio_real_ver"),
        f.col("delta_months_std").alias("Delta_months_std"),
        f.col("delta_months_ver").alias("Delta_months_ver"),
        f.col("has_avg").alias("Has_avg"),
        f.col("has_real").alias("Has_real"),
        f.col("has_std").alias("Has_std"),
        f.col("has_ver").alias("Has_ver"),
        f.col("nb_late_not_done").alias("Nb_late_not_done"),
        f.col("path_avg").alias("Path_avg"),
        f.col("path_as_built").alias("Path_as_built"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr 4.2. Aggregated data

# COMMAND ----------

# Aggregation per (Material_number, Plant, Delivery_month, Component)
df_demand_all = (
    df_demand_detail
    .groupBy("Material_number", "Plant", "Delivery_month", "Component")
    .agg(
        f.flatten(f.collect_set("Unit_price")).alias("_prices"),
        f.flatten(f.collect_set("Lead_time")).alias("_lts"),
        f.flatten(f.collect_set("Component_plant")).alias("_plants"),
        f.flatten(f.collect_set("Path_as_built")).alias("_paths_ab"),
        f.first("Material_description", ignorenulls=True).alias("Material_description"),
        f.first("CACCCODE", ignorenulls=True).alias("CACCCODE"),
        f.first("Designation", ignorenulls=True).alias("Designation"),
        f.first("Profit_center", ignorenulls=True).alias("Profit_center"),
        f.first("Path_avg", ignorenulls=True).alias("Path_avg"),
        f.round(f.sum("Qty_avg"), 4).alias("Qty_avg"),
        f.round(f.sum("Qty_std"), 4).alias("Qty_std"),
        f.round(f.sum("Qty_ver"), 4).alias("Qty_ver"),
        f.round(f.sum("Qty_real"), 4).alias("Qty_real"),
        f.first("Nb_late_not_done", ignorenulls=True).alias("Nb_late_not_done"),
        f.first("Delta_months_std", ignorenulls=True).alias("Delta_months_std"),
        f.first("Delta_months_ver", ignorenulls=True).alias("Delta_months_ver"),
        f.first("Has_avg", ignorenulls=True).alias("Has_avg"),
        f.first("Has_real", ignorenulls=True).alias("Has_real"),
        f.sum(f.when(f.col("Has_std"), 1).otherwise(0)).alias("_nb_has_std"),
        f.sum(f.when(f.col("Has_ver"), 1).otherwise(0)).alias("_nb_has_ver"),
    )
    .join(
        df_nb_del.select(
            f.col("plant").cast("int").alias("Plant"),
            f.col("material_number").alias("Material_number"),
            f.col("delivery_month").alias("Delivery_month"),
            "nb_deliveries",
        ),
        ["Delivery_month", "Material_number", "Plant"], "left",
    )
    .withColumnRenamed("nb_deliveries", "Nb_deliveries")
    .withColumn("Unit_price", f.array_distinct(f.transform("_prices", lambda x: f.round(x, 3))))
    .withColumn("Lead_time", f.array_distinct("_lts"))
    .withColumn("Component_plant", f.array_distinct("_plants"))
    .withColumn("Path_as_built", f.array_distinct("_paths_ab"))
    .withColumn("Has_std", f.col("_nb_has_std") > 0)
    .withColumn("Has_ver", f.col("_nb_has_ver") > 0)
    .withColumn("Ratio_real_std", f.round(f.try_divide(f.col("Qty_std") - f.col("Qty_real"), f.col("Qty_real")), 4))
    .withColumn("Ratio_real_ver", f.round(f.try_divide(f.col("Qty_ver") - f.col("Qty_real"), f.col("Qty_real")), 4))
    .withColumn("Ratio_real_avg", f.round(f.try_divide(f.col("Qty_avg") - f.col("Qty_real"), f.col("Qty_real")), 4))
    .select(
        "Material_number", "Material_description",
        "CACCCODE", "Designation", "Plant", "Profit_center",
        "Delivery_month",
        "Component", "Component_plant", "Unit_price", "Lead_time",
        "Qty_real",
        "Nb_deliveries", "Qty_avg", "Qty_std", "Qty_ver",
        "Ratio_real_avg","Ratio_real_std","Ratio_real_ver",
        "Delta_months_std", "Delta_months_ver",
        "Has_avg", "Has_real", "Has_std", "Has_ver", "Nb_late_not_done",
        "Path_avg", "Path_as_built",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tr 4.3 Save proj tables

# COMMAND ----------

catalog = f"{PIPELINE_WRITE_ENV}_proj"
domain = "supply_chain"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{domain}")

# 1. Detail table per (material_number, month, STDCODE, VERSIONCODE, component)
save_table(
    dest_table=f"{catalog}.{domain}.bom_forecast_demand_detail",
    df=df_demand_detail,
    mode="overwrite",
    overwrite_schema=True,
)

# 2. Aggregated table per (material_number, month, component)
save_table(
    dest_table=f"{catalog}.{domain}.bom_forecast_demand",
    df=df_demand_all,
    mode="overwrite",
    overwrite_schema=True,
)

# COMMAND ----------

# SCRATCH: trace null profit_center
_nulls = df_demand_detail.filter(f.col("Profit_center").isNull()).select("Material_number", "Plant", "Component", "Has_avg").distinct()
print(f"Rows with null profit_center: {_nulls.count()}")
display(_nulls.limit(5))

# Check if those ISMs have profit_center in avg BOM
_sample_ism = _nulls.select("Material_number", "Plant").distinct().limit(3)
print("\nAvg BOM profit_center for those ISMs:")
display(df_avg_bom.join(_sample_ism, ["material_number", "plant"], "inner").select("material_number", "plant", "profit_center").distinct())