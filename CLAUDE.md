# CLAUDE.md — LEAP Databricks coding guidelines

This file is the working contract for anyone (human or Claude) writing Databricks notebooks, bundles and
specs in this repository. It condenses the LEAP Confluence (chapters 1–16 + the Data Engineering Code
Template). When this file and Confluence disagree, **Confluence wins** — then fix this file.

> **Rule zero:** before writing anything, open the closest sibling notebook/bundle in the repo and match it.
> Where this file names a `leap_utils` function, its exact signature must be checked against the
> installed package / an existing notebook — do not guess arguments.

---

## 1. Platform at a glance

- **Medallion layers:** Landing Zone (S3) → **Bronze** (Delta, raw) → **Silver** (cleansed, 1:1 columns) →
  **Gold** (business-facing, enriched) → **Proj** (use-case KPI layer, mainly for PowerBI DirectQuery).
- **Environments:** `dev` → `uat` → `prod`. No environment is ever skipped.
- **Catalog naming:** `{env}_{layer}.{schema}.{table}`, e.g. `dev_silver.finance.general_ledger`,
  `prod_bronze.sap_latecoere_ecc6.ekpo`, `dev_proj.commercial_and_program.uc065_late_orders_lines`.
- **Orchestration:** Databricks Asset Bundles (DAB). Jobs are code, never hand-configured in the UI.
- **Quality:** Great Expectations (GX), two severities: RED (halt) / AMBER (warn).
- **Shared library:** `leap_utils`. If a function exists there, **use it — never reimplement it**.
- **Ingestion:** declarative DLT/SDP pipeline driven by a pilotage table (default), or a bespoke notebook.

### Delivery flow (summary)

Business intake (JIRA EPIC, project `DB`) → Kick-off & Investigation → [Acquisition → Ingestion, only if
source not in Bronze] → DAS + Test Definition → SAP Headers → Silver → Gold → Proj → Quality → DAB job →
PR + Convention Checker → Move to Prod (Regression Gate) → Documentation & handover → Self-service.

**Build does not start until both the DAS and the Test Definition exist.**

---

## 2. Repository layout

```
data_asset/<domain>/<asset>/        # Silver + Gold notebooks for a Data Asset
    create_silver_<asset_name>.py
    create_gold_<asset_name>.py
proj/<uc_folder>/                   # Proj notebooks, one subfolder per use case
    create_proj_<asset_name>.py
platform/asset_bundle/<asset>/      # one DAB per job / pipeline
    databricks.yml
    resources/<name>_job.yml
    src/                            # deploy-time config notebooks (e.g. pilotage setup)
    README.md
Headers/SAP/<table>.yml             # source of truth for SAP field mappings (one YAML per table)
Workbench/<asset>/Spec Output/      # DAS, test_definition.md, PowerBI guides
Workbench/<asset>/working/          # corrections_log
leap_utils/                         # shared library (own PR rules, see §14)
templates/                          # notebook / bundle templates (start from these)
```

- Notebook file name: `create_{layer}_{asset_name}.py`, snake_case (checked by the Convention Checker).
- Proj `<uc_folder>` naming is not standardised (`UC02-Inventory`, `uc16-profit_and_loss`, …): copy the
  pattern of the closest sibling use case; do not invent a new one.
- Header notebooks (`Create.Headers.*`) are one-time setup only — never in a daily job.

---

## 3. Notebook structure (mandatory for all industrial pipeline notebooks)

Fixed section order — **never delete or reorder a section**; if empty, keep it and write `#N/A`:

| # | Section | Content |
|---|---------|---------|
| 1 | **Script Overview** | Pure markdown: script name + layer, purpose, pipeline it belongs to, input tables (env pattern), output tables (env pattern). First cell of the notebook. |
| 2 | **Technical Debt** | Pure markdown: known debt, linked to where it occurs in the code if possible. |
| 3 | **Configuration** | Imports, widgets, env variables, logger, notebook "modes". |
| 4 | **Inputs** | Every input: Bronze/Silver/Gold tables, file reads, manual declarations. |
| 5 | **Input quality checks** (TBD) | Freshness / completeness checks deciding whether to launch. Keep as `#N/A` until agreed. |
| 6 | **Data Preparation** | Single-dataset work: filtering, format cleaning (dates, leading zeros), renaming, prefixing. |
| 7 | **Data Transformations** | Joins, conversions, enrichments, calculated columns. |
| 8 | **Quality Checks** | All GX checks, grouped, immediately before Outputs. |
| 9 | **Outputs** | `save_table`, PK constraint, views. |

Hierarchy used in markdown titles: **Section → Subsection → Block → Sub-block** (so the side TOC reflects it).
Data Preparation and Data Transformations have no fixed subsections — they are made of blocks.

Rules:
- A **block** = one markdown cell explaining the *why*, followed by the code doing one distinct action.
  Prefer many small blocks over a big one. Do not worry about "too much markdown".
- **One block per input table. One block per prep step.** Prep as much as possible before transforming.
- **No `# DBTITLE` markers.**
- Tech debt → link to the code location. Functional/business reasoning goes in the notebook (the *why*, not a
  restatement of the code).
- Notebook files are the Databricks source format (`# Databricks notebook source`,
  `# MAGIC %md`, `# COMMAND ----------`).
- Start from `templates/` — exploratory code can be migrated into the template afterwards.

### Imports

```python
import pyspark.sql.functions as f
from leap_utils.data_asset import table_utils
from leap_utils.common import logger
from leap_utils.common.validation import gx_validation
from great_expectations.dataset import SparkDFDataset
```

- Import **modules, not individual functions** (`table_utils.save_table(...)`, never `from … import save_table`).
- PySpark functions only through the `f.` namespace — never `from pyspark.sql.functions import col, …`.

### Widgets / environments (all notebooks)

```python
dbutils.widgets.removeAll()   # MUST be the first call, otherwise interactive re-runs fail
dbutils.widgets.text("pipeline_write_env", "dev")
dbutils.widgets.text("pipeline_read_env", "dev")
dbutils.widgets.text("reference_read_env", "prod")
dbutils.widgets.text("debug", "False")
dbutils.widgets.dropdown("log_level", defaultValue="info",
                         choices=["debug", "info", "warning", "error", "critical"])
```

| What is read | Variable |
|---|---|
| Bronze source tables | `REFERENCE_READ_ENV` |
| SAP header tables | `REFERENCE_READ_ENV` |
| Reference/master Gold tables | `REFERENCE_READ_ENV` |
| Upstream table from *another* data asset | `PIPELINE_READ_ENV` |
| Own Silver table read by Gold; Gold read by Proj | `PIPELINE_WRITE_ENV` |
| Anything written | `PIPELINE_WRITE_ENV` |

**Never hardcode `dev`/`uat`/`prod`** in a table path (Convention Checker BLOCKER).
Every source table is read **once**, in Inputs — never re-read in Preparation/Transformations.

---

## 4. Silver layer

Silver = cleansed and standardised, directly above Bronze. It renames, casts, dedups, strips leading zeros and
defines the PK, but **keeps every source column: no `.select()` / `.drop()` in Silver** (filtering is Gold's job).

Transform flow, in this order:

1. **Prefix** — `table_utils.add_prefix_to_columns(df, "SOURCE_PREFIX")` *before* the rename (join disambiguation).
2. **Rename** — `table_utils.rename_columns_with_sap_business_header(df, df_header)`.
3. **SAP trailing minus** — `table_utils.transform_float_column()` on **every** float/double column, between
   rename and cast. SAP stores negatives as `525.00-`; a bare `.cast("double")` silently nulls them.
4. **Cast** — `table_utils.cast_columns_with_sap_business_headers(df, df_header)`; use `try_cast` /
   `safe_to_date()` semantics, never a raw `.cast()` / `.to_date()` that can raise (STANDARD finding).
5. **Leading zeros** — strip from all document/master-data identifiers (orders, materials, profit centers…)
   before any join, **on both sides** of a planned join, or the join silently returns nothing.
6. Booleans standardised to `True`/`False` (never Yes/No); dates are real date types, not strings.

Reading:

```python
df_source_raw = spark.read.table(f"{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.{table}")
df_header     = spark.read.table(f"{REFERENCE_READ_ENV}_bronze.headers_sap.headers_sap_{table}")
```

### Joins (Silver and Gold)

- Default **LEFT JOIN**. INNER only when an unmatched row is genuinely corrupt (e.g. header–item pairs).
- Pre-filter and dedup the reference side first. A join must **never fan out** the main table.
- Never `.union()` — use `.unionByName()` (BLOCKER).
- At the join, only `log.info()` what will need checking; the actual check runs in the grouped Quality Checks
  section (see §7).

### Primary key

- Single natural column already unique → keep its own business name as PK.
- Composite → `f.concat_ws("-", col1, col2, …)` named **`{table}_ID`** (the table's own business name,
  e.g. `customer_orders_ID`), even if the grain goes slightly beyond the name.
- PK is **always the first column** (reorder right after creating it).
- Constraint name: `silver_{table}_PK` / `gold_{table}_PK`.

```python
df = df.withColumn("customer_orders_ID",
                   f.concat_ws("-", f.col("sales_document"), f.col("SD_item")))
columns = df.columns
columns.remove("customer_orders_ID")
df = df.select(["customer_orders_ID"] + columns)
```

### Save (Silver)

```python
table_utils.save_table(dest_table=f"{PIPELINE_WRITE_ENV}_silver.{domain}.{table}",
                       df=df_silver, mode="overwrite", overwrite_schema=True)
```

Then register NOT NULL + PK via `table_utils.set_table_column_not_null()` / `table_utils.add_table_primary_key()`.
**Never** call `.saveAsTable()` directly — always `table_utils.save_table()` (BLOCKER).

---

## 5. Gold layer

Gold = analytics-ready, business-facing table: enrichment, business calculations, currency handling, exposed view.

Flow: Setup (same as Silver) → Read → Business logic → Enrichment joins → Amount correction & currency
conversion (only if monetary amounts) → PK → Quality Checks → Save (+ PK constraint + exposed view).

- Read own Silver with `PIPELINE_WRITE_ENV`; reference/master Gold with `REFERENCE_READ_ENV`, pre-filtered
  (e.g. language `spras == "E"`).
- Enrichment against another Gold table happens **in Gold, never in Silver**.
- Business logic: calculated columns, `f.when(...).otherwise(...)` flags, `f.coalesce` fallbacks.
  Document the business meaning of each calculated column.
- Join fan-out is a **RED** check (in the grouped section), not just a warning.

### Amount correction and currency conversion (only when amounts exist)

Order matters:
1. `amount_correction.create_corrected_amounts(df, amount_col, currency_col)` — SAP assumes 2 decimals for all
   currencies; TCURX gives the real count (JPY = 0, TND = 3).
2. **AMBER currency coverage check** — every distinct document currency must have a rate row in
   `{REFERENCE_READ_ENV}_gold.finance.currency_exchange` (exclude same-to-same like EUR→EUR). A missing rate
   silently falls back to 1.0. `log.info()` the risk here; run the check in the grouped section.
3. `table_utils.currency_conversion()` — convert to EUR at **all three rate types** (Consolidation, Budget,
   Spot). Not optional; never pick one selectively.

Conversion date: prefer document/posting date over `current_date()` (which makes amounts drift on every rerun —
flag it explicitly in the spec if used deliberately). Rate-type-to-asset-type mapping is **not yet agreed**:
mark any specific choice "to be confirmed" in the spec.

### Exposed view

```python
excluded_prefixes = ["GLPCA", "EKPO"]  # raw source-table prefixes, technical only
selected_columns = [c for c in df_gold.columns if not any(c.startswith(p) for p in excluded_prefixes)]
table_utils.create_table_view(
    src_full_tablename=f"{PIPELINE_WRITE_ENV}_gold.{domain}.{table}",
    dest_full_tablename=f"{PIPELINE_WRITE_ENV}_gold.{domain}.{table}_exposed",
    select_statement=", ".join(selected_columns),
    replace_view=True,
)
```

Always the `_exposed` suffix, same catalog/schema. Only exposed columns are visible to BI. Historisation: *to be
documented* (not yet defined).

---

## 6. Proj layer

Use-case KPI layer above Gold, independent of the Data Asset (can be added on an existing Gold table).

- **Table:** `{write_env}_proj.<domain>.uc<number>_<table_name>` (e.g. `uc065_late_orders_lines`).
- **Notebook:** `proj/<uc_folder>/create_proj_<asset_name>.py`.
- Reads **only from Gold**, via `PIPELINE_WRITE_ENV` — never Silver or Bronze.
- Holds logic too specific for the Data Asset: scope filters, boolean flags for slicers (`is_alert_5`),
  weighted numerators (`days_late_x_value`), coarser aggregated summaries, integer-cast flags for summable KPIs.
- Don't duplicate Silver/Gold logic. Document every calculated column's business meaning in the header.
- Job order: `Gold (stable) → Proj notebooks (parallel if independent)`.

---

## 7. Quality & validation (Great Expectations)

Quality is enforced at the point of transformation: a notebook validates its output **before saving**; bad data
is never saved.

| Level | Behaviour | Used for |
|---|---|---|
| **RED** | Job halts, data NOT saved | PK unique + not null, join fan-out |
| **AMBER** | Warning logged, pipeline continues | Key dimensions not null, null rates, value sets, currency coverage |

**Placement — grouped only (current rule, 2026-07-16):** *no* GX check runs inline, not even AMBER. All checks
live in **one `# Quality Checks` section immediately before Outputs**. A GX check triggers a Spark action;
grouping them lets the notebook run as a single optimized pass.

At a risky transform, only log:

```python
log.info("Joined material master on material_number/plant — null rate on material_description checked in Quality Checks below")
```

Grouped section:

```python
dfgx = SparkDFDataset(df_final, persist=False)

amber_results = [dfgx.expect_column_values_to_not_be_null(column="material_description")]
gx_validation.validate_and_log_gx_results(
    quality_check_results=amber_results, validation_level="AMBER",
    info_message="material_description populated after material master join",
    error_message="material_description null — check material_number format / leading zeros",
)

red_results = [
    dfgx.expect_column_values_to_not_be_null(column="customer_orders_ID"),
    dfgx.expect_column_values_to_be_unique(column="customer_orders_ID"),
]
gx_validation.validate_and_log_gx_results(
    quality_check_results=red_results, validation_level="RED",
    info_message="customer_orders_ID unique and not null", error_message="PK check failed",
)
```

Standard checks (unless the spec says otherwise):
- **RED always:** PK unique; PK not null.
- **AMBER always:** key dimension columns (plant, material, customer) not null. Known low-fill columns use
  `mostly=` (e.g. `mostly=0.005` for ~0.5 % expected fill).

Manual validation (complements GX, does not replace it): sample row counts vs source, key figures cross-checked
in the source (e.g. SAP SE16N); results and AMBER warnings recorded in
`Workbench/<asset>/working/corrections_log`. The Test Definition's golden examples are a *separate* layer:
they confirm specific business-known records come out right — turn them into assertions/GX custom
expectations before the PR.

---

## 8. Naming conventions

**Tables** — understandable, no filler words ("basics", "all"); snake_case, no acronyms; follows
object/sub-object/type; `_history` suffix goes at the **end**, never the middle; **no `_latest` suffix**
(tables are latest by default); exposed view = `<table>_exposed`.

**Columns** — snake_case with acronyms in UPPERCASE (`sales_doc_ID`, `PO_number`; not `SalesDocId`,
`sales_doc_id`); English only; singular; booleans start with `is_`; date columns contain `_date`; technical
metadata columns start with `_`; master-data columns carry their prefix (`PC_`, `WBS_`, …); amount columns
follow the currency-conversion naming pattern.

**Data quality hygiene** — no two columns with the same information; no empty columns; dates are date types;
booleans are `True`/`False`; SAP negative signs moved to leading position; leading zeros stripped from
master-data identifiers.

**Files/objects** — notebook `create_{layer}_{asset_name}.py`; PK `{table}_ID`; constraints
`silver_{table}_PK` / `gold_{table}_PK`; header tables `headers_sap_<tablename>`.

---

## 9. SAP headers

Every SAP Bronze table used in a pipeline needs a header table — it drives Silver rename/cast. Created
**before** the Silver notebook is built, from the DAS "List of Fields" sheet.

- Location: `prod_bronze.headers_sap.headers_sap_<tablename>`; source: `prod_bronze.sap_latecoere_ecc6.<table>`.
- Schema: `Table_Name`, `Field`, `Business_Name`, `Format` (String/Date/Float/Integer/Boolean…).
- Populate from the DAS field list, else all non-null columns (never entirely-NULL columns). Target **~10–20
  most-used fields**; a lean header keeps Gold focused.
- `Business_Name`: snake_case, acronyms UPPERCASE, rest lowercase (`PO_number`, `sales_doc_ID`).
- When validated, add the word **`Complete`** to the table description.
- **Only fields with a header entry appear in the Gold exposed view** — an incomplete header silently hides data.
- Mirror it in `Headers/SAP/<table>.yml` (reviewed through PR):

```yaml
table_name: VBEP
description: SAP Schedule Line data — one row per schedule line per sales order item
fields:
  - field: vbeln
    business_name: CO_number
    format: String
    description: Sales order number
```

---

## 10. Ingestion (Bronze) and Acquisition

Only needed when the source is **not already in Bronze** (decided at kick-off).

**Acquisition → S3 Landing Zone** `s3://leap-s3-raw-weeu1-{env}/` (`erp/…`, `apps/…`, `manual/…`, `archive/…`).
Parquet files `YYYYmmDD_HHMMSS_{extract_name}_{batch_id}_{file_number}.parquet`, a trailing `*_end.end` file
(uploaded last, triggers ingestion) and a one-row metadata parquet per batch. Needs an IT ticket for the
folder and Airbyte/Talend for the flow. Define the freshness expectation (SLE) so the table appears in daily
monitoring. Archive lifecycle: Standard → Glacier-IR (8 days) → Deep Archive (100 days).

**Ingestion — default is Bundle** (pilotage-table-driven, `Generic_table_ingestion_SDP_notebook.py`; DLT is the
old generation — new pipelines use **SDP**). Formats: CSV, PARQUET, XML (classic cluster), Excel (DBR 17.1+).

- Bundle at `platform/asset_bundle/<pipeline_name>/` with `databricks.yml`, `resources/pipeline.yml`,
  `src/configure_pipeline.py` (creates pilotage table, inserts one row per table).
- Pilotage table `dlt_pilotage_table` in `{env}_bronze.data_quality` is the single source of truth: **one row
  per table, no custom code per table**.
- Auto-added metadata columns: `_meta_id`, `_meta_extraction_timestamp`, `_meta_ingestion_timestamp`,
  `_meta_file_name`, `_meta_file_mode`, `_meta_file_path`, `_meta_validity_check`.
- File naming: `YYYYMMDD_HHMMSS_<identifier>[-F|-D].<ext>` (`-F` full snapshot, `-D` delta).
- Flag PK columns, nullable/expected flags, `frequency`, Unity Catalog tags in the pilotage row.
- Deploy: `databricks bundle deploy --target dev` → run config notebook once → `databricks bundle run
  <pipeline>_pipeline --target dev`; verify row counts before promotion.
- Reference bundles: `airbus_bom` (XML), `mimecast_ingestion` (CSV), `mes_ingestion` (PARQUET).
- **Excel gotchas:** write the sheet unquoted in `excel_data_address` (`Planning!B6:FC1000`, not
  `'Planning'!…` — quoted silently reads 0 rows); bind the range tightly when `excel_use_yaml_schema: true`
  (otherwise mostly-empty rows); set `spark.sql.files.ignoreCorruptFiles: "true"`; schema changes need the
  pilotage columns updated and a redeploy. A green run with no data → inspect the REST event log
  (`details.flow_progress.metrics`: `backlog_files > 0` & 0 rows = reader/address issue; `backlog_files = 0` =
  discovery issue).

**Ingestion — Notebook** only when Bundle can't express the logic. Then carry over by hand: ingestion
timestamp + source filename on every row, grouped `# Quality Checks`, and an archive step following the
Landing Zone convention.

---

## 11. Jobs & orchestration (DAB)

One DAB per job in `platform/asset_bundle/<asset_name>/` (`databricks.yml` + `resources/<name>_job.yml`).

**Job name:** `<JOB_LAUNCH>_<ORCHESTRATION_LEVEL>_<DOMAIN>_<JOB_PURPOSE>`, `First_Second_Third` casing.
`JOB_LAUNCH`: `D` daily, `W` weekly, `M` monthly, `F` file arrival, `Z` manual. Example:
`D_2_SAP_General_Ledger_Data_Asset`.

**Clusters** — always policy-based, never unrestricted, `data_security_mode: SINGLE_USER`, no Photon unless
proven faster. `instance_profile_arn` only if the notebook calls AWS directly (e.g. boto3).

| Size | Policy variable | Use |
|---|---|---|
| XS | `job_cluster_policy_id_XS` | no real Spark work |
| S | `job_cluster_policy_id_S` | **default** for most data assets |
| M multi-node | `job_cluster_policy_id_M_MULTI` | medium, benefits from parallelism |
| M single-node | `job_cluster_policy_id_M` | medium, needs in-memory |
| L single-node | `job_cluster_policy_id_L` | large in-memory |

**Required tags:** `activity-type` (`${var.pipeline_write_env}-job`), `job-purpose`
(`data-asset|project|ingestion|platform`); plus `data-asset-domain` + `data-asset-group` (data asset jobs),
`project-name` (e.g. `uc038-customs-morocco`), `ingestion-system`, `platform-activity` as applicable;
`job-settings-saved` (key only) on prod jobs saved to git.

**Task chain:** `Setup_Cluster (GIT source) → Silver (parallel if different Bronze sources) → Gold → Proj
(parallel if only reading Gold)`.

**Prod checklist:** code from git `main` only (`git: branch: main` under the prod target) · policy-based
cluster · single user · tags complete · DBU/h reasonable (~1–2 light, ≤10 heavy) · `run_as`
`job-runner-sa-{env}` service principal · schedule timezone `Europe/Brussels` · failure notification to
`LEAP_Databricks_Alert@latecoere.aero` with `no_alert_for_canceled_runs: true` · queue enabled · tasks pass
`pipeline_read_env`, `pipeline_write_env`, `reference_read_env` via `base_parameters` · libraries declared on
tasks, never on the cluster.

CLI: `databricks bundle validate|deploy -p job-runner-sa-dev -t dev`; bundle-ize an existing job with
`databricks bundle generate job --existing-job-id <id>`; avoid duplicates with
`databricks bundle deployment bind`.

---

## 12. Git, PR and review

- Branches from `main`: `feature/<maingoal_object>` or `bugfix/<maingoal_object>` (ideally matching the Jira EPIC).
- Merge **only via Pull Request**; **never force-push**; no direct push to `main`.
- PR description: **Description** (business + technical, key points) · **Changes** (bullets) · **Links**
  (job/run in DEV or UAT, PBI workspace) · **TODOs** (checkboxes) · **Questions**.
- Run the **LEAP Convention Checker** before opening the PR. Severities: BLOCKER (−15: hardcoded env,
  `.union()`, no RED check, direct `.saveAsTable()`), STANDARD (−5: missing sections, `print()`, `.cast()`
  instead of `try_cast`, reimplemented `leap_utils`), NAMING (−2). Score = 100 − penalties, graded A–D.
- Review general advice, in order: use `leap_utils` · document the *why* · config-driven over hardcoded lists ·
  careful names (they outlive the PR) · no copy-paste of more than a few lines (extract to `leap_utils`/helper)
  · files in the right place.
- Use `log.info()/log.warning()` — **never `print()`**.

PR review checklist (tables, columns, DQ, PK, docs) = §8 + §4/§5 PK rules + table & column descriptions present
in the Databricks catalog.

---

## 13. Moving to prod

`Dev → UAT → Prod`, strictly sequential.

| Env | Branch | Deployed by | Validated by |
|---|---|---|---|
| Dev | `feature/…` / `bugfix/…` | CoreDev (personal OAuth profile) | CoreDev |
| UAT | `main` (after merge) | CoreAdmin | Business + CoreDev |
| Prod | `main` | CoreAdmin | Business sign-off |

**LEAP Regression Gate** — manual Bitbucket step on the PR. Post a comment
`/regression <job_name>` (or `/regression dev-to-uat <job_name>` / `/regression uat-to-prod <job_name>`),
then run the step from the PR's Builds. 10 checks: code coverage, job structure, source tables, run
performance, table discovery, output table drift, schema regression, data regression, downstream lineage,
downstream regression. `SIGN_OFF_REQUIRED` needs a PR comment token: `JOB-STRUCTURE-ACCEPTED`,
`OUTPUT-TABLE-REMOVED: <catalog.schema.table>`, `REGRESSION-ACCEPTED: <catalog.schema.table>`.
Changes under `leap_utils/` are not covered by this gate — use the dedicated leap_utils regression procedure.

---

## 14. `leap_utils` changes

PEP8 lint, Sphinx docstrings, type hints, unit tests (100 % of new/changed functions, ≥ 90 % overall),
CI green. If a notebook needs the same snippet twice, it probably belongs in `leap_utils`.

---

## 15. Documentation & handover

A pipeline is done only when someone who did not build it can maintain it.

| Artifact | Location | Applies to |
|---|---|---|
| DAS | `Workbench/<asset>/Spec Output/` | all Data Asset & Proj |
| Test Definition | `Workbench/<asset>/Spec Output/test_definition.md` | all builds |
| SAP header YAMLs | `Headers/SAP/<table>.yml` | pipelines with SAP sources |
| Notebook header | first cell of every notebook | all notebooks |
| Corrections log | `Workbench/<asset>/working/corrections_log` | all pipelines |
| PowerBI build guide | `Workbench/<asset>/Spec Output/<asset>_powerbi_guide.md` | all Proj assets |
| PowerBI user guide | `…/<asset>_powerbi_user_guide.md` | only when visuals need explaining |

**PowerBI build guide** must cover: context, KPI list (measure, source column, visual type), ASCII mockup per
page, source tables (grain, filters, row counts from the corrections log), column reference (exact formula +
plain English), data model (relationships, keys, cardinality), DAX measures (only those following from Proj
columns), RAG logic (thresholds/colours), data freshness (overwrite vs append, frequency).

**DAS** (Excel workbook, single source of truth): sheets `LoV`, `Overview` (with R/W matrix), `Inputs`,
`Silver` (one sheet per Silver notebook), `Gold`, `Proj`, `List of Fields` (always populated last; 10–20
fields per source table, more is better than fewer), `PowerBI`. Never overwrite header rows; label in
column B, values in column C. Edit DAS files via PowerShell COM (`New-Object -ComObject Excel.Application`),
**never Python** (corrupts formatted tables).

**Test Definition** (`test_definition.md`, drafted in the same kick-off meeting as the DAS): 2–3 golden
examples · 1–2 expected aggregates (rough, spot-check tolerance) · known edge cases (cancelled orders,
intercompany, missing schedule lines…) · a handful of plain-English acceptance questions · any existing
team documents (Excel, home-made PBI). Status: OPEN, may evolve.

**Self-service** (Databricks views + glossary + Referent validation; PowerBI workspace/publish/refresh) —
see Confluence chapter 16; not yet detailed here.

---

## 16. Working rules for Claude in this repo

1. Before coding: identify the **layer** and the **closest sibling notebook**; read it; copy its structure.
2. Start every notebook from `templates/` — full section order, `#N/A` for empty sections.
3. Never invent a `leap_utils` signature. If unsure, search the repo for existing usage, or ask.
4. Never hardcode environments; never use `print()`, `.union()`, bare `.cast()`, `.saveAsTable()`.
5. No GX inline. One grouped `# Quality Checks` block with RED PK checks before Outputs.
6. No `.select()`/`.drop()` in Silver. LEFT joins, deduped and pre-filtered references, fan-out guarded.
7. Don't expand scope: add what the DAS/Test Definition asks for, nothing else. Don't add columns not in the spec.
8. When writing DAS Excel files, use PowerShell COM, not Python.
9. Open points (not defined yet — say "to be confirmed", don't decide silently): Input quality checks layout,
   Gold historisation, rate-type-to-asset-type mapping, where to put logging, Self-service details.
10. Final answer after any change: list files touched, which checklist items were verified, and anything
    that could not be verified (no Databricks connection in this environment — notebooks can't be run here).
