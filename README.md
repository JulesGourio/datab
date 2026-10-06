# datab — LEAP Databricks notebooks

Repository for Databricks notebooks, bundles and specifications following the LEAP conventions.

- **[CLAUDE.md](CLAUDE.md)** — the coding guidelines (notebook structure, Silver/Gold/Proj rules, quality
  checks, naming, jobs, PR, prod, documentation). Read automatically by Claude Code; also the human reference.
- **[templates/](templates/)** — start every new notebook/bundle from these:
  - `notebook_silver_template.py`, `notebook_gold_template.py`, `notebook_proj_template.py`
  - `bundle/` — DAB skeleton (`databricks.yml`, `resources/job.yml`)

## Quick start for a new data asset

1. Kick-off done, **DAS + Test Definition exist** (`Workbench/<asset>/Spec Output/`).
2. SAP headers created (`Headers/SAP/<table>.yml` + header table in Databricks).
3. Copy the Silver template → `data_asset/<domain>/<asset>/create_silver_<asset>.py`; then Gold; then Proj
   under `proj/<uc_folder>/` if needed.
4. Add the DAB under `platform/asset_bundle/<asset>/`.
5. Branch `feature/<maingoal_object>` from `main`, run the LEAP Convention Checker, open a PR.

Notebooks cannot be executed outside Databricks; the templates contain `TODO` markers for details that must be
copied from a sibling notebook (logger setup, `leap_utils` call arguments).
