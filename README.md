# datab — LEAP Databricks notebooks

Personal sandbox for Databricks notebooks, bundles and specifications, written to the LEAP conventions.

**Workflow:** write here (direct push to `main`, no PR) → test the notebooks on Databricks dev → move them to the
company Bitbucket repo with a PR, Convention Checker and Regression Gate at the end. See CLAUDE.md §0.

- **[CLAUDE.md](CLAUDE.md)** — the coding guidelines (notebook structure, Silver/Gold/Proj rules, quality
  checks, naming, jobs, PR, prod, documentation). Read automatically by Claude Code; also the human reference.
- **[examples/gold/create_gold_work_order_operation.py](examples/gold/create_gold_work_order_operation.py)** — a real
  Gold notebook kept verbatim as the reference for structure and house patterns (CLAUDE.md §3.4 lists what to
  copy and the deviations **not** to copy).
- **[templates/](templates/)** — start every new notebook/bundle from these:
  - `notebook_silver_template.py`, `notebook_gold_template.py`, `notebook_proj_template.py`
  - `bundle/` — DAB skeleton (`databricks.yml`, `resources/job.yml`)

## Quick start for a new data asset

1. Kick-off done, **DAS + Test Definition exist** (`Workbench/<asset>/Spec Output/`).
2. SAP headers created (`Headers/SAP/<table>.yml` + header table in Databricks).
3. Open the reference example, then copy the Silver template → `data_asset/<domain>/<asset>/create_silver_<asset>.py`; then Gold; then Proj
   under `proj/<uc_folder>/` if needed.
4. Add the DAB under `platform/asset_bundle/<asset>/`.
5. Branch `feature/<maingoal_object>` from `main`, run the LEAP Convention Checker, open a PR.

Notebooks cannot be executed outside Databricks. Templates keep `TODO` markers where a `leap_utils` signature is not
confirmed by the reference notebook (see CLAUDE.md §3.5).
