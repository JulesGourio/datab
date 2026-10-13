# Références de production (copies, ne pas modifier ici)

Copies de fichiers du dépôt de production `leap-dbx-repository` (Bitbucket, branche `main`), fournies le 2026-10-13.
La source de vérité reste Bitbucket : ce dossier sert uniquement à relire le code et la spec sans y accéder.

| Fichier | Contenu |
|---|---|
| `notebooks_prod/as_design_BOM_bronze_to_silver.py` | Task `create_BOM_silver` : Bronze → `prod_silver.production.BOM` |
| `notebooks_prod/as_design_BOM_silver_to_gold.py` | Task `create_BOM_gold` : Silver → `prod_gold.production.BOM` + vue `BOM_exposed` |
| `notebooks_prod/create_silver_material_master_data_plant.py` | Référentiel article × usine, Silver |
| `notebooks_prod/create_gold_material_master_data_plant.py` | Référentiel article × usine, Gold (enrichissement) |
| `job_W_3_SAP_AS_Design_BOM_DataAsset.yml` | Configuration du job (identifiants retirés) |
| `BOM_spec_v2.docx` | Change request « BOM reliability », version 2 |
