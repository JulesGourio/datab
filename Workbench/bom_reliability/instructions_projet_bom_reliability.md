# Instructions du projet « BOM Reliability »

À coller dans les instructions du projet, avec en connaissances du projet les fichiers listés en bas.

## Contexte
Projet LEAP (Databricks, Latécoère) : mesurer la fiabilité des BOM. Pour chaque article fabriqué (AF), comparer la
prévision connue à une date T0 (besoins des OP / OF et BOM standard) avec la consommation réelle des OF commencés
après T1 et terminés avant T2. Fiabilité = 100 % − moyenne par composant de |prévision − conso| / max(prévision, conso).
Use case 93, tables `{env}_proj.supply_chain.uc093_bom_reliability_component` et `_work_order`.

## Architecture
- A `create_gold_bom_item_history` : BOM au 1er de chaque mois depuis les piles `*_stack` de la landing zone (Prévision 3).
- B `create_gold_order_component_requirement_history` : besoins ouverts OP / OF au 1er de chaque mois (Prévisions 1 et 2).
- C `create_proj_bom_reliability` : périodes, consommations, erreurs, fiabilité.
- Lab : toutes les écritures dans `dev_lab.lab_jules` (widget `lab_target_schema`).

## Règles de travail
- Respecter les guidelines LEAP (structure de notebook, `leap_utils`, contrôles GX groupés, pas d'environnement en dur).
- Répondre en français ; distinguer ce qui est vérifié par les données de ce qui reste à confirmer par le métier.
- Toute décision métier ouverte figure dans `questions_ouvertes.md` ; ne pas la trancher sans le dire.

## Fichiers à ajouter comme connaissances
`README.md`, `rapport_avancement.md`, `guide_notebook_bom_item_history.md`, `questions_ouvertes.md`,
`spec_technique_historisation_bom.md`, `analyse_notebooks_existants.md`, `corrections_log.md`, et les trois notebooks
A, B, C.
