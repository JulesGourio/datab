# Fiabilité des BOM (use case 93) — fonctionnement du projet

## 1. Le besoin

Pour chaque article fabriqué (AF) (toutes branches ; la demande vise la branche IS, filtre optionnel `branch`), mesurer si la BOM connue à une date T0 a
bien prévu ce qui a été réellement consommé par les ordres de fabrication (OF) commencés après T1 et terminés avant T2.

- Fiabilité d'un AF = 100 % − moyenne, sur ses composants, de |prévision − consommation| / max(prévision, consommation).
- Prévision 1 = besoins bruts des OP / OF à T0 ; Prévision 2 = sans rebut composant ; Prévision 3 = BOM standard à T0.
- Conso 1 = tous les mouvements sur l'OF ; Conso 2 = mouvements nominaux 261 / 262 ; Conso 3 = non calculée (méthode à définir).
- Période par défaut : T1 = T0 + 6 mois, T2 = T1 + 3 mois ; option T1 = T0 + 3 mois. Fiabilité par défaut : Prévision 2 / Conso 2.

## 2. Architecture (trois notebooks)

```
landing zone *_stack (STPO, STKO, STAS, MAST, MARC, MARM)  ──► A  {env}_gold.production.bom_item_history
landing zone *_stack (RESB, PLAF, AFKO, AFPO)              ──► B  {env}_gold.mrp.order_component_requirement_history
A + B + OF + mouvements de stock + données article         ──► C  {env}_proj.supply_chain.uc093_bom_reliability_component
                                                                   {env}_proj.supply_chain.uc093_bom_reliability_work_order
```

| Notebook | Fichier | Rôle |
|---|---|---|
| A | `data_asset/production/bom_history/create_gold_bom_item_history.py` | BOM telle que SAP la connaissait au 1er de chaque mois (Prévision 3) |
| B | `data_asset/mrp/order_component_requirement_history/create_gold_order_component_requirement_history.py` | Besoins ouverts des OP / OF au 1er de chaque mois (Prévisions 1 et 2) |
| C | `proj/uc093-bom_reliability/create_proj_bom_reliability.py` | Périodes, périmètre IS, consommations, erreurs, statut, fiabilité |

**Principe de l'historique.** Les tables `*_stack` de la landing zone gardent chaque extraction SAP complète (jamais
purgées). Pour chaque 1er du mois K, on prend la dernière extraction strictement antérieure à K. A et B se
reconstruisent entièrement (`rebuild_mode = full`) ou n'ajoutent que les mois manquants (`incremental`, partition
`snapshot_date` et `replace_where`). La consommation n'a pas besoin d'historique : les mouvements de stock sont un journal.

## 3. Lancer les notebooks (lab)

Ordre : A, puis B, puis C. Paramètres pour A et B :

```json
{"pipeline_write_env": "dev", "pipeline_read_env": "dev", "reference_read_env": "prod",
 "lab_target_schema": "dev_lab.lab_jules", "by_pass_quality_checks": "false", "debug": "False",
 "log_level": "info", "snapshot_from_date": "", "debug_plant": "", "rebuild_mode": "full"}
```

Pour C : mêmes paramètres sans `snapshot_from_date` ni `rebuild_mode`, avec `branch` vide (toutes les branches ; `IS` pour se limiter à l'interconnexion).

- **Run rapide** : `debug_plant = 2400` (et `snapshot_from_date = 2025-01-01` pour A et B). A et B écrivent alors
  dans `*_debug`, et C (avec le même `debug_plant`) lit et écrit des tables `*_debug`.
- `incremental` est refusé avec `debug_plant`.
- Spécifique au lab (à retirer avant Bitbucket) : cellule `%pip install great_expectations==0.18.22`,
  `sys.path.insert` vers `leap_utils`, neutralisation de `persist` / `cache` sur serverless.
- Les widgets gardent leurs valeurs quand on relance la cellule des widgets.

## 4. Documents du dossier

| Fichier | Contenu |
|---|---|
| `rapport_avancement.md` | Où en est le projet, ce qui reste à faire |
| `guide_notebook_bom_item_history.md` | Présentation du notebook A cellule par cellule, avec les pourquoi et les questions métier |
| `instructions_projet_bom_reliability.md` | Instructions et fichiers pour un projet Claude dédié « BOM Reliability » |
| `spec_technique_historisation_bom.md` | Spécification technique détaillée (v5) |
| `questions_ouvertes.md` | Toutes les questions et décisions, avec niveau de blocage |
| `corrections_log.md` | Résultats des runs de lab et corrections |
| `genie_questions_and_output_tests.md` | Questions Genie (lots 1 à 4, X1 à X11) et tests de sortie |
| `analyse_notebooks_existants.md` | Recouvrement avec les notebooks BOM déjà en production |
| `lineage_bom_production.md`, `champs_bom_explications_et_questions.md`, `briefing_debutant.md`, `questions_metier_et_architecture.md` | BOM officielle, champs SAP, présentation, questions métier |
| `reference/` | Copies des notebooks existants, du job officiel et de la spec v2 (non modifiés) |

## 5. Modèle réutilisable

`templates/notebook_gold_history_template.py` reprend le mécanisme de A pour historiser n'importe quelle table SAP
disponible en `*_stack` : calendrier mensuel, choix de l'extraction, dédoublonnage, mode complet / incrémental,
tables de debug, clé primaire, vue exposée. A et B en sont deux applications.
