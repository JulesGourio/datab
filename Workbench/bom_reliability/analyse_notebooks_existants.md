# Notebooks BOM existants — recouvrement avec le projet de fiabilité

Les cinq notebooks de `reference/notebooks_existants/` sont des copies telles quelles des notebooks déjà utilisés. Ils ne
suivent pas la structure LEAP (pas de sections, widgets sans `removeAll`, `print`, `.display()`…) : ce sont des
références de logique métier, pas des modèles de code.

## 1. Ce que fait chaque notebook

| Notebook existant | Rôle | Périmètre | Source de la BOM |
|---|---|---|---|
| `reference/notebooks_existants/create_proj_bom_average_multilevel.py` | BOM moyenne multi-niveaux par article (ISM), version de production active | ISM de `mod_effectivity` (A320) | `gold.production.bom` **actuelle**, `production_version_exposed` actuelle |
| `reference/notebooks_existants/create_proj_bom_as_build.py` | BOM « as built » : traçabilité FIFO des sorties (601/643) vers les OF et leurs composants | Livraisons A320 des 12 derniers mois | Mouvements de stock (`part_movement_exposed`), pas de BOM |
| `reference/notebooks_existants/create_proj_bom_forecast.py` | Compare BOM moyenne / dernier standard / dernière version avec l'as built, par mois de livraison (backtest au 2026-01-01) | ISM A320 | BOM moyenne **actuelle** + as built |
| `reference/notebooks_existants/creation_project_bom_is_budget.py` | Budget achats des usines « IS » : as built par commande client, hors surconsommation | Usines commençant par 2 | Mouvements + réservations (`sap_mrp_reservation_latest`) |
| `reference/notebooks_existants/quality_checks_bom.py` | Règles de qualité saisies par le métier sur la BOM | `gold.production.bom_exposed` | BOM **actuelle** |

## 2. Travail en double ou pas ?

**Pas de doublon sur l'historique.** Aucun de ces notebooks ne garde l'historique de la BOM ni des besoins :
ils lisent la BOM *actuelle* (`gold.production.bom`) et comparent avec le passé. C'est le défaut décrit dans la demande
(« la BOM n'est pas historisée »). Dans `reference/notebooks_existants/create_proj_bom_forecast.py`, la « BOM moyenne » d'aujourd'hui est
comparée avec des livraisons depuis le 2026-01-01 : le backtest utilise une information qui n'existait pas à la date
de référence. Nos notebooks A (`bom_item_history`) et B (`order_component_requirement_history`) donnent ce qui
manquait : la BOM et les besoins tels qu'ils étaient connus à chaque 1er du mois.

**Pas de doublon sur la consommation, mais une autre échelle.**

| | Existant | Projet de fiabilité (C) |
|---|---|---|
| Grain | par livraison d'ISM (avion / MSN), traçabilité vers le bas | par ordre de fabrication et par composant |
| Prévision | BOM moyenne / standard / version | besoins (OP / OF) connus à T0 + BOM standard connue à T0 |
| Consommation | consommation tracée jusqu'à l'ISM livré | consommation des OF commencés après T1 et terminés avant T2 |
| Périmètre | A320 / usines IS | tous les articles fabriqués (AF) |

Les deux répondent à des questions différentes. La traçabilité FIFO de `reference/notebooks_existants/create_proj_bom_as_build.py` n'est pas
nécessaire pour C : la consommation d'un OF est déjà liée à l'OF dans `part_movement_exposed`.

**Pourquoi un historique est toujours nécessaire.** La consommation est un journal d'événements (les mouvements restent
disponibles) : il n'y a rien à historiser de ce côté. La **prévision**, en revanche, doit être celle connue à T0 :
les ordres planifiés sont supprimés quand ils deviennent des OF, les BOM sont modifiées par les changements
d'ingénierie, et les réservations changent. Sans snapshot à T0, la comparaison est faussée. Les snapshots mensuels
construits depuis les tables `*_stack` de la landing zone sont donc le seul moyen d'obtenir cette prévision.

## 3. Logique reprise, à reprendre ou à aligner

| Sujet | Existant | Projet | Décision |
|---|---|---|---|
| Annulations de mouvements | `process_sap_exception_flows` + `material_doc_origin` : annule les 261 par les 262 | Quantité signée par `DC_indicator` (H = +, S = −) | Équivalent pour des sommes par OF et composant. À vérifier sur un échantillon : annulations « de zéro », sans référence |
| Fantômes | `procurement_special_type in (50, 55)` **et** `bulk_material = X` | `MARC.SOBSL = '50'` | **À aligner ou à justifier** (question L6) |
| Composants `49…` exclus | oui, dans le notebook forecast | non | À expliquer avec le métier (L7) |
| Surconsommation | exclut les consommations sur réservations de catégorie 2 à 5 (`RESB_postp`) dans le notebook budget | Conso 2 = 261 / 262 seulement | **Piste pour la Conso 2 « nominale »** (M7) : à confirmer par les données et par le métier avant de l'appliquer |
| Délai d'approvisionnement | `lead_time` = délai externe si achat, interne sinon | les deux délais sont livrés | Ajouter un `lead_time` unique après confirmation de la règle (L8) |
| Prix | `standard_price_eur_budget` du `material_plant_exposed` actuel | idem | Identique (prix actuel, pas à T0) |
| Types de documents (`D`, `T`) | non traités | exclus | Rien à faire |
| Version de production active | `production_version_exposed` (verrouillée / validité) | alternative de l'OF ou de l'OP à T0 | Pas d'historique de version : on garde l'alternative de l'OF / OP |
| Règles de qualité métier | `leap_utils.business_data_consistency.validate_business_data` sur `bom_exposed` | contrôles GX | Piste : appliquer les mêmes règles à `bom_item_history_exposed` (plus tard) |

## 4. Tables Gold repérées dans ces notebooks

- `gold.production.planned_orders_and_work_order_stack_sap` : pile des OP et des OF (type de document, dates réelles).
- `gold.mrp.sap_mrp_reservation_latest` : réservations actuelles (catégorie de poste `RESB_postp`).
- `gold.supply_chain_logistic.part_movement` (non exposée) : contient `MSEG_rsnum` / `MSEG_rspos`, le lien vers la
  réservation absent de `part_movement_exposed`.

Questions à poser à Genie sur ces tables : voir `genie_questions_and_output_tests.md` (lot 4, questions X1 à X5).
