# Notebooks BOM existants — recouvrement avec le projet de fiabilité

Les huit notebooks de `reference/notebooks_existants/` sont des copies telles quelles des notebooks déjà utilisés. Ils ne
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

## 5. Trois notebooks de plus : l'indicateur actuel et les contrôles qualité

| Notebook | Rôle |
|---|---|
| `reference/notebooks_existants/create_proj_average_bom.py` | **L'indicateur existant « Average BOM »** (POC) : compare la BOM standard actuelle aux réservations des OF terminés des 12 derniers mois, branche IS |
| `reference/notebooks_existants/quality_checks_material_master_data.py` | règles de qualité du métier sur `material_master_data_plant` (tables `manual_input.*_is_qc`) |
| `reference/notebooks_existants/quality_checks_production_version.py` | idem sur `production_version` |

Les deux notebooks de qualité sont identiques à `quality_checks_bom.py` (même structure,
`validate_business_data.apply_quality_checks_by_rule`). Ils n'apportent rien à la fiabilité : piste pour plus tard, brancher les
mêmes règles sur `bom_item_history_exposed`.

### Ce que le POC « Average BOM » apprend (et qui n'était pas connu)

1. **Catégories de lignes de réservation (`RESB_postp`)** : `L` et `0` = ligne standard issue de la BOM ; `1` = pièce
   manquante ajoutée à la main ; `2`, `3`, `4`, `5` = surconsommation ajoutée à la main. Cela précise la piste Conso 2
   (M7) : les consommations sur lignes 1 à 5 ne sont pas « nominales ». Notre B garde déjà cette catégorie
   (`BOM_item_category`), mais C ne l'utilise pas encore pour la consommation.
2. **« PF usage »** (colonne demandée) : le POC lit `PF_pourcentage_affectation` dans
   `material_master_data_plant_exposed`, un dictionnaire centre de profit → pourcentage d'affectation du composant,
   affiché « x % » ou « <1 % ». C'est très probablement la définition de « PF usage ».
3. **Conso 3** (« régularisation de stock en proportion du centre de profit ») : le même pourcentage d'affectation est la
   piste naturelle pour répartir les écarts d'inventaire d'un composant entre les centres de profit.
4. **Périmètre** : branche `IS` (interconnexion, c'est la demande), OF au statut livré, hors ordres « rush »
   (types `ZP09`, `ZP10`, `ZP11`, `YP09`, `YP10`, `YP11`, production sans article). C n'applique aucun de ces filtres.
5. **Exclusions** : matière en vrac (`schgt` dans les réservations, `Bulk_Material` dans la BOM), composant `SPLIT`
   (gestion de coupe), composants dont le statut usine n'est pas `05`. C garde les articles en vrac (B les marque).
6. **Rebut** : `Quantity_wo_scrap = quantité × (1 − RESB_ausch / 100)`. Chez nous, la Prévision 2 est `esmng` (quantité
   sans rebut composant), ce qui est équivalent quand `bdmng` = `esmng` × (1 + `ausch`) arrondi.
7. **Choix de la BOM** : version de production active (non verrouillée, valide aujourd'hui), arbitrage par les quotas,
   sinon la BOM créée en premier. Ce choix n'existe qu'à la date du jour ; nous prenons l'alternative de l'OF ou de l'OP à T0.
8. **Fantômes** : GraphFrames sur `Procurement_Special_Type = 50` uniquement (comme nous). Le notebook forecast utilise
   `50, 55` + vrac : les notebooks existants ne sont pas d'accord entre eux.
9. **Fiabilité du POC** : binaire par composant, écart de ±5 % entre la BOM standard et la moyenne des réservations
   d'OF ; elle ne mesure pas la consommation réelle. C'est exactement le défaut décrit dans la demande.

## 6. Résultats Genie X1 à X9 (2026-10-13)

| Question | Résultat | Conséquence |
|---|---|---|
| X1 `planned_orders_and_work_order_stack_sap` | Pas un vrai stack : table recréée chaque jour (`CREATE OR REPLACE`), contient seulement les ordres planifiés encore ouverts (991 146) ; un OP converti en OF disparaît | Inutilisable pour l'historique : B reste construit sur les stacks `plaf` / `afko` / `afpo` |
| X2 `sap_mrp_reservation_latest` | `RESB_postp` : `L` 61,3 M, `0` 15,6 M, `R`, `Z`, `2` (189 k), `T`, `U`, `1` (24 k), `3`, `5`, `V`, `W`, `4`… Les significations données par Genie (coproduit, sous-produit, sous-traitance…) contredisent celles du POC (1 = pièce manquante, 2 à 5 = surconsommation) | Significations **non fiables** : à faire confirmer par le métier avant toute règle |
| X3 mouvements 261 de 2025 | 63 717 mouvements (0,91 %), 815 920 pièces (0,68 % de la quantité) sur réservations de catégorie 2 à 5 ; `MSEG_rsnum` / `MSEG_rspos` existent dans `part_movement` | Exclure ces lignes de la Conso 2 changerait le total de moins de 1 % |
| X4 `procurement_special_type = 55` | 7 830 articles, 30 en vrac ; seulement 199 consommés sur OF depuis 2025 (620 mouvements) | 55 n'est pas un fantôme (plutôt approvisionnement spécial / sous-traitance) : on garde `sobsl = 50` (L6) |
| X5 articles `49…` | 24 693 articles (POUT surtout), 606 présents dans des réservations d'OF (68 683 lignes), 31 seulement dans des BOM | Des pièces achetées / outillage consommés hors BOM ; exclusion du notebook forecast à faire confirmer (L7) |
| X6 `work_orders_sap_exposed` | Pas de colonne branche ; `work_order_type`, `work_order_status`, `is_delivered`, `is_cancelled`, `site`, `division`, `region` existent. Types ZP09, ZP10, ZP11, YP09, YP10, YP11 : 29 616 OF sur 280 697 terminés en 2025 (10,6 %) | Filtre branche IS à définir sans colonne branche (L10) ; ces types = « rush » (POC) ou retouche / réparation (Genie) : à trancher |
| X7 `PF_pourcentage_affectation` | Dans `material_plant_exposed` et `material_master_data_plant_exposed`, `MAP<STRING, DOUBLE>`, clés de 3 caractères (E2P, E7P, OTW…) sans zéro de tête | Les clés ne ressemblent pas aux centres de profit numériques de C (101, 119…) : jointure à vérifier avant d'ajouter « PF usage » (X10) |
| X8 réservations d'OF terminés 2025 | 97,2 % de la quantité 261 sur `L` et `0` ; 1,44 % des mouvements 261 sans réservation | Une consommation sans réservation n'a aucune prévision : erreur maximale pour ce composant |
| X9 vrac et `SPLIT` | 529 201 lignes en vrac (4,5 %), 1 299 composants ; 2 783 lignes `SPLIT` | Décision métier sur le vrac (L9) ; `SPLIT` = gestion de coupe (POC) ou éclatement de réservation (Genie) |

## 7. FIFO et matière en vrac

Notre notebook C **n'utilise aucun FIFO**. La consommation d'un composant est lue sur les mouvements 261 / 262 imputés à
l'ordre de fabrication (`work_order`), donc le composant est déjà rattaché à l'OF et à son article fabriqué sans avoir
à trouver de lot. Le FIFO des notebooks existants sert à autre chose : retrouver, pour un avion livré, quels lots de
composants ont été consommés en amont. La matière en vrac (vis, colles…) est donc lue comme les autres composants :
elle est rattachée à l'OF, mais ses quantités sont souvent approximatives (déclarées en vrac) et 1,4 % des mouvements
n'ont pas de réservation. Elle est repérable par `is_bulk_material` (B) pour décider de la garder ou de l'écarter (L9).
