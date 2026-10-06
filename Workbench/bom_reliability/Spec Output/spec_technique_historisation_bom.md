# Spec technique — Historisation des BOM et mesure de fiabilité (BOM reliability)

| | |
|---|---|
| Statut | **DRAFT v1** — à valider en kick-off (DAS + Test Definition pas encore écrits) |
| Remplace | « Spec technique v2 — Historisation BOM IS » générée par Databricks Genie (revue critique au §2) |
| Couches | Gold (2 tables historisées) + Proj (calcul de fiabilité) |
| Tests | Toutes les sorties en **`dev_lab.lab_jules`** pendant les tests (CLAUDE.md §0.1) |
| Conventions | CLAUDE.md (LEAP). Les noms `prod_…` ci-dessous sont les noms résolus avec `REFERENCE_READ_ENV = prod` ; dans le code ils s'écrivent toujours `f"{REFERENCE_READ_ENV}_…"` / `f"{PIPELINE_WRITE_ENV}_…"` |

Légende : **[À VÉRIFIER]** = hypothèse sur la donnée, à confirmer par les requêtes du §4 avant de coder.
**[TBD]** = décision métier ouverte (liste au §11), à ne pas trancher silencieusement dans le code.

---

## 0. Résumé (à lire en premier)

1. **Ce qui doit être historisé, et ce qui ne l'est pas.**
   - **BOM standard (as-design, Prévision 3)** → à historiser. Les tables `*_stack` de la landing zone permettent un
     **backfill depuis avril 2023** (STKO/STPO) / juin 2024 (MAST/STAS).
   - **Besoins des OP/OF (as-planned, Prévisions 1 et 2)** → à historiser. C'est **le point critique** : la proposition
     Genie lit la table de réservations *courante*, elle ne donne donc **pas** la prévision connue à T0. Si aucun
     historique RESB/PLAF n'existe (stack ou bronze append), **la capture doit démarrer tout de suite** : chaque jour
     non capturé est perdu, et avec T0 = J−6 mois la première mesure complète arrive 6 mois après le début de capture.
   - **Consommations (as-built, Conso 1/2/3)** → **pas d'historisation**. Les mouvements de stock (MSEG) sont des
     documents immuables (une annulation est un nouveau document) : la table Gold courante contient déjà tout l'historique.
2. **3 notebooks au lieu de 4 (+ 1 table CDC)** :

   | # | Notebook | Sortie (prod) | Rôle |
   |---|---|---|---|
   | A | `data_asset/production/bom_history/create_gold_bom_item_history.py` | `prod_gold.production.bom_item_history` (+ `_exposed`) | BOM standard bitemporelle (SCD2), reconstruite depuis les stacks |
   | B | `data_asset/mrp/order_component_requirement_history/create_gold_order_component_requirement_history.py` | `prod_gold.mrp.order_component_requirement_history` (+ `_exposed`) | Besoins composants des OP **et** OF, capturés chaque jour (SCD2) |
   | C | `proj/<uc_folder>/create_proj_bom_reliability.py` | `prod_proj.<domain>.uc<NNN>_bom_reliability_component` + `uc<NNN>_bom_reliability_work_order` | Périmètre OF, Prévisions 1/2/3 à T0, Conso 1/2/3, explosion des fantômes, % d'erreur, fiabilité |

   Pas de notebook « As-Built » séparé : l'agrégation des mouvements est une étape du notebook C. Pas de table CDC : le
   modèle SCD2 (`recorded_from_date` / `recorded_to_date`) répond directement à « quelle était la BOM à la date X ».
3. **Modèle bitemporel** pour la BOM : *temps de connaissance* (`recorded_from/to_date`, ce que SAP contenait à une
   date, issu des snapshots) × *temps de validité* (`DATUV` / `LKENZ` SAP, gestion par numéro de modification).
   Genie ignore complètement la validité : les versions successives d'un même poste sont additionnées.
4. **Ordre de travail** : requêtes de découverte (§4) → démarrage de la capture B (même en lab, planifiée) → backfill A
   → notebook C.

---

## 1. Besoin traduit en données

### 1.1 Rappel du besoin métier

Pour chaque AF (article fabriqué) : comparer la **prévision connue à T0** avec la **consommation des OF commencés
après T1 et terminés avant T2** (T1 = T0 + P1, T2 = T1 + P2, T2 ≤ aujourd'hui ; défaut T0 = J−6 mois, P1 = P2 = 3 mois).

| Mesure | Définition métier | Source de données | Historisation nécessaire ? |
|---|---|---|---|
| Prévision 1 | BOM brute des OP/OF (avec rebut) | RESB `BDMNG` des OP (`BDART = SB`) et OF (`BDART = AR`) **tels que connus à T0** | **Oui** (notebook B) |
| Prévision 2 | BOM des OP/OF sans les ajustements (% rebut, TBD) | Prévision 1 retraitée du rebut composant `AUSCH` / opération `AVOAU` / ensemble (§8.4) | **Oui** (notebook B) |
| Prévision 3 | BOM standard | STKO/STPO/STAS/MAST connus à T0 × quantité de l'OF | **Oui** (notebook A) |
| Conso 1 | Tous les codes mouvement | MSEG (part_movement) imputés à l'OF | Non |
| Conso 2 | Mouvements « nominaux » (TBD, sans la casse) | idem, liste de types de mouvement TBD | Non |
| Conso 3 | Conso 2 + régularisations d'inventaire au prorata du centre de profit (méthode TBD) | idem + mouvements d'inventaire (701/702, 711/712 ? TBD) | Non |

Fiabilité d'un couple (PrévisionX, ConsoY) pour un AF :
`erreur_composant = |prévision − conso| / max(prévision, conso)` (= la formule du besoin dans les deux cas),
statut `surstock` si prévision > conso, `manquant` si prévision < conso, `ok` sinon ;
`fiabilité_AF = 100 % − moyenne(erreur_composant)` sur tous les composants prévus **et/ou** consommés.
Les composants fantômes sont **éclatés** et n'apparaissent jamais eux-mêmes.

### 1.2 Conséquence : ce que les tables historisées doivent permettre

- **A** : « quelle BOM standard SAP connaissait-on à la date K pour (usine, AF, utilisation, alternative), valide à la
  date D ? » — avec quantité de base, quantité composant, unité, rebut composant/opération, fixe, fantôme, vrac.
- **B** : « quels besoins composants portaient l'OP n° X / l'OF n° Y à la date K ? » — avec quantité d'ordre à K,
  quantité de besoin, rebut, fantôme, lien OP → OF.
- La consommation se lit directement dans la table Gold des mouvements.

---

## 2. Revue critique de la proposition Genie

Gravité : 🔴 rend le résultat faux ou non conforme (BLOCKER LEAP) · 🟠 erreur de données probable · 🟡 convention / qualité.

### 2.1 Architecture et conventions (tous notebooks)

| # | Constat | Gravité | Correction |
|---|---|---|---|
| G1 | Environnements en dur (`prod_landingzone…`, `prod_gold…`, `dev_proj…`) | 🔴 BLOCKER | `f"{REFERENCE_READ_ENV}_…"` en lecture, `f"{PIPELINE_WRITE_ENV}_…"` en écriture, `lab_target_schema` en test |
| G2 | `.write…saveAsTable()` direct | 🔴 BLOCKER | `table_utils.save_table()` (A, C) ; `MERGE` Delta pour l'append SCD2 de B (§6.6) |
| G3 | Aucun contrôle GX RED, aucune PK, pas de contrainte | 🔴 BLOCKER | Section `# Quality Checks` groupée, PK `{table}_ID`, contrainte `gold_{table}_PK` |
| G4 | Historique de BOM écrit en **Proj** (`dev_proj.supply_chain.*`) | 🟠 | Donnée réutilisable, indépendante du cas d'usage → **Gold** (`production`, `mrp`). Proj ne contient que le calcul de fiabilité |
| G5 | `logging.basicConfig`, `display()` de vérification, `.count()`/`.collect()`/`.cache()` pour les logs | 🟡 STANDARD | `logger.setup_applevel_logger` ; pas d'action Spark pour logger ; analyses exploratoires dans le corrections_log, pas dans le notebook de job |
| G6 | Pas de structure LEAP (header, Technical debt, Inputs, Prep, Tr., Quality Checks, Outputs) | 🟡 STANDARD | Partir des `templates/` |
| G7 | `.cast("double")` sur des chaînes SAP | 🟠 STANDARD | `try_cast` + traitement du signe moins SAP en fin de chaîne (`525.000-`) — les quantités négatives de BOM (co-produits) sont mises à NULL par un cast nu |
| G8 | `regexp_replace("^0+")` | 🟡 STANDARD | `table_utils.remove_leading_zeros()` sur les deux côtés de chaque jointure |
| G9 | Filtre `IS%` sur le matériel | 🟠 | Contredit « absolument toutes les BOM » ; le besoin est par division / centre de profit. Aucun filtre de branche dans A et B ; le filtrage se fait dans C (division, CP) |

### 2.2 Notebook 1 (As-Design)

| # | Constat | Gravité | Correction |
|---|---|---|---|
| D1 | **Validité ignorée** : `DATUV`/`LKENZ` de STKO, STAS, STPO non exploités, versions (`STKOZ`, `STASZ`, `STPOZ`) non distinguées → un poste modifié par numéro de modification apparaît **deux fois** (ancienne + nouvelle version) et les quantités sont additionnées | 🔴 | Modèle bitemporel, résolution « valide à la date D » à la lecture (§5.8) |
| D2 | `STLTY = 'M'` filtré sur STKO seulement : STPO et STAS ne sont pas filtrés alors que `STLNR` n'est unique que **par catégorie de BOM** → fan-out avec les BOM d'autres catégories | 🔴 | Filtrer `stlty = 'M'` sur STKO, STAS **et** STPO, et joindre aussi sur `stlty` |
| D3 | Jointure sur `snapshot_date` égale entre tables quotidiennes (MAST, STAS) et hebdomadaires (STKO, STPO) en INNER : un chargement quotidien manquant supprime silencieusement toute la semaine | 🟠 | Chaque table est d'abord compressée en intervalles (SCD2) puis jointe par **intersection d'intervalles** (§5.6) — plus aucune dépendance au calendrier des chargements |
| D4 | Fallback avant juin 2024 : `mast_latest` (et le 1er snapshot STAS) **cross-joinés** sur toutes les dates : réinjecte dans le passé des liens créés après coup (pas de filtre sur `andat`), duplique 60 fois les lignes | 🟠 | Rétro-datation du **premier intervalle** seulement, bornée par la date de création (`andat`), avec un flag technique `_is_backdated_link` (§5.5) |
| D5 | 290 M lignes de snapshots complets **+** une table CDC séparée : redondance, et la CDC (événements) ne permet pas de requête « BOM à la date X » | 🟠 | Une seule table SCD2 (quelques millions de lignes : un enregistrement par version réelle) |
| D6 | CDC : au 1er snapshot tous les postes sont `ADDED` ; une disparition du poste (sans `lkenz`) n'est pas détectée ; changement de composant / unité non détecté | 🟠 | Disparu = intervalle fermé ; tout attribut métier fait partie du hash de version |
| D7 | `quantity_per_base_unit` : mélange quantité fixe (par ordre) et proportionnelle (par `BMENG`), division par 1 si `BMENG = 0` | 🟠 | Stocker `component_quantity`, `BOM_base_quantity`, `is_fixed_quantity` bruts ; le calcul de besoin se fait dans C (§8.5), `BMENG = 0` est un contrôle AMBER |
| D8 | Unité : `STPO.MEINS` (unité de saisie BOM) ≠ unité de base du composant, qui est l'unité de RESB et de MSEG | 🟠 | Conversion via MARM dans A (`component_quantity_in_base_unit`) |
| D9 | Fantômes (`DUMPS`) ignorés alors que le besoin les cite explicitement | 🔴 | `is_phantom_item` dans A et B, explosion récursive dans C (§8.6) |
| D10 | Choix de l'alternative non traité (plusieurs alternatives par AF) | 🟠 | Dans C, l'alternative est **celle de l'OF** (`AFKO.STLAL` / version de production) — pas besoin d'historiser MKAL |

### 2.3 Notebook 2 (As-Planned)

| # | Constat | Gravité | Correction |
|---|---|---|---|
| P1 | **Lit les réservations courantes** : la quantité est celle d'aujourd'hui, pas celle connue à T0. Le besoin principal (Prévision 1/2 à T0) n'est pas couvert | 🔴 | Notebook B : capture quotidienne SCD2 (+ backfill si un historique RESB existe) |
| P2 | Seulement les OF (`work_order_origin`) : les **OP** (ordres planifiés), qui représentent l'essentiel de la prévision à 3–6 mois, sont absents | 🔴 | B capture aussi les besoins dépendants des OP (`BDART = 'SB'`, `PLAF`) et le lien OP → OF (`AFPO.PLNUM`) |
| P3 | `filter(is_cancelled_item == False)` : en Spark `NULL == False` → NULL → **ligne supprimée** ; toutes les lignes à NULL disparaissent | 🟠 | `f.coalesce(f.col("is_cancelled_item"), f.lit(False)) == False`, ou garder la ligne et le flag |
| P4 | `qty_bom_pure = qty / (1 + scrap%)` : ignore rebut d'ensemble, rebut opération, indicateur net, quantité fixe | 🟠 | §8.4 |
| P5 | `qty_per_wo_unit` sur la quantité planifiée actuelle | 🟡 | Normalisation volume définie au §8.3 (TBD) |
| P6 | `quantity_consumed` réservation (= `ENMNG`) recopiée : double information avec As-Built | 🟡 | Non historisée dans B (elle change tous les jours et créerait une version par jour) |

### 2.4 Notebook 3 (As-Built)

| # | Constat | Gravité | Correction |
|---|---|---|---|
| B1 | Seulement 261/262 : Conso 1 (« tous les codes ») impossible | 🟠 | Tous les mouvements imputés à l'OF sauf l'entrée de l'AF lui-même ; Conso 2 = liste paramétrée (§8.7) |
| B2 | `groupBy` sur 19 colonnes dont des descriptions et des attributs d'OF : une description qui varie scinde une ligne | 🟡 | Agréger sur la clé (OF, composant) uniquement, enrichir ensuite |
| B3 | `item_value` sommé sans signe | 🟡 | Signe appliqué aussi à la valeur (ou valeur non reprise : la fiabilité est en quantité) |
| B4 | Inner join sur `plant` | 🟡 | Jointure sur l'OF seul (l'OF porte sa division) |

### 2.5 Notebook 4 (Comparaison)

| # | Constat | Gravité | Correction |
|---|---|---|---|
| C1 | `as_planned` a plusieurs lignes par (OF, composant) (plusieurs postes / opérations) ; FULL OUTER JOIN avec `as_built` déjà agrégé → **la consommation est dupliquée** sur chaque ligne de réservation | 🔴 | Agréger chaque côté à (OF, composant) **avant** la jointure ; RED unicité sur la clé de jointure |
| C2 | Snapshot as-design choisi à la date de création de l'OF au lieu de T0 | 🔴 | K = T0 (connaissance), D = date de validité (§8.5) |
| C3 | `design_qty_total = quantity × (1+scrap) × wo_qty_planned` : oublie la quantité de base `BMENG` (erreur d'un facteur BMENG) et les quantités fixes | 🔴 | §8.5 |
| C4 | `overconsumption_vs_scrap` = `delta_built_vs_design` (le notebook l'admet) | 🟡 NAMING | Deux colonnes avec la même information : interdit (CLAUDE.md §8) |
| C5 | **La formule de fiabilité du besoin n'est pas implémentée** (pas de % d'erreur borné, pas de surstock/manquant, pas de moyenne, pas de T0/T1/T2) | 🔴 | Notebook C |
| C6 | Jointure design sur `(composant)` : un même composant peut figurer sur plusieurs postes de la BOM → fan-out | 🟠 | Agréger la BOM à (AF, composant) après explosion |

**Ce qu'on garde de Genie** : la liste des tables stack et leurs fréquences, le dictionnaire de colonnes SAP (complété au
§12), l'idée « snapshot STKO hebdomadaire le plus proche », le signe 261 + / 262 −, le nettoyage des zéros de l'OF.

---

## 3. Architecture cible

```
                       ┌──────────────── Landing zone stacks (historique) ────────────────┐
                       │ prod_landingzone.sap_latecoere_ecc6.{mast,stko,stas,stpo}_stack  │
                       └──────────────────────────────┬───────────────────────────────────┘
                                                      │ (backfill complet, reconstruit à chaque run)
  prod_bronze.sap_latecoere_ecc6.marm_latest ────────►│
                                                      ▼
                       A  create_gold_bom_item_history ──► prod_gold.production.bom_item_history (+_exposed)

  prod_bronze.sap_latecoere_ecc6.{resb,plaf,afko,afpo}_latest ─┐  (capture quotidienne, MERGE SCD2,
  [stack/historique RESB-PLAF si existant → backfill one-shot] ─┤   jamais d'overwrite)
                                                                ▼
                       B  create_gold_order_component_requirement_history ──► prod_gold.mrp.order_component_requirement_history (+_exposed)

  prod_gold.supply_chain_logistic.part_movement_exposed ─┐
  prod_gold.production.work_orders_sap_exposed ──────────┤
  prod_gold.master_data.material_plant / material_exposed┤
  A + B (PIPELINE_WRITE_ENV) ────────────────────────────┤
                                                         ▼
                       C  create_proj_bom_reliability ──► prod_proj.<domain>.uc<NNN>_bom_reliability_component
                                                          prod_proj.<domain>.uc<NNN>_bom_reliability_work_order
```

### 3.1 Tables de sortie

| Table | Prod | Lab (tests) | Grain | Mode d'écriture |
|---|---|---|---|---|
| BOM standard historisée | `prod_gold.production.bom_item_history` | `dev_lab.lab_jules.bom_item_history` | 1 ligne = 1 version d'un poste de BOM (usine, AF, utilisation, alternative, n° BOM, nœud, compteur poste, compteur allocation, compteur en-tête) sur un intervalle de connaissance | overwrite complet (reconstruit depuis les stacks à chaque run) |
| Besoins OP/OF historisés | `prod_gold.mrp.order_component_requirement_history` | `dev_lab.lab_jules.order_component_requirement_history` | 1 ligne = 1 version d'un poste de réservation (`RSNUM`, `RSPOS`, `RSART`) sur un intervalle de connaissance | **MERGE incrémental, jamais d'overwrite** (la donnée capturée n'est pas reconstructible) |
| Fiabilité par composant | `prod_proj.<domain>.uc<NNN>_bom_reliability_component` | `dev_lab.lab_jules.uc<NNN>_bom_reliability_component` | (période T0/P1/P2, division, AF, composant) | overwrite |
| Périmètre OF | `prod_proj.<domain>.uc<NNN>_bom_reliability_work_order` | `dev_lab.lab_jules.uc<NNN>_bom_reliability_work_order` | (période T0/P1/P2, OF) | overwrite |

Vues exposées : `<table>_exposed` dans le même catalog/schéma pour A et B (checklist Gold).
`<domain>` Proj et `uc<NNN>` / `<uc_folder>` : **[TBD]** (numéro de use case JIRA, copier le dossier du cas d'usage voisin).

---

## 4. Étape 0 — Requêtes de découverte (avant toute ligne de code)

À exécuter dans un notebook scratch (pas dans le repo) ; résultats à consigner dans
`Workbench/bom_reliability/working/corrections_log`. Chaque requête lève un **[À VÉRIFIER]** de cette spec.

```sql
-- Q1. Quels historiques existent ? (stacks landing zone ET tables bronze non-"_latest")
SHOW TABLES IN prod_landingzone.sap_latecoere_ecc6 LIKE '*stack*';
SHOW TABLES IN prod_bronze.sap_latecoere_ecc6 LIKE 'resb*|plaf*|afko*|afpo*|stpo*|stko*|stas*|mast*|marm*|mkal*';

-- Q2. Colonnes réelles et nom de la colonne de date d'extraction (extraction_timestamp ? _meta_extraction_timestamp ?)
DESCRIBE TABLE prod_landingzone.sap_latecoere_ecc6.stpo_stack;
DESCRIBE TABLE prod_bronze.sap_latecoere_ecc6.resb_latest;

-- Q3. Profondeur et régularité des snapshots ; plusieurs extractions le même jour ?
SELECT to_date(extraction_timestamp) AS snapshot_date,
       count(DISTINCT extraction_timestamp) AS nb_extractions,
       count(*) AS nb_rows
FROM prod_landingzone.sap_latecoere_ecc6.stpo_stack
GROUP BY 1 ORDER BY 1;
-- idem pour stko_stack, stas_stack, mast_stack (et resb/plaf si un stack existe)

-- Q4. Catégories de BOM et utilisations présentes (filtres stlty / stlan)
SELECT stlty, count(*) FROM prod_landingzone.sap_latecoere_ecc6.stpo_stack
WHERE to_date(extraction_timestamp) = (SELECT max(to_date(extraction_timestamp)) FROM prod_landingzone.sap_latecoere_ecc6.stpo_stack)
GROUP BY 1;
SELECT werks, stlan, count(*) FROM prod_bronze.sap_latecoere_ecc6.mast_latest GROUP BY 1, 2 ORDER BY 1, 2;

-- Q5. Usage des numéros de modification (gestion de la validité) : part de nœuds avec plusieurs versions
WITH last AS (
  SELECT * FROM prod_landingzone.sap_latecoere_ecc6.stpo_stack
  WHERE stlty = 'M'
    AND to_date(extraction_timestamp) = (SELECT max(to_date(extraction_timestamp)) FROM prod_landingzone.sap_latecoere_ecc6.stpo_stack)
)
SELECT count(*) AS nb_items,
       count_if(trim(aennr) <> '') AS nb_with_change_number,
       count(DISTINCT stlnr, stlkn) AS nb_nodes,
       count(DISTINCT stlnr, stlkn, stpoz) AS nb_node_versions,
       count_if(lkenz = 'X') AS nb_deleted_flag
FROM last;

-- Q6. Unicité de la clé technique dans un snapshot (doublons d'extraction ?)
SELECT stlty, stlnr, stlkn, stpoz, count(*) FROM <stpo d'un snapshot> GROUP BY ALL HAVING count(*) > 1;
-- idem STKO (stlty, stlnr, stlal, stkoz), STAS (stlty, stlnr, stlal, stlkn, stasz), MAST (matnr, werks, stlan, stlnr, stlal)

-- Q7. Format des quantités (signe moins en fin de chaîne ? virgule ?)
SELECT menge, ausch FROM <stpo d'un snapshot> WHERE menge LIKE '%-%' OR menge LIKE '%,%' LIMIT 20;

-- Q8. Besoins des OP présents dans RESB ? (BDART = 'SB' + PLNUM renseigné)
SELECT bdart, rsart, count(*), count_if(trim(plnum) <> ''), count_if(trim(aufnr) <> '')
FROM prod_bronze.sap_latecoere_ecc6.resb_latest GROUP BY 1, 2;

-- Q9. La table Gold de réservations couvre-t-elle déjà OP + OF avec les champs nécessaires ?
DESCRIBE TABLE prod_gold.mrp.reservation_mrp_sap;

-- Q10. Colonnes de l'en-tête OF : dates réelles, lien OP (PLNUM), alternative / version de production
DESCRIBE TABLE prod_gold.production.work_orders_sap_exposed;

-- Q11. Mouvements imputés aux OF : types présents, colonne signe (SHKZG ?), unité de la quantité
DESCRIBE TABLE prod_gold.supply_chain_logistic.part_movement_exposed;
SELECT Movement_type, count(*) FROM prod_gold.supply_chain_logistic.part_movement_exposed
WHERE Work_order IS NOT NULL AND Work_order <> '' GROUP BY 1 ORDER BY 2 DESC;

-- Q12. Rétention : les stacks sont-ils purgés ? (min(snapshot) stable d'une semaine à l'autre ?)
```

**Règles de décision issues de Q1/Q8/Q9** :

| Résultat | Conséquence pour B |
|---|---|
| Un stack / bronze append RESB + PLAF existe | Backfill one-shot depuis cet historique, puis capture quotidienne |
| Rien n'existe | Capture quotidienne **à démarrer immédiatement** (job lab planifié en attendant la prod, §9.3) ; avant le début de capture, mode dégradé (§6.7) |
| `reservation_mrp_sap` expose OP + OF avec `BDART`, `PLNUM`, `AUFNR`, `AUSCH`, `AVOAU`, `NETAU`, `DUMPS`, `SCHGT`, `XLOEK`, `STLNR/STLKN/STPOZ`, `BAUGR` | Capturer depuis cette table Gold (réutilisation) plutôt que depuis le bronze |

---

## 5. Notebook A — `create_gold_bom_item_history`

**Fichier** : `data_asset/production/bom_history/create_gold_bom_item_history.py` (à partir de
`templates/notebook_gold_template.py`).
**Règle zéro** : ouvrir d'abord le notebook qui produit `prod_silver.production.bom` (Bitbucket) — il contient déjà la
jointure MAST/STKO/STAS/STPO courante, ses renommages et ses filtres. Les reprendre à l'identique quand ils existent.

### 5.1 Header (première cellule)

```
# GOLD BOM ITEM HISTORY
**Description:** Historique bitemporel de toutes les BOM matériel SAP (STLTY = M) : chaque version de chaque poste,
avec l'intervalle pendant lequel SAP la contenait (recorded_from/to_date) et sa validité SAP (DATUV/LKENZ).
Sert à reconstituer la BOM standard connue à une date T0 (BOM reliability, Prévision 3).
**Highlighted complexities:** 2 axes de temps (connaissance / validité) ; stacks de fréquences différentes
(STKO/STPO hebdo depuis 2023-04, MAST/STAS quotidien depuis 2024-06) ; rétro-datation du premier lien MAST/STAS ;
jointures par intersection d'intervalles ; quantités SAP au format texte avec signe en fin de chaîne.
**Intended Pipeline** D_2_Production_BOM_History_Data_Asset [TBD]
**Inputs Data**
- {REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6.mast_stack
- {REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6.stko_stack
- {REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6.stas_stack
- {REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6.stpo_stack
- {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.marm_latest
- {REFERENCE_READ_ENV}_gold.master_data.material_exposed   (unité de base du composant) [À VÉRIFIER nom/colonne]
**Output Tables (Pipeline)**
- {PIPELINE_WRITE_ENV}_gold.production.bom_item_history
- {PIPELINE_WRITE_ENV}_gold.production.bom_item_history_exposed (view)
```

`# Technical debt` à déclarer : lecture directe de la landing zone (à remplacer par des tables bronze `*_history` si
la plateforme les expose) ; MARM courant utilisé pour toutes les dates ; rétro-datation MAST/STAS avant juin 2024.

### 5.2 Configuration

Imports et widgets du template (y compris `lab_target_schema`). Constantes métier :

```python
BOM_CATEGORY = "M"                 # STLTY : BOM matériel
BOM_USAGES = ["1"]                 # STLAN [TBD selon Q4 : production seulement ?]
DATE_SENTINEL = "9999-12-31"       # recorded_to_date d'une version encore présente dans le dernier snapshot
```

### 5.3 Inputs (une lecture par table, rien d'autre)

```python
LZ = f"{REFERENCE_READ_ENV}_landingzone.sap_latecoere_ecc6"
df_mast_raw = spark.read.table(f"{LZ}.mast_stack")
df_stko_raw = spark.read.table(f"{LZ}.stko_stack")
df_stas_raw = spark.read.table(f"{LZ}.stas_stack")
df_stpo_raw = spark.read.table(f"{LZ}.stpo_stack")
df_marm_raw = spark.read.table(f"{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.marm_latest")
df_material_raw = spark.read.table(f"{REFERENCE_READ_ENV}_gold.master_data.material_exposed")
```

`# Input quality checks` → `#N/A` (règle LEAP encore TBD).

### 5.4 Data Preparation — un bloc `##PrepN` par table

Principe commun à STKO, STAS, STPO, MAST (factoriser dans une fonction locale du notebook, puis proposer
l'extraction dans `leap_utils` si validée) :

1. **Sélection + renommage** via constantes `STPO_COLUMNS` / `STPO_COLUMNS_RENAME` (pattern du notebook de référence).
2. **`snapshot_date = to_date(extraction_timestamp)`** ; si Q3 montre plusieurs extractions par jour, garder la plus
   récente : `max(extraction_timestamp)` par jour puis semi-join (jamais mélanger deux chargements partiels).
3. **Trim** de toutes les chaînes ; filtres `stlty = 'M'` (STKO, STAS, STPO) et `stlan IN BOM_USAGES` (MAST).
4. **Typage** : quantités par `try_cast` après déplacement du signe moins SAP (`table_utils.transform_float_column` —
   signature **[À VÉRIFIER]** ; à défaut, comportement attendu ci-dessous) ; dates `f.try_to_date(col, "yyyyMMdd")` ;
   indicateurs `'X'` → booléens `True/False` (`f.coalesce(f.col(c) == "X", f.lit(False))`).
5. **Zéros non significatifs** : `table_utils.remove_leading_zeros` sur `material_number` (MAST) et
   `component_material_number` (STPO) — ce sont eux qui seront joints à des tables Gold. `STLNR/STLKN/STLAL` restent tels
   quels (jointures internes aux stacks, même format des deux côtés).
6. **Dédoublonnage** sur (clé technique, `snapshot_date`) — contrôle RED d'unicité déclaré ici (`SparkDFDataset`), évalué
   dans `# Quality Checks`.
7. **Compression en intervalles** (gaps & islands, §5.6).

Comportement attendu du traitement du signe (si `transform_float_column` n'existe pas ou ne convient pas) :

```python
def sap_quantity(col_name):
    """'525.000-' -> -525.0 ; '1,5' -> 1.5 ; valeur illisible -> NULL (jamais d'exception)."""
    raw = f.regexp_replace(f.trim(f.col(col_name)), ",", ".")
    unsigned = f.regexp_replace(raw, "-$", "")
    value = unsigned.try_cast("double")          # Column.try_cast : Spark >= 4.0 ; sinon f.expr("try_cast(... AS DOUBLE)")
    return f.when(raw.endswith("-"), -value).otherwise(value)
```
→ à ajuster une fois Q7 connu ; **ne pas** dupliquer ce code dans plusieurs notebooks (candidat `leap_utils`).

#### Prep1 — STKO (en-tête de BOM)

| SAP | Colonne cible | Type | Note |
|---|---|---|---|
| `stlty` | (filtre) | | `= 'M'` |
| `stlnr` | `BOM_number` | string | clé |
| `stlal` | `BOM_alternative` | string | clé |
| `stkoz` | `BOM_header_counter` | string | clé (versions d'en-tête) |
| `datuv` | `header_valid_from_date` | date | validité SAP |
| `lkenz` | `is_header_deleted` | boolean | |
| `aennr` | `header_change_number` | string | |
| `stlst` | `BOM_status` | string | 01 = actif [À VÉRIFIER valeurs] |
| `bmeng` | `BOM_base_quantity` | double | quantité de base |
| `bmein` | `BOM_base_unit` | string | |
| `loekz` | `is_header_locked` | boolean | |

Clé technique par snapshot : (`BOM_number`, `BOM_alternative`, `BOM_header_counter`).

#### Prep2 — STAS (allocation poste ↔ alternative)

| SAP | Colonne cible | Type |
|---|---|---|
| `stlnr`, `stlal`, `stlkn` | `BOM_number`, `BOM_alternative`, `BOM_node` | string (clé) |
| `stasz` | `BOM_allocation_counter` | string (clé) |
| `datuv` | `allocation_valid_from_date` | date |
| `lkenz` | `is_allocation_deleted` | boolean |
| `andat` | `allocation_created_date` | date [À VÉRIFIER présence] |

Clé : (`BOM_number`, `BOM_alternative`, `BOM_node`, `BOM_allocation_counter`).

#### Prep3 — STPO (postes, quantités, rebut) — table principale

| SAP | Colonne cible | Type | Note |
|---|---|---|---|
| `stlnr`, `stlkn`, `stpoz` | `BOM_number`, `BOM_node`, `BOM_item_counter` | string | clé |
| `posnr` | `BOM_item_number` | string | 0010, 0020… |
| `postp` | `BOM_item_category` | string | L stock, N hors stock, R taille variable, T texte… |
| `idnrk` | `component_material_number` | string | zéros retirés |
| `menge` | `component_quantity` | double | **signé** (négatif = co-produit) |
| `meins` | `component_unit` | string | unité de la BOM |
| `fmeng` | `is_fixed_quantity` | boolean | quantité par ordre, pas par unité |
| `ausch` | `component_scrap_percentage` | double | ⭐ rebut composant (20 = +20 %) |
| `avoau` | `operation_scrap_percentage` | double | rebut opération |
| `netau` | `is_net_scrap` | boolean | rebut d'ensemble ignoré si `X` |
| `dumps` | `is_phantom_item` | boolean | ⭐ fantôme |
| `schgt` | `is_bulk_material` | boolean | vrac (pas de sortie sur OF) |
| `alpos`, `alpgr`, `ewahr` | `is_alternative_item`, `alternative_item_group`, `usage_probability` | bool/string/double | postes alternatifs |
| `lgort` | `issue_storage_location` | string | |
| `datuv` | `item_valid_from_date` | date | validité SAP |
| `aennr` | `item_change_number` | string | |
| `lkenz` | `is_item_deleted` | boolean | |
| `vgknt`, `vgpzl` | `previous_BOM_node`, `previous_BOM_item_counter` | string | chaîne de versions (contrôle) [À VÉRIFIER] |

Clé : (`BOM_number`, `BOM_node`, `BOM_item_counter`). Pas de filtre sur `postp` ici (la Gold garde tout ; le filtre
« composants consommables » est fait dans C).

#### Prep4 — MAST (lien matériel ↔ BOM)

| SAP | Colonne cible | Type |
|---|---|---|
| `matnr` | `material_number` | string (zéros retirés) |
| `werks` | `plant` | string |
| `stlan` | `BOM_usage` | string |
| `stlnr`, `stlal` | `BOM_number`, `BOM_alternative` | string |
| `losvn`, `losbs` | `lot_size_from_quantity`, `lot_size_to_quantity` | double |
| `andat` | `BOM_link_created_date` | date |

Clé : (`material_number`, `plant`, `BOM_usage`, `BOM_number`, `BOM_alternative`).

#### Prep5 — conversion d'unité (MARM courant)

`material_number`, `alternative_unit` (`MEINH`), `numerator` (`UMREZ`), `denominator` (`UMREN`) ; unité de base du
composant depuis `material_exposed` (`MARA.MEINS`). Dédoublonnage sur (`material_number`, `alternative_unit`) + RED
d'unicité. Facteur : `quantité_base = quantité × UMREZ / UMREN` quand `component_unit ≠ base_unit`.

### 5.5 Rétro-datation MAST et STAS (avant juin 2024)

STKO/STPO remontent à avril 2023, MAST/STAS à juin 2024. Plutôt que de répliquer le snapshot courant sur 60 dates
(Genie), on **étend uniquement le premier intervalle** de chaque clé MAST/STAS :

```python
FIRST_STKO_SNAPSHOT_DATE = <min(snapshot_date) de STKO préparé>   # calculé, pas en dur
FIRST_MAST_SNAPSHOT_DATE = <min(snapshot_date) de MAST préparé>

df_mast_intervals = df_mast_intervals.withColumn(
    "_is_backdated_link",
    f.col("recorded_from_date") == f.lit(FIRST_MAST_SNAPSHOT_DATE),
).withColumn(
    "recorded_from_date",
    f.when(
        f.col("_is_backdated_link"),
        f.greatest(f.lit(FIRST_STKO_SNAPSHOT_DATE), f.coalesce(f.col("BOM_link_created_date"), f.lit(FIRST_STKO_SNAPSHOT_DATE))),
    ).otherwise(f.col("recorded_from_date")),
)
# idem STAS avec allocation_created_date
```

Ces deux scalaires coûtent une action Spark chacun : acceptable (2 `agg(min)` sur des colonnes de partition) ;
logger leur valeur. Limite connue (Technical debt) : un lien MAST/STAS supprimé *sans* numéro de modification entre
avril 2023 et juin 2024 est perdu → les postes STPO correspondants n'ont pas d'alternative sur cette période
(contrôle AMBER §5.9).

### 5.6 Compression en intervalles (gaps & islands) — par table, avant les jointures

Pour chaque table préparée (clé technique `K`, attributs métier `A`, `snapshot_date`) :

```python
def to_recorded_intervals(df, key_cols, attribute_cols):
    """1 ligne par snapshot -> 1 ligne par version continue (recorded_from_date, recorded_to_date exclusive)."""
    snapshots = (df.select("snapshot_date").distinct()
                   .withColumn("snapshot_seq", f.dense_rank().over(Window.orderBy("snapshot_date"))))
    w_key = Window.partitionBy(*key_cols).orderBy("snapshot_seq")
    df = (
        df.join(snapshots, ["snapshot_date"], how="left")   # petite table de dates : broadcast implicite
        .withColumn("_row_hash", f.xxhash64(*[f.coalesce(f.col(c).cast("string"), f.lit("<null>")) for c in attribute_cols]))
        .withColumn("_prev_hash", f.lag("_row_hash").over(w_key))
        .withColumn("_prev_seq", f.lag("snapshot_seq").over(w_key))
        # nouvelle version si : 1re apparition, attributs changés, ou trou (absente d'au moins un snapshot)
        .withColumn("_is_new_version",
                    f.col("_prev_hash").isNull()
                    | (f.col("_row_hash") != f.col("_prev_hash"))
                    | (f.col("snapshot_seq") != f.col("_prev_seq") + 1))
        .withColumn("_version_id", f.sum(f.col("_is_new_version").cast("int")).over(w_key))
    )
    df_intervals = df.groupBy(*key_cols, "_version_id", *attribute_cols).agg(
        f.min("snapshot_date").alias("recorded_from_date"),
        f.max("snapshot_seq").alias("_last_seq"),
    )
    # recorded_to_date = date du snapshot qui SUIT le dernier où la version est présente (exclusif) ; sentinelle sinon
    next_dates = snapshots.select(
        (f.col("snapshot_seq") - 1).alias("_last_seq"), f.col("snapshot_date").alias("recorded_to_date"))
    return (df_intervals.join(next_dates, ["_last_seq"], how="left")
            .withColumn("recorded_to_date", f.coalesce("recorded_to_date", f.lit(DATE_SENTINEL).cast("date")))
            .drop("_last_seq", "_version_id"))
```

Points d'attention :
- `attribute_cols` = **toutes** les colonnes métier de la table (pas seulement quantité et rebut) : un changement de
  composant, d'unité, de flag fantôme ou de validité crée une nouvelle version.
- Le calcul de `snapshot_seq` est **par table** (MAST quotidien, STPO hebdo) : la notion de « trou » est relative au
  rythme de la table.
- Si Q3 montre des snapshots manquants ponctuels (chargement raté), un poste présent avant et après serait coupé en
  deux versions identiques : acceptable (pas de perte), ou fusionner les îlots adjacents de même hash en option.
- Volume : STPO ≈ 22 M lignes × ~180 snapshots ≈ 4 Md de lignes lues une fois (filtre `stlty = 'M'` + colonnes
  utiles d'abord). Cluster `job_cluster_policy_id_M_MULTI` pour le backfill [À MESURER en lab] ; si trop lourd,
  traiter par tranches de `snapshot_date` (widget `backfill_to_date`) — la compression est alors refaite sur
  l'ensemble à la fin, pas tranche par tranche.

### 5.7 Data Transformations — jointures par intersection d'intervalles

Une jointure par cellule, toutes avec la même condition de chevauchement :

```python
def overlap_join(df_left, df_right, keys):
    """INNER join on keys + overlapping recorded intervals; the result keeps the intersection [max(from), min(to))."""
    df_r = (df_right.withColumnRenamed("recorded_from_date", "_r_from")
                    .withColumnRenamed("recorded_to_date", "_r_to")
                    .withColumnsRenamed({k: f"_r_{k}" for k in keys}))
    join_cond = [df_left[k] == df_r[f"_r_{k}"] for k in keys] + [
        df_left["recorded_from_date"] < df_r["_r_to"],
        df_r["_r_from"] < df_left["recorded_to_date"],
    ]
    return (df_left.join(df_r, join_cond, how="inner")
            .withColumn("recorded_from_date", f.greatest("recorded_from_date", "_r_from"))
            .withColumn("recorded_to_date", f.least("recorded_to_date", "_r_to"))
            .drop("_r_from", "_r_to", *[f"_r_{k}" for k in keys]))
```
Principe : `gauche.from < droite.to AND droite.from < gauche.to`, nouvel intervalle = `[max(from), min(to))`. Les
intervalles d'une même clé ne se chevauchant pas de chaque côté, le résultat n'en a pas non plus.

| Bloc | Jointure | Clés | Type | Justification |
|---|---|---|---|---|
| `## Tr. 1` | STPO ⋈ STAS | `BOM_number`, `BOM_node` | INNER | Un poste sans allocation n'appartient à aucune alternative (inutilisable). Comptage des postes perdus = AMBER |
| `## Tr. 2` | ⋈ STKO | `BOM_number`, `BOM_alternative` | INNER | Alternative sans en-tête = incohérence SAP |
| `## Tr. 3` | ⋈ MAST | `BOM_number`, `BOM_alternative` | INNER | Donne usine + AF + utilisation ; une BOM sans lien MAST n'est pas une BOM matériel exploitable. Une BOM partagée par plusieurs AF/usines **duplique volontairement** les postes (une ligne par AF) — c'est le grain voulu, pas un fan-out |
| `## Tr. 4` | conversion d'unité (Prep5) | `component_material_number`, `component_unit` | LEFT | `component_quantity_in_base_unit`, `component_base_unit` ; NULL si facteur absent → AMBER |
| `## Tr. 5` | drapeau `is_current = recorded_to_date == DATE_SENTINEL` | | | |
| `## Tr. 6` | PK | | | ci-dessous |

Les clés de version (`BOM_header_counter`, `BOM_allocation_counter`, `BOM_item_counter`) font partie du grain : plusieurs
versions de validité d'un même nœud coexistent sur le même intervalle de connaissance — c'est normal, la résolution de
validité se fait à la lecture (§5.8). Ne **pas** dédoublonner sur le nœud.

PK (`## Tr. 6 - Create ID column`, mise en première colonne) :

```python
PK_COL = "bom_item_history_ID"
PK_PARTS = ["plant", "material_number", "BOM_usage", "BOM_alternative", "BOM_number", "BOM_node",
            "BOM_item_counter", "BOM_allocation_counter", "BOM_header_counter", "recorded_from_date"]
df_transf = df_transf.withColumn(PK_COL, f.concat_ws("-", *[f.col(c).cast("string") for c in PK_PARTS]))
```

Schéma de sortie (ordre des colonnes) : `bom_item_history_ID`, `plant`, `material_number`, `BOM_usage`,
`BOM_alternative`, `BOM_number`, `BOM_node`, `BOM_item_counter`, `BOM_allocation_counter`, `BOM_header_counter`,
`BOM_item_number`, `BOM_item_category`, `component_material_number`, `component_quantity`, `component_unit`,
`component_quantity_in_base_unit`, `component_base_unit`, `BOM_base_quantity`, `BOM_base_unit`,
`component_scrap_percentage`, `operation_scrap_percentage`, `is_net_scrap`, `is_fixed_quantity`, `is_phantom_item`,
`is_bulk_material`, `is_alternative_item`, `alternative_item_group`, `usage_probability`, `issue_storage_location`,
`BOM_status`, `is_header_locked`, `header_valid_from_date`, `is_header_deleted`, `header_change_number`,
`allocation_valid_from_date`, `is_allocation_deleted`, `item_valid_from_date`, `is_item_deleted`,
`item_change_number`, `lot_size_from_quantity`, `lot_size_to_quantity`, `recorded_from_date`, `recorded_to_date`,
`is_current`, `_is_backdated_link`.

### 5.8 Lecture « BOM connue à K, valide à D » (utilisée par C, à documenter dans le header)

1. Connaissance : `recorded_from_date <= K < recorded_to_date`.
2. Validité, par niveau (SAP crée une nouvelle version avec un nouveau compteur et un nouveau `DATUV`) :
   - en-tête : par (`plant`, `material_number`, `BOM_usage`, `BOM_alternative`) garder le `BOM_header_counter` de plus
     grand `header_valid_from_date <= D` ; exclure si `is_header_deleted` ;
   - allocation : par (… , `BOM_node`) garder le `BOM_allocation_counter` de plus grand `allocation_valid_from_date <= D` ;
     exclure si `is_allocation_deleted` ;
   - poste : par (… , `BOM_node`) garder le `BOM_item_counter` de plus grand `item_valid_from_date <= D`
     (départage : compteur le plus grand) ; exclure si `is_item_deleted`.
3. Résultat : au plus **une** ligne par (usine, AF, utilisation, alternative, nœud) → contrôle RED dans C.

**[À VÉRIFIER]** avec Q5 et un cas réel (CS03 « date de validité » sur un AF modifié par numéro de modification) que
cette règle reproduit la BOM affichée par SAP. Si Q5 montre que les numéros de modification ne sont quasiment pas
utilisés, l'étape 2 reste correcte (une seule version par nœud) et ne coûte rien.

### 5.9 Quality Checks (section groupée)

| Niveau | Contrôle | GX |
|---|---|---|
| RED | PK non nulle et unique | `expect_column_values_to_not_be_null`, `expect_column_values_to_be_unique` |
| RED | Unicité de la clé technique par snapshot dans chaque table préparée (STKO, STAS, STPO, MAST, MARM) | `expect_compound_columns_to_be_unique` sur les DataFrames de Prep |
| RED | Pas de chevauchement d'intervalles pour une même clé : unicité (clé + `recorded_from_date`) après compression | `expect_compound_columns_to_be_unique` |
| AMBER | `component_material_number`, `component_quantity` non nuls (hors postes texte) | `expect_column_values_to_not_be_null` (`mostly` à calibrer) |
| AMBER | Facteur d'unité trouvé | `expect_column_values_to_not_be_null(column="component_quantity_in_base_unit", mostly=…)` |
| AMBER | `BOM_base_quantity > 0` | `expect_column_values_to_be_between(min_value=0, strict_min=True)` [À VÉRIFIER disponibilité dans la version GX installée] |
| AMBER | Part des postes STPO perdus aux jointures INNER (surtout avant juin 2024) | log + contrôle sur un DataFrame de comptage, calculé ici seulement |

`validation_level = "AMBER" if BYPASS_QUALITY_CHECKS else "RED"` (template).

### 5.10 Outputs

Template Gold : `CATALOG`/`SCHEMA` (override `lab_target_schema`), `save_table(mode="overwrite",
overwrite_schema=True)` — la table est entièrement reconstructible depuis les stacks, l'overwrite est donc sûr **tant
que Q12 confirme que les stacks ne sont pas purgés**. Si les stacks ont une rétention, passer au MERGE incrémental du
§6.6 (même logique). Contrainte `gold_bom_item_history_PK` idempotente. Vue `bom_item_history_exposed` (toutes les
colonnes sauf celles préfixées `_`). Partitionnement : aucun au départ (Liquid Clustering sur `material_number`,
`recorded_from_date` à évaluer si la lecture dans C est lente).

---

## 6. Notebook B — `create_gold_order_component_requirement_history`

**Fichier** : `data_asset/mrp/order_component_requirement_history/create_gold_order_component_requirement_history.py`.
**Objet** : photographier chaque jour les besoins composants de **tous** les OP et OF ouverts, et ne garder que les
changements (SCD2). C'est la seule source possible de Prévision 1/2 à T0.

### 6.1 Header

```
# GOLD ORDER COMPONENT REQUIREMENT HISTORY
**Description:** Historique (SCD2) des besoins composants des ordres planifiés (OP, besoins dépendants) et des
ordres de fabrication (OF, réservations) : quantité de besoin, rebut, fantôme, quantité d'ordre, telles que connues
chaque jour. Sert à reconstituer la prévision connue à T0 (BOM reliability, Prévisions 1 et 2).
**Highlighted complexities:** table d'accumulation NON reconstructible : jamais d'overwrite, MERGE idempotent,
contrôle de complétude du snapshot du jour avant écriture ; conversion OP → OF (nouveau RSNUM, lien AFPO.PLNUM).
**Intended Pipeline** D_2_MRP_Order_Requirement_History_Data_Asset [TBD]
**Inputs Data**
- {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.resb_latest
- {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.plaf_latest
- {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.afko_latest
- {REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.afpo_latest
  (ou {REFERENCE_READ_ENV}_gold.mrp.reservation_mrp_sap / {REFERENCE_READ_ENV}_gold.production.work_orders_sap_exposed
   si Q9/Q10 montrent qu'elles portent tous les champs — réutiliser plutôt que relire le bronze)
**Output Tables (Pipeline)**
- {PIPELINE_WRITE_ENV}_gold.mrp.order_component_requirement_history
- {PIPELINE_WRITE_ENV}_gold.mrp.order_component_requirement_history_exposed (view)
```

### 6.2 Inputs et Prep

**Prep1 — RESB (postes de réservation / besoins dépendants)**

| SAP | Colonne cible | Type | Note |
|---|---|---|---|
| `rsnum`, `rspos`, `rsart` | `reservation_number`, `reservation_item`, `reservation_record_type` | string | **clé** |
| `bdart` | `requirement_type` | string | `AR` réservation d'OF, `SB` besoin dépendant d'OP [À VÉRIFIER Q8] |
| `aufnr` | `work_order_number` | string | zéros retirés |
| `plnum` | `planned_order_number` | string | zéros retirés |
| `matnr` | `component_material_number` | string | zéros retirés |
| `werks` | `plant` | string | |
| `bdmng` | `requirement_quantity` | double | ⭐ Prévision 1 (inclut les rebuts) |
| `meins` | `component_base_unit` | string | unité de base |
| `bdter` | `requirement_date` | date | |
| `ausch` | `component_scrap_percentage` | double | |
| `avoau` | `operation_scrap_percentage` | double | |
| `netau` | `is_net_scrap` | boolean | |
| `dumps` | `is_phantom_item` | boolean | ligne de l'article fantôme lui-même (jamais consommée) |
| `baugr` | `higher_level_assembly` | string | fantôme parent des composants éclatés [À VÉRIFIER] |
| `schgt` | `is_bulk_material` | boolean | |
| `xloek` | `is_item_deleted` | boolean | |
| `postp` | `BOM_item_category` | string | |
| `posnr` | `BOM_item_number` | string | |
| `stlty`, `stlnr`, `stlkn`, `stpoz` | `BOM_category`, `BOM_number`, `BOM_node`, `BOM_item_counter` | string | lien vers A ; vide = composant ajouté manuellement |
| `vornr` | `operation_number` | string | [À VÉRIFIER présence] |

**Non repris volontairement** : `enmng` (quantité prélevée) et `kzear` (sortie finale) — ils changent pendant la
production et créeraient une version par jour ; la consommation vient des mouvements (C).

Filtre : `requirement_type IN ('AR', 'SB')` et (`work_order_number` ou `planned_order_number` renseigné). Pas de filtre
sur `is_item_deleted` : la suppression doit être historisée (elle devient un attribut qui change).

**Prep2 — PLAF (en-tête OP)** : `plnum` → `planned_order_number` ; `matnr` → `material_number` ; `plwrk` → `plant` ;
`gsmng` → `order_quantity` ; `psttr` → `order_planned_start_date` ; `pedtr` → `order_planned_end_date` ;
`paart` → `order_type` ; `stlal` → `BOM_alternative` ; `stlan` → `BOM_usage` ; `verid` → `production_version`.
Dédoublonnage + RED d'unicité sur `planned_order_number`.

**Prep3 — AFKO + AFPO (en-tête OF)** : `aufnr` → `work_order_number` ; `afpo.matnr` → `material_number` ;
`afpo.dwerk` → `plant` ; `afko.gamng` → `order_quantity` ; `afko.gstrp` / `gltrp` → `order_planned_start_date` /
`order_planned_end_date` ; `afko.stlal` / `stlan` → `BOM_alternative` / `BOM_usage` ; `afpo.verid` →
`production_version` ; `afpo.plnum` → `origin_planned_order_number` (⭐ lien OP → OF). Garder `afpo.posnr = '0001'`
(une ligne par OF) [À VÉRIFIER : OF multi-postes ?] ; RED d'unicité sur `work_order_number`.

### 6.3 Transformations

- `## Tr. 1` RESB (OF) ⋈ en-tête OF sur `work_order_number` (LEFT) ; `## Tr. 2` RESB (OP) ⋈ PLAF sur
  `planned_order_number` (LEFT) ; `unionByName` des deux (jamais `.union()`), colonne
  `order_category = 'WORK_ORDER' | 'PLANNED_ORDER'`.
  Les attributs d'en-tête (quantité d'ordre, dates, alternative) sont **dénormalisés** dans chaque poste : un
  changement de quantité d'ordre crée une nouvelle version de tous ses postes — c'est voulu (la quantité d'ordre à T0
  sert à la normalisation, §8.3).
- `## Tr. 3` `snapshot_date` = date d'extraction du bronze (`_meta_extraction_timestamp` [À VÉRIFIER nom]), **pas**
  `current_date()` : si le bronze n'a pas été rafraîchi, on n'enregistre pas une fausse date.
- `## Tr. 4` `_row_hash = xxhash64(toutes les colonnes métier)`.

### 6.4 Clé et colonnes de sortie

Clé naturelle : (`reservation_number`, `reservation_item`, `reservation_record_type`).
PK : `order_component_requirement_history_ID = concat_ws("-", reservation_number, reservation_item,
reservation_record_type, recorded_from_date)`.

Colonnes : PK, `order_category`, `requirement_type`, `planned_order_number`, `work_order_number`,
`origin_planned_order_number`, `plant`, `material_number`, `order_type`, `order_quantity`,
`order_planned_start_date`, `order_planned_end_date`, `BOM_usage`, `BOM_alternative`, `production_version`,
`reservation_number`, `reservation_item`, `reservation_record_type`, `component_material_number`,
`requirement_quantity`, `component_base_unit`, `requirement_date`, `component_scrap_percentage`,
`operation_scrap_percentage`, `is_net_scrap`, `is_phantom_item`, `higher_level_assembly`, `is_bulk_material`,
`is_item_deleted`, `BOM_item_category`, `BOM_item_number`, `BOM_category`, `BOM_number`, `BOM_node`,
`BOM_item_counter`, `operation_number`, `recorded_from_date`, `recorded_to_date`, `is_current`, `_row_hash`,
`_capture_source` (`'daily_capture'` | `'backfill'`).

### 6.5 Quality Checks (avant toute écriture)

| Niveau | Contrôle |
|---|---|
| RED | Clé naturelle unique dans le snapshot du jour (`expect_compound_columns_to_be_unique`) |
| RED | PLAF / en-tête OF uniques sur leur clé de jointure |
| RED | **Complétude** : nb de lignes du snapshot ≥ 80 % du nb de lignes `is_current` de la table (`expect_table_row_count_to_be_between(min_value=…)` [À VÉRIFIER disponibilité]) — protège contre un bronze vide ou partiel qui fermerait toutes les versions courantes |
| RED | `snapshot_date` > `max(recorded_from_date)` de la table, sinon **skip propre** (log, pas d'erreur) : on ne réécrit pas le passé |
| AMBER | `material_number` (AF) non nul après jointure d'en-tête |
| AMBER | `requirement_quantity` non nul |

Après le MERGE : RED unicité de la PK et au plus une ligne `is_current` par clé naturelle.

### 6.6 Écriture SCD2 (MERGE Delta)

`save_table(mode="overwrite")` est **interdit** sur cette table hors création initiale. Aucune fonction
`leap_utils` de MERGE n'est confirmée (§3.5 de CLAUDE.md) → chercher dans le package installé ; à défaut, MERGE Delta
SQL **à faire valider par l'équipe** (ce n'est pas un `.saveAsTable()`).

```python
# 1er run : création via table_utils.save_table(dest_table=..., df=df_snapshot_versions, mode="overwrite", overwrite_schema=True)
#           uniquement si la table n'existe pas (spark.catalog.tableExists), puis contrainte PK.
df_snapshot.createOrReplaceTempView("requirement_snapshot")

# Étape 1 : fermer les versions courantes modifiées ou disparues
spark.sql(f"""
MERGE INTO {DESTINATION_TABLE_FULL_PATH} AS t
USING requirement_snapshot AS s
  ON  t.reservation_number = s.reservation_number
  AND t.reservation_item = s.reservation_item
  AND t.reservation_record_type = s.reservation_record_type
  AND t.is_current
WHEN MATCHED AND t._row_hash <> s._row_hash THEN
  UPDATE SET t.recorded_to_date = s.recorded_from_date, t.is_current = false
WHEN NOT MATCHED BY SOURCE AND t.is_current THEN
  UPDATE SET t.recorded_to_date = DATE'{SNAPSHOT_DATE}', t.is_current = false
""")

# Étape 2 : insérer les nouvelles versions (nouvelles clés + clés modifiées)
spark.sql(f"""
MERGE INTO {DESTINATION_TABLE_FULL_PATH} AS t
USING requirement_snapshot AS s
  ON  t.reservation_number = s.reservation_number
  AND t.reservation_item = s.reservation_item
  AND t.reservation_record_type = s.reservation_record_type
  AND t.is_current
  AND t._row_hash = s._row_hash
WHEN NOT MATCHED THEN INSERT *
""")
```

`requirement_snapshot` porte déjà `recorded_from_date = SNAPSHOT_DATE`, `recorded_to_date = '9999-12-31'`,
`is_current = true` et la PK. Les deux étapes sont **idempotentes** : relancer le même jour ne crée ni ne ferme rien
de plus. Une disparition de RESB (OP converti, OF archivé ou soldé) ferme la version — l'historique reste.
Sécurité : activer la rétention de time travel Delta suffisante sur cette table et ne jamais la `DROP` en dev/lab
sans copie.

### 6.7 Avant le début de capture : mode dégradé (à afficher, pas à cacher)

Si aucun historique RESB/PLAF n'existe, pour une période dont T0 précède le début de capture :
- **OF** : les réservations des OF terminés existent encore dans `resb_latest` (jusqu'à archivage) mais avec leurs
  valeurs *finales* (ajouts manuels, modifications de quantité inclus). Proxy possible : postes issus de la BOM
  (`BOM_number` renseigné), quantités actuelles → chargement one-shot avec `_capture_source = 'backfill'` et
  `recorded_from_date = date de création de l'OF` **[TBD métier : accepter ce proxy ?]**.
- **OP** : aucune reconstitution possible (un OP converti disparaît de SAP).
- Dans C, `forecast_source` indique pour chaque OF si la prévision vient d'un OP à T0, d'un OF à T0, du proxy, ou
  n'existe pas. Le rapport doit afficher cette couverture.

---

## 7. Consommations — pas de notebook dédié

Les mouvements de stock sont immuables : `prod_gold.supply_chain_logistic.part_movement_exposed` contient déjà
l'historique (vérifier la profondeur avec Q11 ; Genie mentionne des données depuis 2020). L'agrégation par
(OF, composant) est faite dans C (§8.7). Si une autre table Gold expose déjà les consommations par OF, la réutiliser.

---

## 8. Notebook C — `create_proj_bom_reliability` (cadrage)

**Fichier** : `proj/<uc_folder>/create_proj_bom_reliability.py`. Lit **uniquement du Gold** : A et B via
`PIPELINE_WRITE_ENV` (ou `LAB_TARGET_SCHEMA` en test), le reste via `REFERENCE_READ_ENV`.

### 8.1 Inputs

| Table (prod) | Usage | Clé de jointure |
|---|---|---|
| `{PIPELINE_WRITE_ENV}_gold.production.bom_item_history` | Prévision 3 | `plant`, `material_number`, `BOM_usage`, `BOM_alternative` |
| `{PIPELINE_WRITE_ENV}_gold.mrp.order_component_requirement_history` | Prévisions 1/2 | `work_order_number` / `planned_order_number` |
| `prod_gold.production.work_orders_sap_exposed` | Périmètre OF (dates réelles, quantités, CP, alternative, OP d'origine) | `work_order_number` |
| `prod_gold.supply_chain_logistic.part_movement_exposed` | Conso 1/2/3 | `Work_order` → `work_order_number` (zéros retirés) |
| `prod_gold.master_data.material_plant` | CP, délai d'appro, classification, type d'appro spécial (fantôme `SOBSL = 50`) | `material_number`, `plant` |
| `prod_gold.master_data.material_exposed` | Description, famille, classe | `material_number` |
| `prod_gold.<…>.<plant / profit_center>_exposed` | Libellés division / CP | [À VÉRIFIER noms] |
| prix composant | `standard_price_eur_budget` (cf. `reservation_mrp_sap`) ou table de prix Gold | [À VÉRIFIER] |

### 8.2 Périodes

Une période = (T0, P1, P2). Le rapport permet de choisir T0/P1/P2 : on **précalcule une grille** (le calcul composant
par composant puis la moyenne ne se fait pas proprement en DAX DirectQuery) :
- T0 = 1er de chaque mois depuis le début d'historique disponible ; P1, P2 ∈ {3} par défaut, liste étendue
  {1, 2, 3, 6} mois **[TBD : volumétrie vs besoin]** ;
- garder seulement T2 = T0 + P1 + P2 ≤ date du run ; colonne `is_default_period` pour T0 = 1er du mois de J−6 mois,
  P1 = P2 = 3.
- Construite comme petite table (`spark.createDataFrame` d'une liste générée), jointe aux OF.

### 8.3 Périmètre OF et rattachement à la prévision T0

Par période : OF avec `actual_start_date >= T1` et `actual_finish_date <= T2` (colonnes réelles de
`work_orders_sap_exposed`, = `AFKO.GSTRI` / `GLTRI` [À VÉRIFIER Q10]), hors OF annulés.
Pour chaque OF, prévision **connue à T0** :
1. si l'OF existait à T0 → ses lignes B avec `recorded_from_date <= T0 < recorded_to_date` (`forecast_source = 'WORK_ORDER_AT_T0'`) ;
2. sinon, si son OP d'origine (`origin_planned_order_number`) existait à T0 → lignes B de l'OP à T0 (`'PLANNED_ORDER_AT_T0'`) ;
3. sinon proxy §6.7 (`'RECONSTRUCTED'`) ou rien (`'NONE'` → Prévisions 1/2 NULL, Prévision 3 reste calculée).

**Normalisation du volume [TBD — décision clé]** : la quantité d'ordre à T0 peut différer de la quantité finale
(OP de 10 devenu OF de 8). Sans correction, la « fiabilité BOM » mesure aussi l'erreur de volume. Proposition par
défaut : `prévision_normalisée = prévision_T0 × quantité_OF_finale / quantité_ordre_T0` (quantité finale =
`order_quantity` de l'OF terminé). Garder la colonne non normalisée pour contrôle.

### 8.4 Prévisions 1 et 2 (depuis B)

- Exclure `is_phantom_item = True` (ligne du fantôme : SAP a déjà éclaté ses composants dans RESB), `is_item_deleted`,
  et `is_bulk_material` **[TBD]**.
- Prévision 1 = Σ `requirement_quantity` par (OF, composant).
- Prévision 2 = Prévision 1 sans rebut **[TBD : quels rebuts ?]**. Proposition :
  `requirement_quantity / (1 + component_scrap_percentage/100) / (1 + operation_scrap_percentage/100)`, et en plus
  `/ (1 + assembly_scrap_percentage/100)` quand `is_net_scrap = False` (rebut d'ensemble `MARC.AUSSS` de l'AF —
  [À VÉRIFIER] s'il est exposé dans `material_plant`). Les quantités fixes ne portent pas de rebut proportionnel.
- Postes de nomenclature (page 3 optionnelle) : composant issu d'un fantôme (`higher_level_assembly` renseigné) →
  `BOM_item_number = '9999'`.

### 8.5 Prévision 3 (depuis A)

Pour chaque OF : BOM de (`plant`, `material_number`, `BOM_usage`, alternative **de l'OF**) **connue à K = T0, valide à
D = date de début prévue de l'OF connue à T0** (sinon début réel) **[TBD : D = T0 ou date de l'OF]**, résolue selon §5.8.

Besoin par poste (unité de base composant) :
```
si is_fixed_quantity : q = component_quantity_in_base_unit
sinon                : q = component_quantity_in_base_unit / BOM_base_quantity × quantité_OF
q = q × (1 + component_scrap_percentage/100)        # rebut composant (BOM standard SAP) [TBD : inclus ou non]
```
puis éclatement des fantômes (§8.6) et Σ par (OF, composant).

### 8.6 Explosion des fantômes (Prévision 3)

Un poste est fantôme si `is_phantom_item = True` ou si le composant a `SOBSL = '50'` dans `material_plant`
[À VÉRIFIER]. Boucle bornée :

```python
MAX_PHANTOM_DEPTH = 5
for level in range(MAX_PHANTOM_DEPTH):
    # remplacer chaque ligne fantôme par la BOM (même K, même D) du composant fantôme,
    # quantités multipliées par la quantité du poste fantôme, BOM_item_number = '9999'
    ...
# AMBER : plus aucune ligne fantôme après la boucle (sinon profondeur > MAX ou BOM fantôme absente)
```
Les fantômes eux-mêmes ne sont jamais conservés (besoin métier). Utilisation et alternative de la BOM du fantôme :
même `BOM_usage`, alternative `01` **[À VÉRIFIER]**.

### 8.7 Consommations

Depuis `part_movement_exposed`, OF du périmètre seulement (`remove_leading_zeros` sur `Work_order`) :
- exclure l'entrée en stock de l'AF lui-même (`component = material_number de l'OF`, mouvements 101/102) ;
- signe : `SHKZG = 'H'` (sortie) → +, `'S'` → − si la colonne est exposée [Q11] ; sinon table de signes par type de
  mouvement en constante ;
- Conso 1 = tous les autres mouvements imputés à l'OF ;
- Conso 2 = types dans `NOMINAL_MOVEMENT_TYPES = ["261", "262"]` **[TBD : exclure la casse ; liste à valider avec Q11]** ;
- Conso 3 = **non calculée en v1** (méthode TBD) : colonne présente à NULL pour que le rapport ait sa forme finale.
- Agrégation Σ par (OF, composant) **avant** toute jointure ; quantité en unité de base [À VÉRIFIER Q11].

### 8.8 Comparaison et fiabilité

1. Prévisions (1, 2, 3) et Consos (1, 2, 3) agrégées par (période, OF, composant) → FULL OUTER JOIN sur
   (période, OF, composant) ; chaque côté RED-unique sur cette clé avant la jointure.
2. Σ par (période, `plant`, `material_number` AF, composant) — 1 ligne par composant prévu et/ou consommé.
3. Pour chaque paire (P2,C2) défaut, (P2,C1), (P1,C1), optionnel (P3,C2) :
   `error_percentage = |prev − conso| / greatest(prev, conso)` ; 0 si les deux sont 0 ; conso nette négative →
   ramenée à 0 et flag **[TBD]** ; statut `OK` / `OVERSTOCK` / `SHORTAGE`.
4. `reliability = 1 − avg(error_percentage)` par AF (et par CP / division dans le rapport : moyenne des erreurs de
   tous les composants du CP **[TBD : ou moyenne des fiabilités AF]**). Calculable en DAX `AVERAGE` sur la table
   composant, donc pas de table de synthèse supplémentaire.

### 8.9 Sorties Proj

- `uc<NNN>_bom_reliability_component` — grain (période, plant, AF, composant) : `bom_reliability_component_ID`,
  `T0_date`, `P1_months`, `P2_months`, `T1_date`, `T2_date`, `is_default_period`, `plant`, `profit_center`,
  `material_number`, `material_description`, `component_material_number`, `component_description`,
  `component_base_unit`, `forecast_1_quantity`, `forecast_2_quantity`, `forecast_3_quantity`,
  `consumption_1_quantity`, `consumption_2_quantity`, `consumption_3_quantity`,
  `error_percentage_forecast_2_consumption_2`, `error_percentage_forecast_2_consumption_1`,
  `error_percentage_forecast_1_consumption_1`, `error_percentage_forecast_3_consumption_2`,
  `reliability_status` (défaut P2/C2), `replenishment_lead_time_days`, `component_standard_price_EUR`,
  `component_classification` [TBD], `PF_usage` [TBD : définition à obtenir].
- `uc<NNN>_bom_reliability_work_order` — grain (période, OF) : compteur d'OF, `forecast_source`, quantités T0 et finale.
- Variante page 3 (optionnelle) : même table au grain (… , `BOM_item_number`) — à décider avant le build pour ne pas
  faire deux tables. Commentaires par poste : hors Databricks (write-back Power BI ou table manuelle) [TBD].

---

## 9. Tests en lab (`dev_lab.lab_jules`)

### 9.1 Paramètres de run

| Widget | Valeur en lab |
|---|---|
| `pipeline_write_env` | `dev` |
| `pipeline_read_env` | `dev` |
| `reference_read_env` | `prod` |
| `lab_target_schema` | `dev_lab.lab_jules` |
| `by_pass_quality_checks` | `false` (sauf debug explicite) |

Tables produites en lab : `dev_lab.lab_jules.bom_item_history` (+ `_exposed`),
`dev_lab.lab_jules.order_component_requirement_history` (+ `_exposed`),
`dev_lab.lab_jules.uc<NNN>_bom_reliability_component`, `dev_lab.lab_jules.uc<NNN>_bom_reliability_work_order`.
C lit A et B dans `dev_lab.lab_jules` quand `lab_target_schema` est renseigné.

### 9.2 Ordre et contrôles manuels

1. Q1–Q12 → corrections_log, mise à jour de cette spec (lever les [À VÉRIFIER]).
2. **B en premier** (le plus urgent : démarre l'horloge de l'historique), deux runs successifs le même jour → la table
   ne doit pas changer (idempotence) ; un run le lendemain → seules les lignes modifiées sont versionnées.
3. A sur un sous-ensemble (widget de debug : une usine, quelques AF) puis complet ; mesurer durée/volume.
   Contrôle : pour 3 AF, BOM reconstituée à 3 dates vs SAP CS03 (date de validité) et vs l'ancien snapshot du stack.
4. C sur la période par défaut ; golden examples de la Test Definition (OF connu, composant connu, consommation MB51
   vs Conso 2, besoin MD04/CO03 vs Prévision 1).

### 9.3 Capture en attendant la prod

Si aucun historique RESB/PLAF n'existe : planifier B **tout de suite** en job personnel quotidien écrivant dans
`dev_lab.lab_jules` (widget `lab_target_schema`), en attendant le job prod. À la mise en prod, l'historique accumulé
en lab sera repris une fois dans `prod_gold.mrp.order_component_requirement_history` (copie one-shot à faire valider
par CoreAdmin) — sinon les mois de capture lab sont perdus.

---

## 10. Jobs (DAB) — esquisse

| Job [TBD noms] | Tâches | Fréquence | Cluster |
|---|---|---|---|
| `D_2_MRP_Order_Requirement_History_Data_Asset` | B | quotidien, **après** le rafraîchissement bronze RESB/PLAF/AFKO/AFPO | S |
| `W_2_Production_BOM_History_Data_Asset` | A | hebdomadaire (dimanche, après le stack STPO du samedi) | M multi (à mesurer) |
| `W_3_<Domain>_BOM_Reliability_Project` | C (après A et B) | hebdo ou mensuel [TBD] | S/M |

Tags, `run_as`, notifications, `base_parameters` : CLAUDE.md §11. `lab_target_schema` n'est jamais passé par un job.

---

## 11. Points ouverts

| # | Sujet | Proposition par défaut | Qui |
|---|---|---|---|
| D1 | Historique RESB/PLAF existant ? | Q1/Q8 ; sinon capture immédiate | Data |
| D2 | Proxy OF pour la période avant capture (§6.7) | Accepté, flaggé `RECONSTRUCTED` | Métier |
| D3 | Normalisation volume (§8.3) | prévision × qté finale / qté T0 | Métier |
| D4 | Prévision 2 : quels rebuts retirer | composant + opération + ensemble | Métier |
| D5 | Prévision 3 : avec ou sans rebut composant ; date de validité D | avec ; D = début prévu de l'OF connu à T0 | Métier |
| D6 | Conso 2 : types de mouvement nominaux | 261/262 | Métier + Q11 |
| D7 | Conso 3 : méthode de régularisation | non calculée en v1 | Métier |
| D8 | Vrac, hors stock, conso nette négative | exclus / exclus / ramenée à 0 | Métier |
| D9 | Agrégation CP/division | moyenne des erreurs composants | Métier |
| D10 | Grille P1/P2 | {3} ; étendue {1,2,3,6} si volumétrie OK | Métier |
| D11 | Classification composant, « PF usage » | à définir | Métier |
| D12 | Utilisations de BOM (`STLAN`) | `1` | Data (Q4) |
| D13 | Lecture landing zone en Gold | dette technique déclarée | Équipe LEAP |
| D14 | MERGE Delta hors `leap_utils` | à valider | Équipe LEAP |
| D15 | Numéro de UC, domaine Proj, dossier `proj/` | — | Équipe |
| D16 | Nommage PK/contraintes (Confluence vs existant) | `{table}_ID` / `gold_{table}_PK` | Équipe (CLAUDE.md §16.9) |

---

## 12. Annexe — clés SAP et pièges

| Table | Clé SAP complète | Pièges |
|---|---|---|
| MAST | `MATNR, WERKS, STLAN, STLNR, STLAL` | une BOM (`STLNR`) peut servir plusieurs AF/usines |
| STKO | `STLTY, STLNR, STLAL, STKOZ` | versions d'en-tête par `DATUV` ; `BMENG` peut ≠ 1 |
| STAS | `STLTY, STLNR, STLAL, STLKN, STASZ` | indispensable pour les BOM multi-alternatives ; suppression = nouvel enregistrement `LKENZ = X` |
| STPO | `STLTY, STLNR, STLKN, STPOZ` | `STLNR` unique **par `STLTY`** ; versions par `DATUV` ; `MENGE` en unité `MEINS` de la BOM, signée ; `FMENG` fixe |
| RESB | `RSNUM, RSPOS, RSART` | `BDART` AR/SB ; ligne fantôme `DUMPS = X` + composants éclatés ; `BDMNG` inclut les rebuts ; `ENMNG` change pendant la prod |
| PLAF | `PLNUM` | disparaît à la conversion en OF |
| AFKO / AFPO | `AUFNR` / `AUFNR, POSNR` | `AFPO.PLNUM` = OP d'origine ; `GSTRI`/`GLTRI` dates réelles |
| MSEG | `MBLNR, MJAHR, ZEILE` | immuable ; `SHKZG` sens ; `MENGE` unité de base, `ERFMG` unité de saisie |
