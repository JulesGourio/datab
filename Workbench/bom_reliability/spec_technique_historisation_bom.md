# Spec technique v5 — Historisation des BOM et mesure de fiabilité (BOM reliability)

| | |
|---|---|
| Statut | **v5 — état au 2026-10-13.** Remplace la v4 (archivée dans `archive/`). Lab uniquement, rien en prod. |
| Couches | Gold (2 tables d'historique, nouvelles) + Proj (fiabilité, nouvelle) |
| Tests | Toutes les sorties en **`dev_lab.lab_jules`** (CLAUDE.md §0.1) |
| Conventions | Guidelines LEAP. Les noms `prod_…` sont les noms résolus avec `REFERENCE_READ_ENV = prod` ; dans le code : `f"{REFERENCE_READ_ENV}_…"` et `f"{PIPELINE_WRITE_ENV}_…"` |

Légende : **[À CONFIRMER]** = hypothèse ou fait non vérifié dans le code ; **[DÉCISION]** = choix métier ou d'équipe ouvert.

---

## 0. Résumé

1. **Objectif.** Pour chaque article fabriqué (AF) et chaque composant, comparer la **prévision connue à T0** (BOM standard,
   besoins des OP/OF) à la **consommation réelle** des OF lancés entre T1 = T0 + P1 et terminés avant T2 = T1 + P2.
2. **Ce qui existe déjà en prod.** La BOM courante `prod_silver.production.bom` (186 → ~214 colonnes selon comptage, à confirmer)
   et sa Gold `prod_gold.production.bom` + vue `prod_gold.production.bom_exposed` (22 colonnes). Pas d'historique.
3. **Ce qui est nouveau (lab).** Deux tables d'historique reconstruites **depuis les stacks** de la landing zone, en
   **snapshots mensuels** (1er du mois), et un calcul Proj de fiabilité.
4. **Pourquoi des snapshots mensuels et pas un SCD2.** La v1 à v3 (commit `2bc7972`) décrivait un SCD2 bitemporel.
   Abandonné dans `ea82192` : T0 est toujours le 1er du mois, et chaque snapshot contient déjà la validité résolue.
5. **Point critique.** La BOM Silver officielle **ne répond pas exactement comme notre A** (§8). Il faut décider avant le
   passage sur Bitbucket si notre A s'aligne sur la Silver officielle, la complète ou la remplace (§9, D1).

---

## 1. Besoin

Pour chaque AF, mesurer la fiabilité de sa BOM :

| Mesure | Définition | Source |
|---|---|---|
| Prévision 1 | besoin brut des OP/OF connu à T0 (avec le rebut appliqué par SAP) | RESB à T0 (notebook B) |
| Prévision 2 | Prévision 1 sans le rebut composant (`ESMNG`) | RESB à T0 (notebook B) |
| Prévision 3 | BOM standard connue à T0 × quantité finale de l'OF, composants fantômes éclatés | BOM à T0 (notebook A) |
| Conso 1 | tous les mouvements imputés à l'OF, sauf l'AF lui-même et hors 101/102/122 | `part_movement_exposed` (Gold) |
| Conso 2 | mouvements nominaux 261 / 262 | idem |
| Conso 3 | Conso 2 + régularisations d'inventaire au prorata | **[DÉCISION]** méthode non définie → NULL |

Erreur d'un composant : `|prévision − conso| / max(prévision, conso)` (0 si les deux sont nuls ; conso négative ramenée
à 0). Statut (défaut P2/C2) : `OVERSTOCK` si prévision > conso, `SHORTAGE` si prévision < conso, sinon `OK`.
Fiabilité d'un AF ou d'un centre de profit : `1 − moyenne des erreurs composants` (DAX `AVERAGE`).

Période (défaut) : P1 = 6 mois, P2 = 3 mois ; option P1 = 3 mois. T0 = 1er de chaque mois, T2 jamais dans le futur.
Périmètre : OF non annulés, début réel ≥ T1, fin réelle ≤ T2.

---

## 2. Architecture

### 2.1 Schéma complet (existant + nouveau)

```
SAP ECC6  (extractions en fichiers, S3)
   │
   │  job F_0_SAP_Ingestion_V2 : COPY INTO (APPEND) + OPTIMIZE, VACUUM 40 j, jamais de DELETE
   ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│ LANDING ZONE   {env}_landingzone.sap_latecoere_ecc6                            │
│   *_stack : historique complet, 1 extraction par extraction_timestamp          │
│   stpo, stko, stas, mast, marc, marm, resb, plaf, afko, afpo, mkal             │
└───────┬───────────────────────────────────────────┬───────────────────────────┘
        │ CTAS nocturne (dernier état)               │ lecture directe (NOUVEAU)
        ▼                                            │
┌────────────────────────────────────┐              │
│ BRONZE  {env}_bronze.sap_latecoere_ecc6 │          │
│   *_latest : 1 snapshot, TABLE       │          │
│   mara_latest, makt_latest (utilisés │          │
│   par A et B)                        │          │
└───────┬────────────────────────────┘              │
        │ job W_3_SAP_AS_Design_BOM_DataAsset (id 1007135342276807, à confirmer)
        │ task create_BOM_silver : as_design_BOM_bronze_to_silver
        ▼                                            │
┌────────────────────────────────────┐              │
│ SILVER OFFICIELLE  {env}_silver.production.BOM    │   (existant, prod)
│   état courant, ~214 col., ~3,23 M lignes         │
└───────┬────────────────────────────┘              │
        │ task create_BOM_gold : as_design_BOM_silver_to_gold
        │   + {REF}_gold.master_data.material_master_data_plant
        ▼                                            │
┌────────────────────────────────────┐              │
│ GOLD OFFICIELLE  {env}_gold.production.bom        │   (existant, prod)
│   ~216 col., 3,23 M lignes, business_id unique    │
│   ├─ bom_exposed  VIEW · 22 col.                  │
│   └─ bom_qc       TABLE · job W_0_uc27-business_data_consistency (474119062548779)
└────────────────────────────────────┘              │
                                                     ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ GOLD HISTORIQUE  (NOUVEAU, lab)                                               │
│   A  {env}_gold.production.bom_item_history (+ _exposed)                      │
│      snapshot mensuel × usine × AF × usage × alternative × BOM × nœud         │
│   B  {env}_gold.mrp.order_component_requirement_history (+ _exposed)          │
│      snapshot mensuel × poste de réservation OP (SB) / OF (AR)                │
└───────────────────────┬──────────────────────────────────────────────────────┘
                        │
   Gold officielle lue par C :
   prod_gold.production.work_orders_sap_exposed · supply_chain_logistic.part_movement_exposed
   · master_data.material_plant · material_exposed · plant_master_data_latest_exposed
   · finance.profit_center_exposed
                        ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ PROJ (NOUVEAU, lab, noms provisoires)  {env}_proj.supply_chain                │
│   C  uc093_bom_reliability_component   (période × usine × AF × composant)     │
│      uc093_bom_reliability_work_order  (période × OF)                         │
└──────────────────────────────────────────────────────────────────────────────┘

Consommateurs de la BOM officielle (lecture de bom_exposed sauf mention) :
  SPS RFQ (dev)        dev_bronze.sps_rfq.rfq_* (dont rfq_bom_where_used)
  Supply Chain (dev)   dev_proj.supply_chain.current_average_bom, dev_lab.supply_chain.average_bom, average_bom_multilevel
  QC                   prod_gold.production.bom_qc
  Disaster recovery    dr_prod_gold.production.bom
  Finance (source à confirmer)  prod_gold.finance.bill_of_material (23 col., 4,97 M lignes)
```

### 2.2 Ce qui est officiel, ce qui est nouveau

| Table | Statut | Rôle |
|---|---|---|
| `{env}_landingzone.sap_latecoere_ecc6.<t>_stack` | Officiel, prod | Historique brut SAP, source de toute reconstruction |
| `{env}_bronze.sap_latecoere_ecc6.<t>_latest` | Officiel, prod | État courant seul |
| `{env}_silver.production.BOM` | Officiel, prod | BOM courante (nom en majuscules dans le code) |
| `{env}_gold.production.BOM` / `bom_exposed` | Officiel, prod | BOM Gold et vue métier |
| `{env}_gold.production.bom_qc` | Officiel, prod | Contrôles qualité (job uc27, pas dans le notebook Gold) |
| `{env}_gold.master_data.material_master_data_plant` | Officiel, prod | Référentiel article × usine (job `W_2_SAP_Master_Data_Material_Plant_Asset`, 1117849829825817) |
| `{env}_gold.master_data.material_plant` | Officiel, prod | Autre référentiel (job `D_2_SAP_Master_Data_Material_Plant-Asset`, 639869952897627) |
| `{env}_gold.production.bom_item_history` (+ `_exposed`) | **Nouveau, lab** | Historique BOM (A) |
| `{env}_gold.mrp.order_component_requirement_history` (+ `_exposed`) | **Nouveau, lab** | Historique des besoins OP/OF (B) |
| `{env}_proj.supply_chain.uc093_bom_reliability_component` / `_work_order` | **Nouveau, lab** | Fiabilité (C) |

Jobs connus : `W_3_SAP_AS_Design_BOM_DataAsset` (id 1007135342276807 selon les notes), `W_0_uc27-business_data_consistency` (474119062548779),
`F_0_SAP_Ingestion_V2` (ingestion des stacks, nom issu du journal). Planning du job Silver : hebdomadaire, dimanche
vers 01h **[À CONFIRMER dans le JSON du job]**.

### 2.3 Lineage de la BOM officielle (détail)

| Étape | Entrée | Sortie | Notebook |
|---|---|---|---|
| Silver | `{REF}_bronze.sap_latecoere_ecc6.{mast,stko,stas,stpo}_latest` + headers `{REF}_bronze.headers_sap.headers_sap_business_{mast,stko,stas}` et `header_sap_business_stpo` (sans « s », nom réel) | `{PIPELINE_WRITE_ENV}_silver.production.BOM` | `data_asset/production/as_design_bom/as_design_BOM_bronze_to_silver.py` |
| Gold | Silver + `{REF}_gold.master_data.material_master_data_plant` | `{PIPELINE_WRITE_ENV}_gold.production.BOM`, vue `BOM_exposed` | `data_asset/production/as_design_bom/as_design_BOM_silver_to_gold.py` |
| QC | `BOM_exposed` | `bom_qc` | job uc27 (notebook non lu) |

Référentiel `material_master_data_plant` : Silver = `marc_latest` + `mbew_latest` (notebook
`data_asset/master_data/create_silver_material_master_data_plant.py`) ; Gold = Silver + ~15 tables Gold et Bronze, dont
la sortie précédente (auto-dépendance pour les nulls de `PF_pourcentage_affectation`).

### 2.4 Colonnes de la BOM officielle

- **Silver (~214 colonnes, comptage manuel)** : clés `BOM_number`, `node`, `alternative`, `material_number`, `plant` ;
  champs métier renommés par le header : `component`, `item_number`, `item_type`, `unit`, `quantity`, `quantity_base`,
  `probability`, `alternative_item`, `storage_location`, `alternative_station_group`, `bulk_material`, `BOM_type`,
  `BOM_status`, `creation_date`, `last_modif` ; le reste garde le nom SAP préfixé (`MAST_*`, `STKO_*`, `STAS_*`,
  `STPO_*`, dont `STKO_datuv`, `STKO_stkoz`, `STAS_datuv`, `STAS_stasz`, `STAS_aennr`, `STPO_datuv`, `STPO_aennr`,
  `STPO_vgknt`, `STPO_lkenz`, `STPO_ausch` (string), `STPO_netau`, `STPO_fmeng`). **`STAS_lkenz` n'est pas conservé.**
  Métadonnées `*_stack_row_id`, `*_ID`, `*_extraction_timestamp`, `*_ingestion_timestamp`, `*_file_name`, `*_file_mode`,
  `*_table_name`, `*_file_modification_time`, `*_mandt` sur chaque table source.
- **Gold (~216 colonnes)** : la Silver moins `quantity_base` et `alternative_item` (supprimés après usage), `storage_location`
  renommée `Storage_Location` ; plus `Component_Plant_Status`, `backflush`, `Material_Plant_Status`, `business_id`.
- **Sémantique de `quantity` (Gold)** : `quantity = STPO.menge × (probability / 100 si alternative_item = 'X') ÷ quantity_base`.
  C'est une quantité **par unité d'AF, pondérée**, et non `STPO.MENGE` brut. Notre A garde le brut (§5.6).
- **`business_id`** = `concat_ws('_', BOM_number, plant, item_number, material_number, alternative, node)`, unique (3 230 287 / 3 230 287).
- **Vue `bom_exposed` (22 colonnes)** : toutes les colonnes ne commençant pas par `STPO`, `MAST`, `STKO` ou `STAS` :
  `BOM_number`, `node`, `alternative`, `material_number`, `plant`, `BOM_type`, `creation_date`, `BOM_status`,
  `last_modif`, `component`, `item_type`, `item_number`, `unit`, `quantity`, `bulk_material`, `probability`,
  `Storage_Location`, `alternative_station_group`, `Component_Plant_Status`, `backflush`, `Material_Plant_Status`, `business_id`.
  Pas de rebut, pas de quantité de base, pas de date de validité, pas d'indicateur de fantôme dans la vue.

---

## 3. Sources SAP (landing zone et Bronze)

### 3.1 Stacks (landing zone, historique)

| Stack | Première extraction | Dernière (2026-10-05) | Dates | Lignes | Remarque |
|---|---|---|---|---|---|
| `stpo_stack` | 2023-04-23 | 2026-10-04 | 181 (hebdo) | 2,82 Md | 7 semaines manquantes |
| `stko_stack` | 2023-04-23 | 2026-10-04 | 181 (hebdo) | 93 M | |
| `mast_stack` | 2024-06-11 | 2026-10-05 | 841 | 246 M | quotidien, 2 extractions certains jours |
| `stas_stack` | 2024-06-11 | 2026-10-05 | 834 | 15,8 Md | quotidien, 2 extractions certains jours |
| `marc_stack` | 2022-11-29 | 2026-10-05 | 630 | 485 M | FULL 592 + DELTA 44 |
| `marm_stack` | 2023-04-09 | 2026-10-05 | 612 | 230 M | FULL 184 + DELTA 500 |
| `resb_stack` | 2023-09-17 | 2026-10-05 | 787 | 59 Md | deux familles par jour (ACT, NOACT) |
| `plaf_stack` | 2022-12-11 | 2026-10-05 | 804 | 6,2 Md | |
| `afko_stack` | 2022-11-29 | 2026-10-05 | 1 101 | 5,3 Md | |
| `afpo_stack` | 2022-11-30 | 2026-10-05 | 1 105 | 5,3 Md | |
| `mkal_stack` | 2023-04-23 | 2026-10-05 | 281 | 94 M | |

Colonnes techniques : `extraction_timestamp`, `ingestion_timestamp`, `file_name`, `file_mode` (FULL / DELTA),
`stack_row_id`, `ID` (STPO, STKO). Aucune purge observée (VACUUM 40 j seulement).

### 3.2 Bronze

- `{REF}_bronze.sap_latecoere_ecc6.<t>_latest` : **TABLE**, un snapshot, recréée chaque nuit (CTAS). Pas d'historique.
- `{REF}_bronze.sap_latecoere_ecc6.<t>_stack` : **VIEW** vers la landing zone (constat Genie ; à confirmer).
- Utilisés par la chaîne : `mast`, `stko`, `stas`, `stpo`, `marc`, `marm`, `mara`, `makt`, `mbew`, `equk`, `equp`, `eord`, `t460a`, `mdma`, `ausp`.
- Headers SAP (`{REF}_bronze.headers_sap`) : `headers_sap_business_{mast,stko,stas,marc,mbew,mara,makt,mdma,equk,equp,eord,…}`
  et **`header_sap_business_stpo`** (sans « s »). Pas de `headers_sap_business_stpo`.
  Variantes `_v2` présentes pour `marc`, `mbew`, `makt`, `mara`.

### 3.3 Règles SAP utilisées

| Sujet | Règle | Source |
|---|---|---|
| Catégorie de BOM | `STLTY = 'M'` (matériel) sur STKO, STPO, STAS ; `K` (coût) partage les `STLNR` (961 cas) | corrections_log |
| Usage | `STLAN = '1'` (production), 99,97 % des liens MAST | corrections_log |
| Alternative | une BOM peut avoir plusieurs alternatives par AF ; l'alternative vient de l'OF (`AFKO.STLAL`) ou de l'OP (`PLAF.STALT`) | spec §12 v4 |
| Nœud | `STLKN` identifie un poste de façon stable ; `STAS` relie chaque nœud à une ou plusieurs alternatives | Genie, lab |
| Changement d'ingénierie (ECN) | nouveau nœud avec `AENNR` et `VGKNT` = ancien nœud ; fin de l'ancien = enregistrement `STAS` avec `LKENZ = X` ; `STPO.LKENZ` n'est jamais `X` | corrections_log |
| Validité | `DATUV` = début de validité ; aucune validité future par rapport à l'extraction | corrections_log |
| Signe SAP | nombre au format texte avec signe moins final (`1.000-`) ; 1 080 postes | corrections_log |
| Rebut | `STPO.AUSCH` (poste, 1 859 postes), `MARC.KAUSF` (article composant, 350), `MARC.AUSSS` (article AF, 32 196 — surtout des **achetés**) | corrections_log, RB1–RB4 |
| Fantôme | `MARC.SOBSL = '50'` du composant dans l'usine (≈ 17 269 articles). **`STPO.STKKZ` n'est pas l'indicateur** (test invalide) | corrections_log B11–B13 |
| Unités | `MENGE` en `MEINS` (unité de saisie) ; conversion par `MARM` (`UMREZ / UMREN`), sinon facteurs ISO (IN → M, MM → M…) | corrections_log F9 |
| RESB | deux familles par jour `SAP-RESB-F-ACT` (réservations mélangées) et `SAP-RESB-F-NOACT` (ouvertes) ; deux timestamps voisins | corrections_log 2026-10-12 |
| Besoin ouvert | `BDART` AR / SB, `XLOEK` et `KZEAR` vides, `ENMNG = 0`, `BDTER` ≤ snapshot + 13 mois | notebook B |
| Alternative PLAF | le champ s'appelle `STALT` dans PLAF, `STLAL` dans AFKO | corrections_log 2026-10-12 |

---

## 4. Calendrier et choix des extractions

- **Snapshots** : 1er de chaque mois, du premier mois suivant la première extraction STPO jusqu'au mois courant.
  Lab run 2 : 42 snapshots, de 2023-05-01 à 2026-10-01.
- **Extraction retenue** : pour chaque table et chaque snapshot K, la **dernière extraction strictement antérieure à K**
  (`ts.date() < K`). Snapshot à l'état connu la veille du 1er.
- **Rétro-datation** : avant 2024-06-11, MAST et STAS n'existent pas en stack. On prend la première extraction, flag
  `_is_backdated_link` / `_is_backdated_allocation`, et on ne garde que les liens créés avant K (`andat`).
- **MARC / MARM** : seules les extractions `FULL` sont lues.
- **RESB (B)** : une extraction par famille (ACT / NOACT) avant chaque 1er, union, dédoublonnage par réservation
  (la version la plus récente gagne). Un mois sans l'une des deux familles est écarté (log).
- **Doublons intra-jour** : MAST et STAS ont parfois deux extractions complètes le même jour → la dernière gagne.

---

## 5. Notebook A — `create_gold_bom_item_history`

**Fichier** : `data_asset/production/bom_history/create_gold_bom_item_history.py`.
**Sortie** : `{PIPELINE_WRITE_ENV}_gold.production.bom_item_history` (+ `_exposed`), partitionnée par `snapshot_date`.

### 5.1 Lecture

| Table | Source | Usage |
|---|---|---|
| `mast_stack`, `stko_stack`, `stas_stack`, `stpo_stack` | landing zone | BOM, validité |
| `marc_stack`, `marm_stack` | landing zone | phantom, rebut, unités |
| `mara_latest` | Bronze | unité de base du composant |
| `makt_latest` | Bronze | descriptions (FR puis EN) |

`material_exposed` **n'est pas utilisé** : 37 k composants (obsolètes ou remplacés) y manquent (lab run 2).

### 5.2 Règles de validité (ce que fait A)

1. **STAS** : dernier enregistrement par (BOM, alternative, nœud), tri `datuv` desc puis `stasz` desc, en ignorant les
   enregistrements créés ou valides après le snapshot ; supprimé si `LKENZ = X`.
2. **STKO** : dernier enregistrement par (BOM, alternative), tri `datuv` desc puis `stkoz` desc ; supprimé si `LKENZ = X`.
3. **STPO** : `datuv < snapshot` (ou vide) et non supprimé.
4. **MAST** : `stlan = '1'`, `werks` non vide (80 liens exclus), `andat < snapshot` pour les snapshots rétro-datés.

### 5.3 Jointures (Transformations)

| Bloc | Jointure | Clés | Type | Motif |
|---|---|---|---|---|
| Tr. 1 | STPO ⋈ STAS | snapshot, BOM_number, BOM_node | INNER | un poste sans allocation n'appartient à aucune alternative (97 postes) |
| Tr. 2 | ⋈ STKO | snapshot, BOM_number, BOM_alternative | INNER | alternative sans en-tête = incohérence SAP |
| Tr. 3 | ⋈ MAST | snapshot, BOM_number, BOM_alternative | INNER | donne AF, usine, usage ; une BOM partagée par plusieurs AF est dupliquée (grain voulu) |
| Tr. 4 | ⋈ MARC composant | snapshot, component, plant | LEFT | fantôme, rebut composant |
| Tr. 4 | ⋈ MARC AF | snapshot, material, plant | LEFT | rebut d'ensemble |
| Tr. 5 | ⋈ MARM | snapshot, component, unité | LEFT | facteur d'unité |
| Tr. 5 | ⋈ MARA | component | LEFT | unité de base |
| Tr. 5 | ⋈ facteurs ISO | unité, unité de base | LEFT (broadcast) | conversions sans MARM |
| Tr. 6 | ⋈ MAKT | material ; component | LEFT | descriptions |

### 5.4 Quantité en unité de base

```
component_quantity_in_base_unit = component_quantity × {
    1                     si component_unit = component_base_unit
    umrez / umren (MARM)  sinon, si MARM existe pour (composant, unité, snapshot)
    facteur ISO           sinon, si le couple est dans ISO_UNIT_FACTORS
    NULL                  sinon (contrôle AMBER)
}
```

### 5.5 Clé primaire

`bom_item_history_ID` = `concat_ws('-', yyyyMMdd(snapshot_date), plant, material_number, BOM_usage, BOM_alternative, BOM_number, BOM_node)`.
Exemple : `20251101-2400-XXXXXXX-1-01-00000556-00000052`. Contrainte `gold_bom_item_history_PK`, idempotente
(recherche en `lower(constraint_name)`).

### 5.6 Colonnes de sortie (35, dont 32 exposées)

| # | Colonne | Signification | Source |
|---|---|---|---|
| 1 | `bom_item_history_ID` | PK | calculée |
| 2 | `snapshot_date` | 1er du mois, clé de partition | calendrier |
| 3 | `plant` | usine de l'AF | MAST.WERKS |
| 4 | `material_number` | AF, sans zéros de tête | MAST.MATNR |
| 5 | `material_description` | libellé de l'AF (FR, sinon EN) | MAKT |
| 6 | `BOM_usage` | utilisation (`1` production) | MAST.STLAN |
| 7 | `BOM_alternative` | alternative | STLAL |
| 8 | `BOM_number` | numéro de BOM | STPO.STLNR |
| 9 | `BOM_node` | nœud du poste | STPO.STLKN |
| 10 | `BOM_item_number` | n° de poste visible | STPO.POSNR |
| 11 | `BOM_item_category` | catégorie de poste (L, N, R, T, D, Z, 0…) | STPO.POSTP |
| 12 | `component_material_number` | composant, sans zéros | STPO.IDNRK |
| 13 | `component_description` | libellé du composant | MAKT |
| 14 | `component_quantity` | quantité saisie, **signée, brute** | STPO.MENGE |
| 15 | `component_unit` | unité de saisie | STPO.MEINS |
| 16 | `component_quantity_in_base_unit` | quantité convertie (§5.4) | calculée |
| 17 | `component_base_unit` | unité de base | MARA.MEINS |
| 18 | `BOM_base_quantity` | quantité de base de l'en-tête (3,5 % des BOM ≠ 1) | STKO.BMENG |
| 19 | `BOM_base_unit` | unité de la quantité de base | STKO.BMEIN |
| 20 | `BOM_status` | statut de la BOM | STKO.STLST |
| 21 | `component_scrap_percentage` | rebut du poste | STPO.AUSCH |
| 22 | `material_component_scrap_percentage` | rebut de l'article composant | MARC.KAUSF |
| 23 | `assembly_scrap_percentage` | rebut d'ensemble de l'AF | MARC.AUSSS |
| 24 | `is_net_scrap` | rebut d'ensemble ignoré | STPO.NETAU = X |
| 25 | `is_fixed_quantity` | quantité fixe par ordre | STPO.FMENG = X |
| 26 | `is_phantom_item` | composant fantôme | MARC.SOBSL = '50' |
| 27 | `is_bulk_material` | vrac, pas de sortie sur OF | STPO.SCHGT = X |
| 28 | `item_valid_from_date` | début de validité du poste | STPO.DATUV |
| 29 | `item_change_number` | numéro de modification (ECN) | STPO.AENNR |
| 30 | `previous_BOM_node` | nœud remplacé | STPO.VGKNT |
| 31 | `lot_size_from_quantity` | taille de lot minimale | MAST.LOSVN |
| 32 | `lot_size_to_quantity` | taille de lot maximale | MAST.LOSBS |
| 33 | `_is_backdated_link` | lien rétro-daté (technique, masqué) | calculée |
| 34 | `_is_backdated_allocation` | allocation rétro-datée (technique, masqué) | calculée |
| 35 | `_BOM_extraction_timestamp` | extraction STPO utilisée (technique, masqué) | STPO |

**Vue** `bom_item_history_exposed` : colonnes sans préfixe `_` (32).

### 5.7 Contrôles

- **RED** : PK non nulle et unique ; unicité des clés de chaque table préparée (STPO, STAS, STKO, MAST, MARC, MARM, MARA, MAKT).
- **AMBER** : composant et quantité renseignés ; quantité de base renseignée ; quantité en unité de base renseignée (`mostly = 0.99`).

### 5.8 Résultats lab

| Run | Résultat |
|---|---|
| Run 1 (2026-10-06) | RED : 4 doublons de PK, dus à MARC / MARM répétés dans le fichier SAP (F5391312700300, usine 1900). Corrigé par `drop_duplicates_by_order`. |
| Run 2 (2026-10-07) | OK. 141 702 196 lignes, 42 snapshots, PK unique. Dernier snapshot (2026-10-01) : 3 688 354 lignes. Contrainte créée. Bug de casse corrigé (`lower()`). |

---

## 6. Notebook B — `create_gold_order_component_requirement_history`

**Fichier** : `data_asset/mrp/order_component_requirement_history/create_gold_order_component_requirement_history.py`.
**Sortie** : `{PIPELINE_WRITE_ENV}_gold.mrp.order_component_requirement_history` (+ `_exposed`).

- Lit `resb_stack`, `plaf_stack`, `afko_stack`, `afpo_stack` (landing zone).
- Prépare les réservations ouvertes (§3.3), une extraction par famille RESB (§4).
- Tr. 1 : réservations AR (OF) ⋈ AFKO (LEFT, snapshot + ordre) ⋈ AFPO (LEFT, snapshot + ordre, premier poste).
- Tr. 2 : réservations SB (OP) ⋈ PLAF (LEFT, snapshot + ordre planifié).
- Tr. 3 : `unionByName(allowMissingColumns=True)`.
- PK : `order_component_requirement_history_ID` = `yyyyMMdd-reservation_number-reservation_item-reservation_record_type`.
- Colonnes de sortie : 35 (liste dans le code, `OUTPUT_COLUMNS`). Quantités : `requirement_quantity` (BDMNG, avec rebut),
  `component_BOM_quantity` (ESMNG, sans rebut).
- **Statut** : première version. Le run lab a échoué une fois (`stlal` introuvable dans PLAF, corrigé par `stalt`). **Run complet
  non documenté** [À CONFIRMER].

Libellé de la section *Technical debt* corrigé : les stacks sont lues depuis la landing zone, pas depuis Gold.

---

## 7. Notebook C — `create_proj_bom_reliability` (Proj)

**Fichier** : `proj/uc093-bom_reliability/create_proj_bom_reliability.py`.
**Sorties** : `{PIPELINE_WRITE_ENV}_proj.supply_chain.uc093_bom_reliability_component` et `…_work_order`.

- Entrées : A et B (dans `PIPELINE_WRITE_ENV`) ; `work_orders_sap_exposed`, `part_movement_exposed`, `material_plant`,
  `material_exposed`, `plant_master_data_latest_exposed`, `profit_center_exposed` (dans `REFERENCE_READ_ENV`).
- T0 = snapshots communs à A et B. Périodes : (P1, P2) = (6, 3) par défaut, (3, 3) en option.
- Périmètre OF : OF non annulés, début réel ≥ T1, fin réelle ≤ T2. OF sans OP ni OF à T0 → `forecast_source = NONE`, hors fiabilité.
- Prévision 2 = besoin sans rebut composant (ESMNG) ; Prévision 1 = besoin avec rebut ; Prévision 3 = BOM standard × quantité
  finale, rebut composant du poste sinon de l'article, **sans** rebut d'ensemble (jamais appliqué aux OF, RA2).
- Fantômes éclatés niveau par niveau (≤ 5 niveaux), ligne de poste `9999`.
- Normalisation : prévisions 1 et 2 × (quantité finale / quantité à T0) (`volume_scaling_factor`).
- Conso 1 : tous les mouvements de l'OF hors 101 / 102 / 122 et hors l'AF, signe `DC_indicator` (H +, S −).
- Conso 2 : 261 / 262. Conso 3 : NULL (méthode non définie).
- Alternative de l'OF à T0 : `BOM_alternative_T0` ; repli sur la plus petite alternative si absente.

### 7.1 Résultats lab (2026-10-12, usine 2400, T0 2025-01 → 2026-04)

- 326 888 lignes composant, 303 118 lignes OF (16 s d'écriture), 0 doublon de clé.
- Période par défaut `20260101-6-3` : 11 222 composants, **fiabilité 88,6 %**. Autres périodes entre 85 % et 90 %.
- Statuts (période par défaut) : OK 6 389, SHORTAGE 2 844, OVERSTOCK 1 989.
- Totaux : Prévision 1 = 2 802 886 ; Prévision 2 = 2 585 721 ; Conso 1 = Conso 2 = 2 577 722 (écart Prévision 2 / conso 0,3 %).
- Sources de prévision à T0 : 5 271 OP, 2 OF, 4 191 ordres sans prévision (44 % hors périmètre).
- Anomalie corrigée : `profit_center_exposed` garde les zéros de tête → `remove_leading_zeros` avant jointure.

---

## 8. Comparaison avec la BOM officielle (Silver et Gold)

Notre A et la Silver officielle lisent les **mêmes** tables SAP, mais n'appliquent pas les mêmes règles. Ce tableau décrit les
différences constatées dans le code. Les effets chiffrés sont à mesurer.

| # | Sujet | Silver officielle (bronze_to_silver) | Notre A | Effet attendu |
|---|---|---|---|---|
| 1 | Source | `*_latest` (état courant) | stacks, dernière extraction avant K | A permet l'historique ; la Silver non |
| 2 | En-tête STKO | dernier `creation_date` par (BOM, alternative) | dernier `datuv` puis `stkoz` | ordre de choix différent ; à vérifier sur des cas |
| 3 | Statut STKO | exclut `02` et vide | aucun filtre | **À mesurer** : BOM inactives présentes dans A seulement |
| 4 | Type MAST | exclut `cslty = X` | aucun filtre | **À mesurer** |
| 5 | Usine MAST | garde les lignes sans usine (`werks` NULL) | exclut (80 liens) | écart de lignes |
| 6 | STAS | garde toutes les lignes non annulées (`cancel`) | dernière par (alternative, nœud), `LKENZ`, dates | doublons possibles côté Silver |
| 7 | STPO | filtre `stlty = M` et composant non nul | + `datuv < K` et non supprimé | peu d'effet attendu (0 `LKENZ = X`) |
| 8 | Quantité | `quantity` = `menge` pondéré par `probability` / `quantity_base` (Gold) | `component_quantity` brut + `BOM_base_quantity` | sémantique différente ; cf. §2.4 |

### 8.1 Volumes et doublons (constats du 2026-10-13)

- Silver : **3 230 287 lignes**. Gold : **3 230 287 lignes**, `business_id` unique : les jointures Gold ne créent ni ne suppriment de lignes.
- Silver : `count(DISTINCT plant, material_number, MAST_stlan, alternative, BOM_number, node)` = 3 229 596. Écart de **691 lignes**.
  **Correction** : `count(DISTINCT …)` ignore les lignes dont une des colonnes est NULL. L'hypothèse de « doublons de nœud avec
  item_number différent » était fausse. La requête suivante, qui cherche des groupes à plus d'une ligne sur la même clé, n'a rien
  renvoyé : c'est cohérent avec des lignes à clé NULL, qui ne se joignent jamais à elles-mêmes.
  **Hypothèse à confirmer** : les 80 liens MAST sans usine (`werks` NULL) multipliés par leurs postes, soit ≈ 691 lignes.
- Silver : `probability` numérique pour les 190 634 postes alternatifs (0 valeur non numérique) : la formule de la Gold est sûre.
- Dernier snapshot de A (2026-10-01) : 3 688 354 lignes. Silver du 2026-10-13 : 3 230 287 lignes. Les dates diffèrent ; l'écart
  n'est **pas expliqué**. Candidats : statut `02` (#3), `cslty = X` (#4), règle STAS (#6). À mesurer en comparant les deux sur la même date.

### 8.2 Requête de réconciliation à lancer

```sql
-- Lignes à clé NULL dans la Silver (hypothèse : werks NULL)
SELECT count_if(plant IS NULL) AS plant_null,
       count_if(material_number IS NULL) AS material_null,
       count_if(MAST_stlan IS NULL) AS usage_null,
       count_if(alternative IS NULL) AS alt_null,
       count_if(BOM_number IS NULL) AS bom_null,
       count_if(node IS NULL) AS node_null
FROM prod_silver.production.bom;

-- Statuts BOM dans la Silver (règle 3)
SELECT BOM_status, count(*) FROM prod_silver.production.bom GROUP BY BOM_status;
```

---

### 8.3 Constats complémentaires (2026-10-13)

- **Les 691 lignes à clé NULL sont toutes des lignes à `plant` NULL** : `plant_null = 691`, les autres colonnes de clé = 0.
  Cause : liens MAST sans usine (`werks` vide), gardés par la Silver et exclus par notre A. L'hypothèse de doublons est retirée.
- **Statuts BOM dans la Silver** (3 230 287 lignes) : `01` = 3 132 352 ; `12` = 97 238 ; `07` = 532 ; `14` = 108 ; `21` = 19 ;
  `03` = 38. Aucun `02` ni vide : la Silver les exclut. **Le sens du statut `12` (97 238 lignes, 3 %) n'est pas connu** : à
  demander au métier avant de décider si notre A l'exclut.
- **Lab : la table `dev_lab.lab_jules.bom_item_history` ne contient plus le run complet.** Au 2026-10-01 elle compte
  63 702 lignes, contre 3 688 354 dans le run 2 du 2026-10-07. Un run de debug (`debug_plant`) en mode `full` a réécrit toute la
  table. Corrigé dans le code (§8.4). Le chiffre de 3 688 354 n'est plus reproductible tant qu'un run complet n'a pas été refait.

### 8.4 Correction du code : les runs de debug n'écrasent plus la table complète

Avant : `debug_plant` ou `snapshot_from_date` en `rebuild_mode = full` écrivaient la table principale avec `mode = overwrite`,
donc un sous-ensemble remplaçait tout l'historique. Après : un run de debug écrit dans `bom_item_history_debug`
(A), `order_component_requirement_history_debug` (B) et `uc093_bom_reliability_component_debug` / `…_work_order_debug` (C).
La table principale n'est écrite que par un run sans filtre de debug.

## 9. Décisions ouvertes

| # | Décision | Options | Recommandation | Qui |
|---|---|---|---|---|
| D1 | Rapport avec la BOM officielle | (a) notre A séparée, alignée sur la Silver ; (b) dimension `snapshot_date` dans la Gold officielle ; (c) rebut et quantité de base ajoutés à `bom_exposed` | **(a)** : voir §9.1 | Équipe LEAP + chef de projet |
| D2 | Filtre statut `02` et `cslty = X` | statut : **appliqué dans A comme dans la Silver** (`02` et vide exclus, décision du 2026-10-13) ; `cslty = X` : à décider | `cslty` : à décider après mesure (#4) | Métier + data |
| D3 | MAST sans usine (80 liens) | les garder (comme la Silver) ou les exclure | à décider après mesure (§8.1) | Data |
| D4 | Sémantique de `quantity` pour Proj | brut (A) ou pondéré (Gold) | brut dans A, pondération faite dans C | Équipe Proj |
| D5 | Historique incrémental | tables bronze `*_history` par la plateforme, ou stacks lus directement | voir H1 / H2 (`questions_ouvertes.md`) | Équipe plateforme |
| D6 | Nommage de la Silver et de la Gold | `BOM` (majuscules, code actuel) ou `bom` (CLAUDE.md §8) | `bom` ; Unity Catalog ne fait pas de différence | Équipe LEAP |
| D7 | Planning du job Silver et de l'historique | hebdomadaire (journal) ou autre | confirmer dans le JSON du job | Data |
| D9 | Statut `12` (97 238 lignes dans la Silver) | gardé dans A comme dans la Silver, sens à demander au métier | à confirmer | Métier |
| D8 | Conso 3, Prévision 3 avec ou sans rebut | voir `questions_ouvertes.md` (M8, R4) | inchangé | Métier |

---

### 9.1 Pourquoi séparer (D1 = a)

| Option | Avantages | Inconvénients |
|---|---|---|
| (a) table séparée, alignée sur la Silver | aucun changement de contrat pour les 6 consommateurs ; développement et tests en lab sans risque pour la prod ; nous contrôlons notre grain (snapshot) | logique dupliquée : deux sources de vérité à maintenir |
| (b) `snapshot_date` dans la Gold officielle | une seule table | la table officielle passe de 3,2 M à ≈ 140 M lignes (42 snapshots) ; change la clé et les consommateurs (RFQ, Supply Chain, DR) ; touche un job prod qui écrase tout en `overwrite` |
| (c) colonnes ajoutées à `bom_exposed` | faible impact | la vue reste sans historique ; ne résout pas le besoin de date |

Décision : **(a)**. Garde-fous à mettre en place : (1) réconcilier le dernier snapshot de A avec la Silver après application
des mêmes filtres, et lister chaque écart ; (2) documenter le grain et les règles différentes (§8) ; (3) ne jamais écrire un run de
debug dans la table principale (§8.4).

## 10. Conformité LEAP (CLAUDE.md §16)

| Point | État |
|---|---|
| Structure notebook (sections, `#N/A`) | conforme pour A, B, C |
| Environnements en dur | aucun dans A, B, C (widgets) |
| `.union()`, `.saveAsTable()` direct, `print()` | absents (A, B, C) |
| Cast nu des quantités | absent dans A, B (`try_cast`) |
| GX inline | aucun ; contrôles groupés en `# Quality Checks` |
| Vue `_exposed` | créée (A, B) |
| Contrainte PK idempotente | créée (A) ; B à vérifier |
| Lab redirect (`lab_target_schema`) | présent |
| Dette technique dans l'en-tête | à jour (lecture landing zone, écart avec la Silver) |
| Documentation de la Silver officielle | ce document (§2, §8) |

Points **non conformes** à régler avant Bitbucket : `sys.path.insert` et le patch `persist` (lab), `sap_number` / `month_starts` /
`build_snapshot_mapping` dupliqués entre A et B (à déplacer dans `leap_utils`), nom de table en majuscules côté Silver officielle.

---

## 11. Corrections apportées à la v4

| v4 disait | v5 corrige |
|---|---|
| La BOM Silver est le seul actif BOM officiel | Une Gold officielle existe (`production.BOM`, `BOM_exposed`, `bom_qc`) |
| Job Silver | `W_3_SAP_AS_Design_BOM_DataAsset` (YAML du 2026-10-13). Le nom « W_0 » de la v5 initiale était faux. |
| Silver : 186 colonnes, 3 228 402 lignes | ~214 colonnes (comptage manuel), 3 230 287 lignes (2026-10-13) |
| `bom_qc` produit par le notebook Gold | produit par le job uc27 (`W_0_uc27-business_data_consistency`) |
| Lecture des stacks « depuis Gold » (dette) | lecture de la landing zone |
| `STKKZ` = indicateur fantôme (lineage Genie) | `MARC.SOBSL = '50'` ; `STKKZ` écarté |
| `STKO.BMEIN` = quantité | `BMENG` = quantité, `BMEIN` = unité |
| Silver : 3,2 M lignes pour 4,7 M postes, cause inconnue | écart expliqué en partie (§8) : différences de règles, pas de doublons ; cause restante à mesurer |
| Le « doublon » Silver (691 lignes) | lignes à clé NULL, à confirmer (§8.1) |
| `header_sap_business_stpo` absent / typo supposée | nom réel `header_sap_business_stpo` (sans « s ») |
| Pas de table `material_master_data_plant` | existe (Silver + Gold), alimente le lineage de la BOM Gold |
| Gold `quantity` = STPO.MENGE | Gold `quantity` = quantité pondérée par unité d'AF (§2.4) |
| Lecture de `material_exposed` pour l'unité de base | `mara_latest` (37 k composants absents de `material_exposed`) |

---

## 12. Historique des versions

| Version | Commit | Contenu |
|---|---|---|
| v1–v3 | `2bc7972` → `8020836` | SCD2 bitemporel, intervalles, clés de version (abandonné) |
| v4 | `ea82192` → `d2bb735` | snapshots mensuels, trois notebooks, lab runs 1 et 2 |
| v5 | ce document | architecture refaite, lineage officiel, comparaison avec la Silver, corrections |

Le détail des tests et des corrections est dans `corrections_log.md`.
Le détail des questions ouvertes est dans `questions_ouvertes.md`.
Le texte de la v4 est dans `archive/spec_technique_historisation_bom_v4.md`.

## Annexe —  Mise à jour de l'expression de besoin (2026-10-12)

- Période : T1 = T0 + 6 mois et T2 = T1 + 3 mois par défaut ; seconde option T1 = T0 + 3 mois, T2 = T1 + 3 mois ; T2 jamais dans le futur. La variation libre de T1 / T2 reste un bonus non livré (snapshots mensuels).
- Fiabilité par défaut : Prévision 2 contre Conso 2 ; optionnel Prévision 3 contre Conso 2. Page 2 : erreur par composant pour Prév. 2 / Conso 2, Prév. 2 / Conso 1, Prév. 1 / Conso 1, statut ok / surstock / manquant, prévisions 1-2-3, conso 1-2-3, unité, durée d'approvisionnement, prix, classification, « PF usage », nombre d'OF pertinents, description division et centre de profit.
- Page 3 (optionnelle) : composants par poste de nomenclature, poste `9999` pour les composants issus d'un fantôme ; commentaires par poste.
- Tendance « d'une semaine à l'autre » citée dans les cas d'usage : le pas des snapshots est mensuel pour l'instant (décision 2026-10-12).
- Non couverts : colonne division (source à identifier), « PF usage » (définition à obtenir), Conso 3 (méthode TBD), famille / classe / code 0 ou L (TBD).
