# BOM Production — lineage actuel

Ce document décrit uniquement la **production actuelle** : comment `prod_silver.production.bom` et
`prod_gold.production.bom` sont construites aujourd'hui. Il ne contient ni nos travaux d'historisation ni les questions
ouvertes (voir `spec_technique_historisation_bom.md` et `questions_ouvertes.md`).

Source : notebooks de `leap-dbx-repository` (branche `main`) et job `W_3_SAP_AS_Design_BOM_DataAsset`.

---

## 1. Vue d'ensemble

```
SAP ECC6
   │  ingestion (job F_0_SAP_Ingestion_V2)
   ▼
Landing zone   {env}_landingzone.sap_latecoere_ecc6   *_stack   (historique)
   │  CTAS nocturne
   ▼
Bronze         {env}_bronze.sap_latecoere_ecc6        *_latest  (dernier état)
   │
   │  Étape 1 — as_design_BOM_bronze_to_silver      task create_BOM_silver
   ▼
Silver         {env}_silver.production.BOM          214 colonnes
   │
   │  Étape 2 — as_design_BOM_silver_to_gold        task create_BOM_gold
   │  + référentiel {REF}_gold.master_data.material_master_data_plant
   ▼
Gold           {env}_gold.production.BOM            216 colonnes
   ├── BOM_exposed   (vue, 22 colonnes)          ← lu par les consommateurs
   └── bom_qc        (table, 51 colonnes)        ← produite par un autre job (uc27)
```

## 2. Tables et job

| Objet | Type | Lignes (2026-10-13) | Colonnes | Produit par |
|---|---|---|---|---|
| `{env}_silver.production.BOM` | table | 3 230 287 | 214 | task `create_BOM_silver` |
| `{env}_gold.production.BOM` | table | 3 230 287 | 216 | task `create_BOM_gold` |
| `{env}_gold.production.BOM_exposed` | vue | — | 22 | task `create_BOM_gold` |
| `{env}_gold.production.bom_qc` | table | — | 51 | job `W_0_uc27-business_data_consistency` |

Job `W_3_SAP_AS_Design_BOM_DataAsset` :

- deux tasks enchaînées : `create_BOM_silver` puis `create_BOM_gold` ;
- code lu depuis Git, branche `main` ;
- paramètres par défaut : `pipeline_read_env = prod`, `pipeline_write_env = prod`, `reference_read_env = prod` ;
- cluster de job à un seul nœud (`singleNode`), mode `SINGLE_USER` ;
- alerte mail en cas d'échec : `LEAP_Databricks_Alert@latecoere.aero`.

---

## 3. Étape 1 — Bronze → Silver (`create_BOM_silver`)

### 3.1 Entrées

| Table | Rôle |
|---|---|
| `{REF}_bronze.sap_latecoere_ecc6.mast_latest` | lien article fabriqué ↔ BOM |
| `{REF}_bronze.sap_latecoere_ecc6.stko_latest` | en-tête de BOM (une par BOM et alternative) |
| `{REF}_bronze.sap_latecoere_ecc6.stas_latest` | allocation d'un nœud à une alternative |
| `{REF}_bronze.sap_latecoere_ecc6.stpo_latest` | postes (composants) |
| `{REF}_bronze.headers_sap.headers_sap_business_mast` / `_stko` / `_stas` | noms métier des colonnes |
| `{REF}_bronze.headers_sap.header_sap_business_stpo` | noms métier des colonnes de STPO |

### 3.2 Traitement, dans l'ordre

1. **Préfixage** : chaque table reçoit son préfixe (`MAST_`, `STKO_`, `STAS_`, `STPO_`) sur toutes ses colonnes.
2. **Renommage par header SAP** : une colonne présente dans le header prend son nom métier (`MAST_matnr` → `material_number`).
   Une colonne absente du header garde son nom préfixé (`MAST_stlan`).
3. **Typage** : `creation_date` et `last_modif` deviennent des dates ; `quantity` et `quantity_base` deviennent des nombres.
4. **Filtres** :

| Table | Filtre |
|---|---|
| MAST | usage `stlan = "1"` (production) ; `cslty` différent de `X` ou vide |
| STKO | statut BOM différent de `02` et différent de vide ; type `M` (matériel) ; **une seule version par (BOM, alternative) : la plus récente par `creation_date`** |
| STAS | type `M` ; les lignes marquées `cancel = X` retirent toutes les lignes du même (BOM, alternative, nœud) ; la colonne `cancel` est ensuite supprimée |
| STPO | type `M` ; composant non vide |

5. **Zéros de tête retirés** : sur `material_number` (MAST), `component` et `item_number` (STPO).
6. **Jointures** (toutes INNER) :

```
MAST  ⋈  STKO   sur (BOM_number, alternative)
      ⋈  STAS   sur (BOM_number, alternative)
      ⋈  STPO   sur (BOM_number, node)
```

7. **Écriture** : `overwrite` avec `overwrite_schema`, sur `{PIPELINE_WRITE_ENV}_silver.production.BOM`.

La Silver ne contient ni clé primaire ni contrôle qualité.

---

## 4. Étape 2 — Silver → Gold (`create_BOM_gold`)

### 4.1 Référentiel utilisé

`{REF}_gold.master_data.material_master_data_plant` (une ligne par article et usine) fournit :

- pour le **composant** (jointure sur `component`, `plant` et `MRP_Unit = plant` de la BOM) : `Component_Plant_Status`, `backflush`, `Storage_Location` ;
- pour l'**article parent** (jointure sur `material_number`, `plant` et `MRP_Unit = plant`) : `Material_Plant_Status`.

Les jointures sont LEFT : aucune ligne de la Silver n'est perdue. Le nombre de lignes est identique en Silver et en Gold (3 230 287).

### 4.2 Transformations

1. **Enrichissement** : ajout des colonnes du référentiel (ci-dessus).
2. **Quantité** :
   - si `alternative_item = X` : `quantity = quantity × probability / 100` ;
   - puis `quantity = quantity / quantity_base` ;
   - `quantity_base` et `alternative_item` sont ensuite supprimées.
   La Gold donne donc une quantité **par unité de l'article fabriqué**, pondérée pour les postes alternatifs.
3. **Identifiant métier** : `business_id = concat_ws("_", BOM_number, plant, item_number, material_number, alternative, node)`.
4. **Contrôles qualité (RED, avant écriture)** :
   - `creation_date` et `last_modif` de type date ;
   - `quantity` de type nombre ;
   - `plant` de 4 caractères ; `BOM_number` et `node` de 8 caractères ;
   - `business_id` unique.
5. **Écriture** : `overwrite` sur `{PIPELINE_WRITE_ENV}_gold.production.BOM`.
6. **Vue** : `BOM_exposed` = colonnes dont le nom ne commence pas par `STPO`, `MAST`, `STKO` ou `STAS`.

### 4.3 Différences Silver → Gold

| | Silver | Gold |
|---|---|---|
| Colonnes supprimées | — | `quantity_base`, `alternative_item` |
| Colonne renommée | `storage_location` | `Storage_Location` (valeur complétée par le référentiel) |
| Colonnes ajoutées | — | `Component_Plant_Status`, `backflush`, `Material_Plant_Status`, `business_id` |
| Colonne dont la valeur change | `quantity` (brute) | `quantity` (pondérée, voir 4.2) |
| Nombre de colonnes | 214 | 216 |

---

## 5. Vue `BOM_exposed` (22 colonnes)

C'est le contrat de lecture des consommateurs.

| Colonne | Type | Origine | Contenu |
|---|---|---|---|
| `BOM_number` | string | STLNR | numéro de nomenclature |
| `node` | string | STLKN | nœud du poste |
| `alternative` | string | STLAL | alternative de fabrication |
| `material_number` | string | MATNR | article fabriqué |
| `plant` | string | WERKS | usine |
| `BOM_type` | string | STLTY | type de BOM (`M`) |
| `creation_date` | date | ANDAT (en-tête) | date de création de l'en-tête |
| `BOM_status` | string | STLST | statut de la BOM |
| `last_modif` | date | AEDAT (poste) | date de dernière modification du poste |
| `component` | string | IDNRK | composant |
| `item_type` | string | POSTP | catégorie de poste |
| `item_number` | string | POSNR | numéro de poste |
| `unit` | string | MEINS | unité de saisie |
| `quantity` | double | MENGE, pondérée | quantité par unité de l'article fabriqué |
| `bulk_material` | string | SCHGT | matière en vrac |
| `probability` | string | EWAHR | probabilité d'utilisation (%) |
| `Storage_Location` | string | LGORT, complété | magasin |
| `alternative_station_group` | string | ALPGR | groupe de postes alternatifs |
| `Component_Plant_Status` | string | référentiel | statut du composant dans l'usine |
| `backflush` | string | référentiel | post-consommation |
| `Material_Plant_Status` | string | référentiel | statut de l'article parent dans l'usine |
| `business_id` | string | calculée | identifiant métier |

Ce que la vue ne contient pas : rebut, quantité de base, dates de validité, numéro de version.

---

## 6. Table `bom_qc`

Produite par le job `W_0_uc27-business_data_consistency`, qui lit `BOM_exposed`. Elle contient les 22 colonnes de la vue, puis
`quality_check_1` / `errors_1` à `quality_check_14` / `errors_14`, puis `errors_name`.

---

## 7. Annexe — liste des colonnes

### 7.1 Silver (214)

Clés : `BOM_number`, `node`, `alternative`.

**MAST** : MAST_stack_row_id, MAST_ID, MAST_extraction_timestamp, MAST_ingestion_timestamp, MAST_file_name, MAST_file_mode, MAST_table_name, MAST_file_modification_time, MAST_mandt, MAST_stlan, MAST_losvn, MAST_losbs, MAST_andat, MAST_annam, MAST_aedat, MAST_aenam, MAST_cslty, `material_number`, `plant`.

**STKO** : STKO_stack_row_id, STKO_ID, STKO_extraction_timestamp, STKO_ingestion_timestamp, STKO_file_name, STKO_file_mode, STKO_table_name, STKO_file_modification_time, STKO_mandt, BOM_type, STKO_stkoz, STKO_datuv, STKO_techv, STKO_aennr, STKO_lkenz, STKO_loekz, STKO_vgkzl, creation_date, STKO_annam, STKO_aedat, STKO_aenam, STKO_bmein, quantity_base, STKO_cadkz, STKO_labor, STKO_ltxsp, STKO_stktx, BOM_status, STKO_wrkan, STKO_dvdat, STKO_dvnam, STKO_aehlp, STKO_alekz, STKO_guidx.

**STAS** : STAS_stack_row_id, STAS_ID, STAS_extraction_timestamp, STAS_ingestion_timestamp, STAS_file_name, STAS_file_mode, STAS_table_name, STAS_file_modification_time, STAS_mandt, STAS_stasz, STAS_datuv, STAS_techv, STAS_aennr, STAS_andat, STAS_annam, STAS_aedat, STAS_aenam, STAS_dvdat, STAS_dvnam, STAS_aehlp, STAS_stvkn, STAS_idpos, STAS_idvar, STAS_lpsrt.

**STPO** : STPO_stack_row_id, STPO_ID, STPO_extraction_timestamp, STPO_ingestion_timestamp, STPO_file_name, STPO_file_mode, STPO_table_name, STPO_file_modification_time, STPO_mandt, STPO_stpoz, STPO_datuv, STPO_techv, STPO_aennr, STPO_lkenz, STPO_vgknt, STPO_vgpzl, STPO_andat, STPO_annam, last_modif, STPO_aenam, component, STPO_pswrk, item_type, item_number, STPO_sortf, unit, quantity, STPO_fmeng, STPO_ausch, STPO_avoau, STPO_netau, bulk_material, STPO_beikz, STPO_erskz, STPO_rvrel, STPO_sanfe, STPO_sanin, STPO_sanka, STPO_sanko, STPO_sanvs, STPO_stkkz, STPO_rekri, STPO_rekrs, STPO_cadpo, STPO_nfmat, STPO_nlfzt, STPO_verti, alternative_item, probability, STPO_ekgrp, STPO_lifzt, STPO_lifnr, STPO_preis, STPO_peinh, STPO_waers, STPO_sakto, STPO_roanz, STPO_roms1, STPO_roms2, STPO_roms3, STPO_romei, STPO_romen, STPO_rform, STPO_upskz, STPO_valkz, STPO_ltxsp, STPO_potx1, STPO_potx2, STPO_objty, STPO_matkl, STPO_webaz, STPO_dokar, STPO_doknr, STPO_dokvr, STPO_doktl, STPO_csstr, STPO_class, STPO_klart, STPO_potpr, STPO_awakz, STPO_inskz, STPO_vcekz, STPO_vstkz, STPO_vackz, STPO_ekorg, STPO_clobk, STPO_clmul, STPO_clalt, STPO_cview, STPO_knobj, storage_location, STPO_kzkup, STPO_intrm, STPO_tpekz, STPO_stvkn, STPO_dvdat, STPO_dvnam, STPO_dspst, STPO_alpst, STPO_alprf, alternative_station_group, STPO_kznfp, STPO_nfgrp, STPO_nfeag, STPO_kndvb, STPO_kndbz, STPO_kstty, STPO_kstnr, STPO_kstkn, STPO_kstpz, STPO_clszu, STPO_kzclb, STPO_aehlp, STPO_prvbe, STPO_nlfzv, STPO_nlfmv, STPO_idpos, STPO_idhis, STPO_idvar, STPO_alekz, STPO_itmid, STPO_guid, STPO_itsob, STPO_rfpnt, STPO_guidx, STPO_cufactor, STPO__sapmp_met_lrch, STPO__sapmp_max_fertl, STPO__sapmp_fix_as_j, STPO__sapmp_fix_as_e, STPO__sapmp_fix_as_l, STPO__sapmp_abl_zahl, STPO__sapmp_rund_fakt, STPO_funcid.

### 7.2 Gold (216)

Liste dans l'ordre de la table :

```
BOM_number, node, alternative,
MAST_stack_row_id, MAST_ID, MAST_extraction_timestamp, MAST_ingestion_timestamp, MAST_file_name,
MAST_file_mode, MAST_table_name, MAST_file_modification_time, MAST_mandt, material_number, plant,
MAST_stlan, MAST_losvn, MAST_losbs, MAST_andat, MAST_annam, MAST_aedat, MAST_aenam, MAST_cslty,
STKO_stack_row_id, STKO_ID, STKO_extraction_timestamp, STKO_ingestion_timestamp, STKO_file_name,
STKO_file_mode, STKO_table_name, STKO_file_modification_time, STKO_mandt, BOM_type, STKO_stkoz,
STKO_datuv, STKO_techv, STKO_aennr, STKO_lkenz, STKO_loekz, STKO_vgkzl, creation_date, STKO_annam,
STKO_aedat, STKO_aenam, STKO_bmein, STKO_cadkz, STKO_labor, STKO_ltxsp, STKO_stktx, BOM_status,
STKO_wrkan, STKO_dvdat, STKO_dvnam, STKO_aehlp, STKO_alekz, STKO_guidx,
STAS_stack_row_id, STAS_ID, STAS_extraction_timestamp, STAS_ingestion_timestamp, STAS_file_name,
STAS_file_mode, STAS_table_name, STAS_file_modification_time, STAS_mandt, STAS_stasz, STAS_datuv,
STAS_techv, STAS_aennr, STAS_andat, STAS_annam, STAS_aedat, STAS_aenam, STAS_dvdat, STAS_dvnam,
STAS_aehlp, STAS_stvkn, STAS_idpos, STAS_idvar, STAS_lpsrt,
STPO_stack_row_id, STPO_ID, STPO_extraction_timestamp, STPO_ingestion_timestamp, STPO_file_name,
STPO_file_mode, STPO_table_name, STPO_file_modification_time, STPO_mandt, STPO_stpoz, STPO_datuv,
STPO_techv, STPO_aennr, STPO_lkenz, STPO_vgknt, STPO_vgpzl, STPO_andat, STPO_annam, last_modif,
STPO_aenam, component, STPO_pswrk, item_type, item_number, STPO_sortf, unit, quantity, STPO_fmeng,
STPO_ausch, STPO_avoau, STPO_netau, bulk_material, STPO_beikz, STPO_erskz, STPO_rvrel, STPO_sanfe,
STPO_sanin, STPO_sanka, STPO_sanko, STPO_sanvs, STPO_stkkz, STPO_rekri, STPO_rekrs, STPO_cadpo,
STPO_nfmat, STPO_nlfzt, STPO_verti, probability, STPO_ekgrp, STPO_lifzt, STPO_lifnr, STPO_preis,
STPO_peinh, STPO_waers, STPO_sakto, STPO_roanz, STPO_roms1, STPO_roms2, STPO_roms3, STPO_romei,
STPO_romen, STPO_rform, STPO_upskz, STPO_valkz, STPO_ltxsp, STPO_potx1, STPO_potx2, STPO_objty,
STPO_matkl, STPO_webaz, STPO_dokar, STPO_doknr, STPO_dokvr, STPO_doktl, STPO_csstr, STPO_class,
STPO_klart, STPO_potpr, STPO_awakz, STPO_inskz, STPO_vcekz, STPO_vstkz, STPO_vackz, STPO_ekorg,
STPO_clobk, STPO_clmul, STPO_clalt, STPO_cview, STPO_knobj, Storage_Location, STPO_kzkup, STPO_intrm,
STPO_tpekz, STPO_stvkn, STPO_dvdat, STPO_dvnam, STPO_dspst, STPO_alpst, STPO_alprf,
alternative_station_group, STPO_kznfp, STPO_nfgrp, STPO_nfeag, STPO_kndvb, STPO_kndbz, STPO_kstty,
STPO_kstnr, STPO_kstkn, STPO_kstpz, STPO_clszu, STPO_kzclb, STPO_aehlp, STPO_prvbe, STPO_nlfzv,
STPO_nlfmv, STPO_idpos, STPO_idhis, STPO_idvar, STPO_alekz, STPO_itmid, STPO_guid, STPO_itsob,
STPO_rfpnt, STPO_guidx, STPO_cufactor, STPO__sapmp_met_lrch, STPO__sapmp_max_fertl,
STPO__sapmp_fix_as_j, STPO__sapmp_fix_as_e, STPO__sapmp_fix_as_l, STPO__sapmp_abl_zahl,
STPO__sapmp_rund_fakt, STPO_funcid,
Component_Plant_Status, backflush, Material_Plant_Status, business_id
```

