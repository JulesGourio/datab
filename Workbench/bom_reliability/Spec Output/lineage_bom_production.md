# Lineage BOM Production — référence complète

| | |
|---|---|
| Statut | Document de référence, état au 2026-10-13. Remplace le lineage produit par Genie (corrigé, voir §12). |
| Périmètre | BOM matériel de production : tables SAP `MAST`, `STKO`, `STAS`, `STPO` de la landing zone jusqu'à la vue `bom_exposed` et aux consommateurs. |
| Sources | Notebooks Silver et Gold (`leap-dbx-repository`, branche `main`), `DESCRIBE` des tables du 2026-10-13, lineage Genie corrigé. |
| Non lu | Notebook qui produit `bom_qc` (job uc27), JSON du job `1007135342276807`, notebook `material_master_data_plant` complet. Voir §11. |

Légende : **[À CONFIRMER]** = non vérifié dans le code ou les données ; **sens SAP standard** = libellé SAP usuel, non confirmé
pour Latécoère.

---

## 1. Schéma global

```
SAP ECC6  (extractions en fichiers)
   │  job F_0_SAP_Ingestion_V2 : COPY INTO (append) + OPTIMIZE, VACUUM 40 j, aucun DELETE
   ▼
LANDING ZONE   {env}_landingzone.sap_latecoere_ecc6
   mast_stack · stko_stack · stas_stack · stpo_stack  (historique, une extraction par extraction_timestamp)
   │
   │ CTAS nocturne (dernier état seulement)
   ▼
BRONZE         {env}_bronze.sap_latecoere_ecc6
   mast_latest · stko_latest · stas_latest · stpo_latest  (TABLES, un snapshot)
   mast_stack … stpo_stack (VIEWS vers la landing zone, constat Genie [À CONFIRMER])
   │
   │ job W_0_AS_Design_BOM_DataAsset (id 1007135342276807), task create_BOM_silver
   │ notebook as_design_BOM_bronze_to_silver
   ▼
SILVER         {env}_silver.production.BOM          ~214 colonnes · 3 230 287 lignes (2026-10-13)
   │
   │ même job, task create_BOM_gold
   │ notebook as_design_BOM_silver_to_gold
   │ + {REF}_gold.master_data.material_master_data_plant
   ▼
GOLD           {env}_gold.production.BOM            216 colonnes · 3 230 287 lignes · business_id unique
   ├── {env}_gold.production.BOM_exposed    VUE · 22 colonnes
   └── {env}_gold.production.bom_qc         TABLE · produite par le job W_0_uc27-business_data_consistency
                                            (474119062548779), qui lit BOM_exposed

Consommateurs (lecture de BOM_exposed sauf mention) :
   SPS RFQ (dev)        dev_bronze.sps_rfq.rfq_* (dont rfq_bom_where_used)
   Supply Chain (dev)   dev_proj.supply_chain.current_average_bom, dev_lab.supply_chain.average_bom, average_bom_multilevel
   QC                   prod_gold.production.bom_qc
   Disaster recovery    dr_prod_gold.production.bom  (lit la table, pas la vue)
   Finance              prod_gold.finance.bill_of_material   (23 colonnes, 4,97 M lignes, source [À CONFIRMER])
```

---

## 2. Jobs et notebooks

| Élément | Valeur | Source |
|---|---|---|
| Job | `W_0_AS_Design_BOM_DataAsset` | confirmé par l'utilisateur (nom) |
| Id du job | `1007135342276807` | confirmé (Genie et utilisateur) |
| Tasks | `create_BOM_silver` puis `create_BOM_gold` | Genie, cohérent avec les notebooks |
| Notebook Silver | `data_asset/production/as_design_bom/as_design_BOM_bronze_to_silver.py` | repo |
| Notebook Gold | `data_asset/production/as_design_bom/as_design_BOM_silver_to_gold.py` (crée la table, la vue) | repo |
| Notebook Silver matériel (référentiel) | `data_asset/master_data/create_silver_material_master_data_plant.py` | repo |
| Notebook Gold matériel (référentiel) | `data_asset/master_data/create_gold_material_master_data_plant.py` | repo |
| Job matériel Gold | `W_2_SAP_Master_Data_Material_Plant_Asset` (1117849829825817) | utilisateur |
| Autre référentiel | `material_plant` : job `D_2_SAP_Master_Data_Material_Plant-Asset` (639869952897627), notebooks `create_silver_material_plant` et `create_gold_material_plant`, V2 dans `material_plant_V2/` | utilisateur |
| Job qualité | `W_0_uc27-business_data_consistency` (474119062548779), lit `BOM_exposed`, produit `bom_qc` | utilisateur |
| Fichier de configuration du bundle | `databricks.yml` et `resources/*.yml` à la racine de `leap-dbx-repository` | [À CONFIRMER] (non lu) |
| Planning du job | hebdomadaire, dimanche vers 01h | journal de session [À CONFIRMER dans le JSON du job] |

Paramètres de chaque notebook (widgets) : `pipeline_write_env` (défaut `dev`), `pipeline_read_env` (défaut `dev`),
`reference_read_env` (défaut `prod`) pour la BOM ; `READ_ENV` (défaut `prod`) et `WRITE_ENV` (défaut `dev`) pour le référentiel.
La valeur réelle passée par le job en prod est **[À CONFIRMER]**.

---

## 3. Tables sources SAP

### 3.1 Landing zone (historique, lu par notre A et B)

| Table | Clés | Rôle |
|---|---|---|
| `mast_stack` | `MATNR`, `WERKS`, `STLAN`, `STLNR`, `STLAL` | lien article fabriqué ↔ BOM |
| `stko_stack` | `STLTY`, `STLNR`, `STLAL`, `STKOZ` | en-tête de BOM (versions par `DATUV`) |
| `stas_stack` | `STLTY`, `STLNR`, `STLAL`, `STLKN`, `STASZ` | allocation d'un nœud à une alternative (fin de vie d'un nœud = `LKENZ = X`) |
| `stpo_stack` | `STLTY`, `STLNR`, `STLKN`, `STPOZ` | postes (composants) |

### 3.2 Bronze (lu par la Silver officielle)

Les tables `*_latest` ont les mêmes noms de colonnes SAP en minuscules (`matnr`, `stlnr`, `stlkn`…) plus les colonnes
de métadonnées du stack (`stack_row_id`, `ID`, `extraction_timestamp`, `ingestion_timestamp`, `file_name`, `file_mode`,
`table_name`, `file_modification_time`, `mandt`). Confirmé par le préfixage de la Silver (§5.1).

### 3.3 Headers (renommage SAP → noms métier)

| Table de header | Utilisée pour | Note |
|---|---|---|
| `{REF}_bronze.headers_sap.headers_sap_business_mast` | MAST | existe |
| `{REF}_bronze.headers_sap.headers_sap_business_stko` | STKO | existe |
| `{REF}_bronze.headers_sap.headers_sap_business_stas` | STAS | existe |
| `{REF}_bronze.headers_sap.header_sap_business_stpo` | STPO | **sans « s »** : le nom réel. `headers_sap_business_stpo` n'existe pas. |
| `prod_bronze.headers_sap.headers_sap_business_marc` / `_mbew` | référentiel matériel | existent, `prod_` en dur dans le notebook (§9) |

Colonnes du header : `Table_name`, `Field`, `Business_name`, `Format` (contrôlées par `check_schema_of_the_header_file`).

---

## 4. Préparation de la Silver (`as_design_BOM_bronze_to_silver`)

Chaque table suit la même séquence : `add_prefix_to_columns(df, "<TABLE>")` → `rename_columns_with_sap_business_header` →
`cast_columns_with_sap_business_headers`. **Le préfixe est ajouté avant le renommage** : une colonne sans entrée dans le
header garde son nom `<TABLE>_<champ SAP>` (ex. `MAST_stlan`), une colonne présente dans le header prend son nom métier
(ex. `MAST_matnr` → `material_number`).

| Table | Filtres | Autres traitements | Lignes |
|---|---|---|---|
| MAST | `stlan = "1"` ; `cslty ≠ "X"` ou NULL | zéros retirés sur `material_number` | un lien par (AF, usine, alternative) |
| STKO | `BOM_status ≠ "02"` ; `BOM_status ≠ ""` ; `BOM_type = "M"` | une ligne par (`BOM_number`, `alternative`), **la plus récente par `creation_date`** (fenêtre `row_number`) | une en-tête par alternative |
| STAS | `BOM_type = "M"` | lignes `cancel = "X"` retirent toutes les lignes du même (`BOM_number`, `alternative`, `node`) ; `cancel` supprimée | toutes les allocations non annulées, **sans** dernière version par nœud |
| STPO | `BOM_type = "M"` ; `component` non NULL | zéros retirés sur `component` et `item_number` ; `BOM_type` supprimée | un poste par ligne |

Jointures (INNER, `bronze_to_silver` L323–339) :

```
MAST  ⋈ STKO   sur (BOM_number, alternative)
      ⋈ STAS   sur (BOM_number, alternative)
      ⋈ STPO   sur (BOM_number, node)
```

Conversions finales : `last_modif` et `creation_date` (`to_date`, format `yyyyMMdd`) ; `quantity` et `quantity_base` (`cast("double")`).
Écriture : `table_utils.save_table(..., mode="overwrite", overwrite_schema=True)` sur `{PIPELINE_WRITE_ENV}_silver.production.BOM`.

Aucune clé primaire et aucun contrôle qualité dans la Silver.

---

## 5. Silver : `{env}_silver.production.BOM`

Résultat de `DESCRIBE TABLE` du 2026-10-13 : **214 colonnes** (3 clés + 19 + 34 + 24 + 134), 3 230 287 lignes.
Types : `bigint` pour `*_stack_row_id`, `date` pour `creation_date` et `last_modif`, `double` pour `quantity` et `quantity_base`,
`timestamp` pour les `*_timestamp` et `*_file_modification_time`, `string` pour le reste.

### 5.1 Clés et colonnes d'identification (3)

| Colonne | Type | Origine | Sens |
|---|---|---|---|
| `BOM_number` | string | `STLNR` (MAST, STKO, STAS, STPO) | numéro de nomenclature |
| `node` | string | `STLKN` (STAS, STPO) | nœud du poste dans la BOM |
| `alternative` | string | `STLAL` (MAST, STKO, STAS) | alternative de fabrication |

### 5.2 Colonnes MAST (19 : 9 métadonnées + 10 champs SAP, dont 2 renommés)

| Colonne | Type | Origine | Sens |
|---|---|---|---|
| `MAST_stack_row_id` | bigint | identity du stack | identifiant technique de la ligne (`GENERATED ALWAYS AS IDENTITY`) |
| `MAST_ID` | string | `ID` | identifiant d'extraction |
| `MAST_extraction_timestamp` | timestamp | stack | horodatage de l'extraction SAP |
| `MAST_ingestion_timestamp` | timestamp | stack | horodatage d'ingestion |
| `MAST_file_name` | string | stack | fichier source |
| `MAST_file_mode` | string | stack | `FULL` ou `DELTA` |
| `MAST_table_name` | string | stack | table SAP d'origine |
| `MAST_file_modification_time` | timestamp | stack | date de modification du fichier |
| `MAST_mandt` | string | `MANDT` | mandant SAP |
| `material_number` | string | `MATNR` | article fabriqué (AF), sans zéros de tête |
| `plant` | string | `WERKS` | usine de fabrication |
| `MAST_stlan` | string | `STLAN` | utilisation de la BOM (`1` = production) |
| `MAST_losvn` | string | `LOSVN` | taille de lot minimale |
| `MAST_losbs` | string | `LOSBS` | taille de lot maximale |
| `MAST_andat` | string | `ANDAT` | date de création du lien (yyyyMMdd) |
| `MAST_annam` | string | `ANNAM` | créé par |
| `MAST_aedat` | string | `AEDAT` | date de dernière modification |
| `MAST_aenam` | string | `AENAM` | modifié par |
| `MAST_cslty` | string | `CSLTY` | indicateur de configuration ; `X` exclu par la Silver (sens exact [À CONFIRMER]) |

### 5.3 Colonnes STKO (en-tête, 34 : 9 métadonnées + 25 champs SAP, dont 4 renommés)

| Colonne | Type | Origine | Sens |
|---|---|---|---|
| `STKO_stack_row_id` | bigint | identity | identifiant technique |
| `STKO_ID` | string | `ID` | identifiant d'extraction |
| `STKO_extraction_timestamp` | timestamp | stack | horodatage d'extraction |
| `STKO_ingestion_timestamp` | timestamp | stack | horodatage d'ingestion |
| `STKO_file_name` | string | stack | fichier source |
| `STKO_file_mode` | string | stack | `FULL` / `DELTA` |
| `STKO_table_name` | string | stack | table SAP |
| `STKO_file_modification_time` | timestamp | stack | date de modification du fichier |
| `STKO_mandt` | string | `MANDT` | mandant |
| `BOM_type` | string | `STLTY` | catégorie : `M` matériel (filtre), `K` coût, `S` |
| `STKO_stkoz` | string | `STKOZ` | compteur de version d'en-tête |
| `STKO_datuv` | string | `DATUV` | début de validité (yyyyMMdd) |
| `STKO_techv` | string | `TECHV` | version technique |
| `STKO_aennr` | string | `AENNR` | numéro de modification (ECN) |
| `STKO_lkenz` | string | `LKENZ` | indicateur de suppression |
| `STKO_loekz` | string | `LOEKZ` | indicateur de verrouillage |
| `STKO_vgkzl` | string | `VGKZL` | sens [À CONFIRMER] |
| `creation_date` | date | `ANDAT` | date de création de l'en-tête (typée) |
| `STKO_annam` | string | `ANNAM` | créé par |
| `STKO_aedat` | string | `AEDAT` | date de dernière modification |
| `STKO_aenam` | string | `AENAM` | modifié par |
| `STKO_bmein` | string | `BMEIN` | unité de la quantité de base |
| `quantity_base` | double | `BMENG` | quantité de base de la BOM (≠ 1 pour 3,5 % des BOM) |
| `STKO_cadkz` | string | `CADKZ` | indicateur CAD |
| `STKO_labor` | string | `LABOR` | laboratoire / service |
| `STKO_ltxsp` | string | `LTXSP` | langue du texte |
| `STKO_stktx` | string | `STKTX` | texte descriptif de la BOM |
| `BOM_status` | string | `STLST` | statut de la BOM (`02` exclu par la Silver) |
| `STKO_wrkan` | string | `WRKAN` | [À CONFIRMER] |
| `STKO_dvdat` | string | `DVDAT` | [À CONFIRMER] |
| `STKO_dvnam` | string | `DVNAM` | [À CONFIRMER] |
| `STKO_aehlp` | string | `AEHLP` | [À CONFIRMER] |
| `STKO_alekz` | string | `ALEKZ` | [À CONFIRMER] |
| `STKO_guidx` | string | `GUIDX` | identifiant SAP global |

### 5.4 Colonnes STAS (allocation nœud ↔ alternative, 24 : 9 métadonnées + 15 champs SAP)

| Colonne | Type | Origine | Sens |
|---|---|---|---|
| `STAS_stack_row_id` | bigint | identity | identifiant technique |
| `STAS_ID` | string | `ID` | identifiant d'extraction |
| `STAS_extraction_timestamp` | timestamp | stack | horodatage d'extraction |
| `STAS_ingestion_timestamp` | timestamp | stack | horodatage d'ingestion |
| `STAS_file_name` | string | stack | fichier source |
| `STAS_file_mode` | string | stack | `FULL` / `DELTA` |
| `STAS_table_name` | string | stack | table SAP |
| `STAS_file_modification_time` | timestamp | stack | date de modification du fichier |
| `STAS_mandt` | string | `MANDT` | mandant |
| `STAS_stasz` | string | `STASZ` | compteur d'allocation (version) |
| `STAS_datuv` | string | `DATUV` | début de validité |
| `STAS_techv` | string | `TECHV` | version technique |
| `STAS_aennr` | string | `AENNR` | numéro de modification (ECN) |
| `STAS_andat` | string | `ANDAT` | date de création de l'allocation |
| `STAS_annam` | string | `ANNAM` | créé par |
| `STAS_aedat` | string | `AEDAT` | date de dernière modification |
| `STAS_aenam` | string | `AENAM` | modifié par |
| `STAS_dvdat` | string | `DVDAT` | [À CONFIRMER] |
| `STAS_dvnam` | string | `DVNAM` | [À CONFIRMER] |
| `STAS_aehlp` | string | `AEHLP` | [À CONFIRMER] |
| `STAS_stvkn` | string | `STVKN` | nœud de sous-ensemble [À CONFIRMER] |
| `STAS_idpos` | string | `IDPOS` | identifiant de position [À CONFIRMER] |
| `STAS_idvar` | string | `IDVAR` | identifiant de variante [À CONFIRMER] |
| `STAS_lpsrt` | string | `LPSRT` | tri de position [À CONFIRMER] |

**Absente** : `STAS_lkenz`. Le flag de fin d'allocation est lu par la Silver sous le nom `cancel`, utilisé pour retirer les lignes,
puis supprimé. Une fois la Silver écrite, la fin d'un nœud n'est plus visible.

### 5.5 Colonnes STPO (postes, 134 : 9 métadonnées + 125 champs SAP, dont 12 renommés)

| Colonne | Type | Origine | Sens |
|---|---|---|---|
| `STPO_stack_row_id` | bigint | identity | identifiant technique |
| `STPO_ID` | string | `ID` | identifiant d'extraction |
| `STPO_extraction_timestamp` | timestamp | stack | horodatage d'extraction |
| `STPO_ingestion_timestamp` | timestamp | stack | horodatage d'ingestion |
| `STPO_file_name` | string | stack | fichier source |
| `STPO_file_mode` | string | stack | `FULL` / `DELTA` |
| `STPO_table_name` | string | stack | table SAP |
| `STPO_file_modification_time` | timestamp | stack | date de modification du fichier |
| `STPO_mandt` | string | `MANDT` | mandant |
| `STPO_stpoz` | string | `STPOZ` | compteur du poste (un seul par nœud chez Latécoère, lab) |
| `STPO_datuv` | string | `DATUV` | début de validité du poste |
| `STPO_techv` | string | `TECHV` | version technique |
| `STPO_aennr` | string | `AENNR` | numéro de modification (ECN) |
| `STPO_lkenz` | string | `LKENZ` | indicateur de suppression (0 valeur `X` dans les données) |
| `STPO_vgknt` | string | `VGKNT` | nœud précédent (lien ECN) |
| `STPO_vgpzl` | string | `VGPZL` | poste précédent (lien ECN) |
| `STPO_andat` | string | `ANDAT` | date de création |
| `STPO_annam` | string | `ANNAM` | créé par |
| `last_modif` | date | `AEDAT` | date de dernière modification du poste (typée) |
| `STPO_aenam` | string | `AENAM` | modifié par |
| `component` | string | `IDNRK` | composant, sans zéros de tête |
| `STPO_pswrk` | string | `PSWRK` | usine d'approvisionnement |
| `item_type` | string | `POSTP` | catégorie de poste (L stock, N hors stock, R taille variable, T texte, D document ; Z et 0 observés) |
| `item_number` | string | `POSNR` | numéro de poste visible, sans zéros de tête |
| `STPO_sortf` | string | `SORTF` | clé de tri |
| `unit` | string | `MEINS` | unité de saisie |
| `quantity` | double | `MENGE` | quantité saisie, signée |
| `STPO_fmeng` | string | `FMENG` | quantité fixe par ordre (indicateur `X`) |
| `STPO_ausch` | string | `AUSCH` | rebut du poste (%), **en string** dans la Silver |
| `STPO_avoau` | string | `AVOAU` | rebut opération (5 postes) |
| `STPO_netau` | string | `NETAU` | rebut d'ensemble ignoré (indicateur `X`) |
| `bulk_material` | string | `SCHGT` | matière en vrac (indicateur) |
| `STPO_beikz` | string | `BEIKZ` | indicateur [À CONFIRMER] |
| `STPO_erskz` | string | `ERSKZ` | indicateur [À CONFIRMER] |
| `STPO_rvrel` | string | `RVREL` | pertinence pour la réservation (sens SAP standard) |
| `STPO_sanfe` | string | `SANFE` | sens [À CONFIRMER] |
| `STPO_sanin` | string | `SANIN` | sens [À CONFIRMER] |
| `STPO_sanka` | string | `SANKA` | sens [À CONFIRMER] |
| `STPO_sanko` | string | `SANKO` | sens [À CONFIRMER] |
| `STPO_sanvs` | string | `SANVS` | sens [À CONFIRMER] |
| `STPO_stkkz` | string | `STKKZ` | indicateur de sous-ensemble. **Ce n'est pas l'indicateur fantôme** (test invalide, journal B11–B13). |
| `STPO_rekri` | string | `REKRI` | [À CONFIRMER] |
| `STPO_rekrs` | string | `REKRS` | [À CONFIRMER] |
| `STPO_cadpo` | string | `CADPO` | [À CONFIRMER] |
| `STPO_nfmat` | string | `NFMAT` | [À CONFIRMER] |
| `STPO_nlfzt` | string | `NLFZT` | [À CONFIRMER] |
| `STPO_verti` | string | `VERTI` | [À CONFIRMER] |
| `alternative_item` | string | `ALPOS` | indicateur de poste alternatif (`X`) [À CONFIRMER] ; utilisé par la Gold |
| `probability` | string | `EWAHR` | probabilité d'utilisation en % (postes alternatifs) ; **en string** |
| `STPO_ekgrp` | string | `EKGRP` | groupe d'acheteurs |
| `STPO_lifzt` | string | `LIFZT` | délai de livraison (fournisseur) |
| `STPO_lifnr` | string | `LIFNR` | fournisseur préféré |
| `STPO_preis` | string | `PREIS` | prix |
| `STPO_peinh` | string | `PEINH` | unité de prix |
| `STPO_waers` | string | `WAERS` | devise |
| `STPO_sakto` | string | `SAKTO` | compte de grand livre |
| `STPO_roanz` | string | `ROANZ` | [À CONFIRMER] |
| `STPO_roms1` | string | `ROMS1` | [À CONFIRMER] |
| `STPO_roms2` | string | `ROMS2` | [À CONFIRMER] |
| `STPO_roms3` | string | `ROMS3` | [À CONFIRMER] |
| `STPO_romei` | string | `ROMEI` | [À CONFIRMER] |
| `STPO_romen` | string | `ROMEN` | [À CONFIRMER] |
| `STPO_rform` | string | `RFORM` | [À CONFIRMER] |
| `STPO_upskz` | string | `UPSKZ` | [À CONFIRMER] |
| `STPO_valkz` | string | `VALKZ` | [À CONFIRMER] |
| `STPO_ltxsp` | string | `LTXSP` | langue du texte |
| `STPO_potx1` | string | `POTX1` | texte du poste, ligne 1 |
| `STPO_potx2` | string | `POTX2` | texte du poste, ligne 2 |
| `STPO_objty` | string | `OBJTY` | type d'objet (classification) |
| `STPO_matkl` | string | `MATKL` | groupe de marchandises |
| `STPO_webaz` | string | `WEBAZ` | délai (jours) [À CONFIRMER] |
| `STPO_dokar` | string | `DOKAR` | type de document |
| `STPO_doknr` | string | `DOKNR` | numéro de document |
| `STPO_dokvr` | string | `DOKVR` | version du document |
| `STPO_doktl` | string | `DOKTL` | partie du document |
| `STPO_csstr` | string | `CSSTR` | [À CONFIRMER] |
| `STPO_class` | string | `CLASS` | classe (caractéristiques) |
| `STPO_klart` | string | `KLART` | type de classe |
| `STPO_potpr` | string | `POTPR` | [À CONFIRMER] |
| `STPO_awakz` | string | `AWAKZ` | [À CONFIRMER] |
| `STPO_inskz` | string | `INSKZ` | [À CONFIRMER] |
| `STPO_vcekz` | string | `VCEKZ` | [À CONFIRMER] |
| `STPO_vstkz` | string | `VSTKZ` | [À CONFIRMER] |
| `STPO_vackz` | string | `VACKZ` | [À CONFIRMER] |
| `STPO_ekorg` | string | `EKORG` | organisation d'achats |
| `STPO_clobk` | string | `CLOBK` | [À CONFIRMER] |
| `STPO_clmul` | string | `CLMUL` | [À CONFIRMER] |
| `STPO_clalt` | string | `CLALT` | [À CONFIRMER] |
| `STPO_cview` | string | `CVIEW` | [À CONFIRMER] |
| `STPO_knobj` | string | `KNOBJ` | objet de configuration [À CONFIRMER] |
| `storage_location` | string | `LGORT` | magasin de sortie du composant |
| `STPO_kzkup` | string | `KZKUP` | [À CONFIRMER] |
| `STPO_intrm` | string | `INTRM` | [À CONFIRMER] |
| `STPO_tpekz` | string | `TPEKZ` | [À CONFIRMER] |
| `STPO_stvkn` | string | `STVKN` | nœud de sous-ensemble [À CONFIRMER] |
| `STPO_dvdat` | string | `DVDAT` | [À CONFIRMER] |
| `STPO_dvnam` | string | `DVNAM` | [À CONFIRMER] |
| `STPO_dspst` | string | `DSPST` | [À CONFIRMER] |
| `STPO_alpst` | string | `ALPST` | groupe de postes alternatifs [À CONFIRMER] |
| `STPO_alprf` | string | `ALPRF` | [À CONFIRMER] |
| `alternative_station_group` | string | `ALPGR` | groupe de postes alternatifs |
| `STPO_kznfp` | string | `KZNFP` | [À CONFIRMER] |
| `STPO_nfgrp` | string | `NFGRP` | [À CONFIRMER] |
| `STPO_nfeag` | string | `NFEAG` | [À CONFIRMER] |
| `STPO_kndvb` | string | `KNDVB` | [À CONFIRMER] |
| `STPO_kndbz` | string | `KNDBZ` | [À CONFIRMER] |
| `STPO_kstty` | string | `KSTTY` | type d'affectation de coût [À CONFIRMER] |
| `STPO_kstnr` | string | `KSTNR` | numéro d'affectation de coût [À CONFIRMER] |
| `STPO_kstkn` | string | `KSTKN` | [À CONFIRMER] |
| `STPO_kstpz` | string | `KSTPZ` | [À CONFIRMER] |
| `STPO_clszu` | string | `CLSZU` | [À CONFIRMER] |
| `STPO_kzclb` | string | `KZCLB` | [À CONFIRMER] |
| `STPO_aehlp` | string | `AEHLP` | [À CONFIRMER] |
| `STPO_prvbe` | string | `PRVBE` | zone d'approvisionnement |
| `STPO_nlfzv` | string | `NLFZV` | [À CONFIRMER] |
| `STPO_nlfmv` | string | `NLFMV` | [À CONFIRMER] |
| `STPO_idpos` | string | `IDPOS` | [À CONFIRMER] |
| `STPO_idhis` | string | `IDHIS` | [À CONFIRMER] |
| `STPO_idvar` | string | `IDVAR` | [À CONFIRMER] |
| `STPO_alekz` | string | `ALEKZ` | [À CONFIRMER] |
| `STPO_itmid` | string | `ITMID` | [À CONFIRMER] |
| `STPO_guid` | string | `GUID` | identifiant SAP global |
| `STPO_itsob` | string | `ITSOB` | type de traitement de l'article (sens SAP standard) |
| `STPO_rfpnt` | string | `RFPNT` | [À CONFIRMER] |
| `STPO_guidx` | string | `GUIDX` | identifiant SAP global |
| `STPO_cufactor` | string | `CUFACTOR` | facteur de configuration [À CONFIRMER] |
| `STPO__sapmp_met_lrch` | string | `_SAPMP_MET_LRCH` | champ de planification (méthode de lot) [À CONFIRMER] |
| `STPO__sapmp_max_fertl` | string | `_SAPMP_MAX_FERTL` | champ de planification [À CONFIRMER] |
| `STPO__sapmp_fix_as_j` | string | `_SAPMP_FIX_AS_J` | champ de planification [À CONFIRMER] |
| `STPO__sapmp_fix_as_e` | string | `_SAPMP_FIX_AS_E` | champ de planification [À CONFIRMER] |
| `STPO__sapmp_fix_as_l` | string | `_SAPMP_FIX_AS_L` | champ de planification [À CONFIRMER] |
| `STPO__sapmp_abl_zahl` | string | `_SAPMP_ABL_ZAHL` | champ de planification [À CONFIRMER] |
| `STPO__sapmp_rund_fakt` | string | `_SAPMP_RUND_FAKT` | champ de planification [À CONFIRMER] |
| `STPO_funcid` | string | `FUNCID` | identifiant fonctionnel |

> Les champs marqués **[À CONFIRMER]** viennent de la table `stpo_stack` sans règle qui les utilise dans le code. Ils sont
> conservés tels quels par la Silver (aucun `select` ni `drop` en Silver, CLAUDE.md §4).

---

## 6. Gold : `{env}_gold.production.BOM`

216 colonnes = Silver (214) **− `quantity_base` − `alternative_item`** (supprimées après usage) **+ 4 colonnes d'enrichissement**,
et `storage_location` devient `Storage_Location`.

### 6.1 Transformations (code `as_design_BOM_silver_to_gold`)

1. **Référentiel composant** (lignes 109–129) : `{REF}_gold.master_data.material_master_data_plant` renommé :
   `Material_Number` → `component`, `MARC_rgekz` → `backflush`, `Storage_Location` → `Storage_Location_MD`,
   `Material_Plant_Status` → `Component_Plant_Status`. Colonnes gardées : `component`, `plant`, `Storage_Location_MD`,
   `Component_Plant_Status`, `backflush`, `MRP_Unit`.
2. **Référentiel parent** (lignes 131–133) : même table, colonnes `Material_Number`, `plant`, `Material_Plant_Status`, `MRP_Unit`.
3. **Jointure 1** (lignes 146–156) : `silver LEFT JOIN référentiel composant ON b.MRP_Unit = a.plant AND b.component = a.component AND b.plant = a.plant`.
   Les colonnes `component`, `plant`, `MRP_Unit` du référentiel sont supprimées après jointure.
4. **Jointure 2** (lignes 158–168) : même principe avec `Material_Number` (parent) ; supprime `Material_Number`, `plant`, `MRP_Unit`.
5. **Storage_Location** (lignes 170–175) : `coalesce(storage_location, Storage_Location_MD)` ; crée `Storage_Location`, supprime `Storage_Location_MD`.
6. **Quantité** (lignes 179–187) :
   - si `alternative_item = 'X'` : `quantity = quantity × probability / 100` ;
   - puis `quantity = quantity / quantity_base` ;
   - supprime `quantity_base` et `alternative_item`.
   → **`quantity` Gold = quantité par unité de l'AF, pondérée par la probabilité pour les postes alternatifs.** Ce n'est pas `MENGE`.
7. **`business_id`** (lignes 191–202) : `concat_ws("_", BOM_number, plant, item_number, material_number, alternative, node)`.
8. **Contrôles RED** (lignes 228–251), avant écriture :
   - `creation_date` et `last_modif` de type `DateType` ;
   - `quantity` de type `DoubleType` ;
   - `plant` de longueur 4 ; `BOM_number` et `node` de longueur 8 ;
   - `business_id` unique.
   Le nom passé dans le log est `as_design_BOM_gold`.
9. **Écriture** (lignes 265–271) : `save_table(mode="overwrite")` sur `{PIPELINE_WRITE_ENV}_gold.production.BOM`
   (sans `overwrite_schema`).
10. **Vue** (lignes 280–298) : `create_table_view(…, BOM_exposed, select = colonnes dont le nom ne commence pas par STPO, MAST, STKO ou STAS, replace_view=True)`.

### 6.2 Liste des 216 colonnes Gold (dans l'ordre de la table)

Les colonnes sont identiques à la Silver (§5) avec les exceptions suivantes :

| Changement | Colonnes |
|---|---|
| Supprimées | `quantity_base`, `alternative_item` |
| Renommée | `storage_location` → `Storage_Location` |
| Ajoutées (en fin de table) | `Component_Plant_Status`, `backflush`, `Material_Plant_Status`, `business_id` |
| Valeur modifiée | `quantity` (pondérée, §6.1 point 6) |

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

Types et sens : identiques à la Silver (§5). Les quatre colonnes ajoutées :

| Colonne | Type | Origine | Sens |
|---|---|---|---|
| `Component_Plant_Status` | string | `Material_Plant_Status` du référentiel, côté composant | statut du composant dans l'usine |
| `backflush` | string | `MARC_rgekz` du référentiel | indicateur de post-consommation (rétro-déduction) |
| `Material_Plant_Status` | string | `Material_Plant_Status` du référentiel, côté AF | statut de l'AF dans l'usine (les deux noms `Material_Plant_Status` et `Component_Plant_Status` viennent du même champ de la table de référentiel, selon le côté de la jointure) |
| `business_id` | string | calculée (§6.1 point 7) | identifiant métier, unique |

---

## 7. Vue `BOM_exposed` (22 colonnes)

C'est le contrat de lecture pour les consommateurs. Toute évolution de la Gold doit conserver ces 22 colonnes.

| # | Colonne | Type | Origine | Sens |
|---|---|---|---|---|
| 1 | `BOM_number` | string | `STLNR` | identifiant de la nomenclature |
| 2 | `node` | string | `STLKN` | nœud du poste |
| 3 | `alternative` | string | `STLAL` | alternative de fabrication de l'AF |
| 4 | `material_number` | string | `MATNR` | article fabriqué (AF) |
| 5 | `plant` | string | `WERKS` | usine |
| 6 | `BOM_type` | string | `STLTY` | `M` matériel |
| 7 | `creation_date` | date | `ANDAT` (STKO) | date de création de l'en-tête |
| 8 | `BOM_status` | string | `STLST` | statut de la BOM |
| 9 | `last_modif` | date | `AEDAT` (STPO) | date de dernière modification du poste |
| 10 | `component` | string | `IDNRK` | composant consommé |
| 11 | `item_type` | string | `POSTP` | catégorie de poste |
| 12 | `item_number` | string | `POSNR` | numéro de poste |
| 13 | `unit` | string | `MEINS` | unité de saisie du composant |
| 14 | `quantity` | double | `MENGE` puis pondéré (§6.1) | quantité par unité de l'AF, pondérée |
| 15 | `bulk_material` | string | `SCHGT` | matière en vrac |
| 16 | `probability` | string | `EWAHR` | probabilité d'utilisation (%) |
| 17 | `Storage_Location` | string | `LGORT` (coalesce avec le référentiel) | magasin de sortie |
| 18 | `alternative_station_group` | string | `ALPGR` | groupe de postes alternatifs |
| 19 | `Component_Plant_Status` | string | référentiel | statut du composant dans l'usine |
| 20 | `backflush` | string | référentiel | post-consommation |
| 21 | `Material_Plant_Status` | string | référentiel | statut de l'AF dans l'usine |
| 22 | `business_id` | string | calculée | identifiant métier |

**Ce que la vue ne contient pas** : le rebut (`STPO_ausch`), la quantité de base (`quantity_base`), l'unité de base, les dates
de validité, la version (`STKO_stkoz`, `STPO_datuv`…), le statut de fantôme (`STPO_stkkz` n'est pas un fantôme). C'est la raison
pour laquelle le notebook d'historique (A) expose ces informations séparément (voir `spec_technique_historisation_bom.md`).

---

## 8. Table `bom_qc`

- Produite par le job `W_0_uc27-business_data_consistency` (474119062548779), **pas** par le notebook Gold.
- Lit `BOM_exposed`.
- Existe en prod (`SHOW TABLES` du 2026-10-13).
- Colonnes : **non vérifiées** (le lineage Genie annonce `BOM_exposed` + 14 paires `quality_check_N` / `errors_N` + `errors_name`,
  non confirmé). Commande à lancer : `DESCRIBE TABLE prod_gold.production.bom_qc`.

---

## 9. Référentiel `material_master_data_plant`

| Élément | Valeur |
|---|---|
| Silver | `{WRITE_ENV}_silver.master_data.material_master_data_plant` ← `{READ_ENV}_bronze.sap_latecoere_ecc6.marc_latest` (LEFT) `mbew_latest` (sur `matnr` et `bwkey` = `werks`) |
| Filtres Silver | `MARC_lvorm` NULL ; `werks ≠ "1500"` ; `MBEW_bwtar` NULL ; `MARC_matnr ≠ "SPLIT"` |
| Renommage | headers `headers_sap_business_marc` et `headers_sap_business_mbew` (en dur `prod_bronze.headers_sap`) |
| Gold | `{WRITE_ENV}_gold.master_data.material_master_data_plant` + vue `_exposed` (colonnes `MARC*` et `MBEW*` exclues) |
| Entrées Gold | silver ci-dessus ; `{REF}_gold.master_data.material_master_data_basics_exposed` ; `plant_master_data_latest_exposed` ; `finance.currency_exchange` ; `mrp.list_mrp_full_exposed` ; `supply_chain.supplier_master_data_latest_exposed` ; Bronze `equk`, `equp`, `eord`, `t460a`, `mdma`, `ausp` ; `manual_input.mrp_controller_category` ; **sa propre sortie précédente** (`{READ_ENV}_gold.master_data.material_master_data_plant`, lignes 1036–1038) |
| Auto-dépendance | la Gold relit sa version précédente pour combler `PF_pourcentage_affectation`. Le premier run doit être traité à part. |
| Colonnes Gold | **non listées** : `DESCRIBE TABLE {env}_gold.master_data.material_master_data_plant` à lancer si besoin. |
| Environnements en dur | oui, dans le notebook Gold (`prod_gold…`, `prod_bronze…`) — à corriger avant Bitbucket |
| `udf` | `compute_global_PF_affectation` appelle `udf(...)` sans import dans le fichier — à vérifier contre la version déployée |

Le lineage utilise le référentiel `material_master_data_plant`. L'autre référentiel `material_plant` (job 639869952897627) n'est
pas utilisé par la BOM.

---

## 10. Consommateurs

| Domaine | Tables | Lit | Statut |
|---|---|---|---|
| SPS RFQ | `dev_bronze.sps_rfq.rfq_bom_where_used`, `rfq_master`, `rfq_parts`, `rfq_contracts`, `rfq_movements`, `rfq_stock_by_plant`, `rfq_work_orders`, `rfq_wo_operations`, `rfq_pir`, `rfq_planned_orders`, `rfq_purchase_orders`, `rfq_stock_allocation`, `rfq_stock_coverage`, `rfq_stock_zones` | `BOM_exposed` | dev (lineage Genie) |
| Supply Chain | `dev_proj.supply_chain.current_average_bom`, `dev_lab.supply_chain.average_bom`, `average_bom_multilevel` | `BOM_exposed` | dev / lab |
| QC | `prod_gold.production.bom_qc` | `BOM_exposed` | prod |
| Disaster recovery | `dr_prod_gold.production.bom` | table `BOM` | prod |
| Finance | `prod_gold.finance.bill_of_material` (23 colonnes) | source [À CONFIRMER] | prod |

Conséquence : `BOM_exposed` est le **contrat** de lecture. Changer ses 22 colonnes casse les consommateurs listés.

---

## 11. Ce qui n'a pas été lu

| Élément | Pourquoi | Comment l'obtenir |
|---|---|---|
| JSON du job `1007135342276807` | paramètres `pipeline_read_env` / `reference_read_env` par task, cluster, planning | Workflows → job → *Edit as JSON*, ou `databricks jobs get 1007135342276807` |
| Notebook du job uc27 (`bom_qc`) | hors de la chaîne BOM, colonnes et règles inconnues | Chemin du notebook à fournir |
| `create_gold_material_master_data_plant` complet | lu, mais `udf` et la version déployée à confirmer | comparer avec le workspace |
| `DESCRIBE` de `bom_qc`, `bill_of_material`, `material_master_data_plant` | colonnes non listées | `DESCRIBE TABLE …` |
| Sens des champs SAP marqués [À CONFIRMER] | aucune table des champs SAP dans le repo | table des champs SAP (DD03L / DD02L) ou documentation Latécoère |

---

## 12. Corrections par rapport au lineage Genie

| Point | Genie | Vérifié |
|---|---|---|
| Silver : nombre de colonnes | ~170 | **214** (DESCRIBE du 2026-10-13) |
| Silver : nombre de lignes | 3 230 287 | confirmé |
| Gold : nombre de colonnes | ~175 | **216** |
| Job | `1007135342276807` | confirmé ; nom `W_0_AS_Design_BOM_DataAsset` |
| `bom_qc` | produite par le même job | **produite par le job uc27** (`W_0_uc27-business_data_consistency`) |
| Gold : relations | `production.bom` ← Silver + référentiel | confirmé ; la vue ne contient que 22 colonnes |
| Silver : STKO | `BOMEIN` présenté comme quantité de base | `quantity_base` = `BMENG` (quantité) ; `STKO_bmein` = unité |
| Phantom | `STPO_stkkz` cité comme indicateur phantom | **faux** : `STKKZ` n'est pas le fantôme. Le fantôme est `MARC.SOBSL = '50'` (journal B11–B13) |
| Type de poste | `L`, `N`, `R`, `T` | incomplet : `D`, `Z` et `0` observés |
| Valeur de `quantity` | non mentionnée | **pondérée** dans la Gold (§6.1) |
| Référentiel `material_master_data_plant` | Component_Plant_Status, backflush, Material_Plant_Status, business_id | confirmé, mais la clé de jointure exacte est **MRP_Unit = plant** |
| Header STPO | non mentionné | nom réel `header_sap_business_stpo` (sans « s ») |
| Entrées Silver | `{mast,stko,stas,stpo}_latest` | confirmé ; headers et filtres (§4) non listés par Genie |
| Filtres Silver | non mentionnés | `BOM_status ∉ {02, ""}`, `cslty ≠ X`, `stlan = "1"`, `BOM_type = "M"` |

---

## 13. Écarts entre la Silver officielle et l'historique A

Le détail est dans `spec_technique_historisation_bom.md` §8. Résumé :

| Sujet | Silver officielle | Notre A |
|---|---|---|
| Statut BOM | exclut `02` et vide | aucun filtre |
| Type MAST | exclut `cslty = X` | aucun filtre |
| Usine MAST | garde les liens sans usine | exclut (80 liens) |
| STAS | toutes les allocations non annulées | dernière par (alternative, nœud), `LKENZ`, dates |
| STKO | dernier `creation_date` | dernier `datuv`, puis `stkoz` |
| Quantité | pondérée (Gold) | brute + quantité de base |

Lignes à clé NULL dans la Silver : **691** (écart entre `count(*)` et `count(DISTINCT …)`). Hypothèse : liens MAST sans usine
× leurs postes. À confirmer avec la requête du §8.2 de la spec.

---

## 14. Requêtes utiles

```sql
-- Colonnes exactes (remplace les comptages manuels)
SELECT count(*) FROM system.information_schema.columns
WHERE table_catalog = 'prod_silver' AND table_schema = 'production' AND table_name = 'bom';
SELECT count(*) FROM system.information_schema.columns
WHERE table_catalog = 'prod_gold' AND table_schema = 'production' AND table_name = 'bom';

-- Clés NULL dans la Silver (hypothèse §13)
SELECT count_if(plant IS NULL) AS plant_null, count_if(material_number IS NULL) AS material_null,
       count_if(MAST_stlan IS NULL) AS usage_null, count_if(alternative IS NULL) AS alt_null,
       count_if(BOM_number IS NULL) AS bom_null, count_if(node IS NULL) AS node_null
FROM prod_silver.production.bom;

-- Statuts et types de BOM dans la Silver
SELECT BOM_status, count(*) FROM prod_silver.production.bom GROUP BY BOM_status;
SELECT BOM_type,  count(*) FROM prod_silver.production.bom GROUP BY BOM_type;

-- Colonnes non listées
DESCRIBE TABLE prod_gold.production.bom_qc;
DESCRIBE TABLE prod_gold.finance.bill_of_material;
DESCRIBE TABLE prod_gold.master_data.material_master_data_plant;
```
