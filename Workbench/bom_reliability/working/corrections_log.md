# Corrections log — BOM reliability

## 2026-10-06 — Découverte Genie, blocs 1 à 3 (H1–H5, S1–S3, B1–B18)

Source : réponses Genie collées par Jules. Synthèse et impacts : spec §13.

### Historiques (H)

| Stack (`prod_landingzone.sap_latecoere_ecc6`) | Première extraction | Dernière | Dates distinctes | Lignes |
|---|---|---|---|---|
| `stpo_stack` | 2023-04-23 | 2026-10-04 | 181 (hebdo) | 2 817 159 287 |
| `stko_stack` | 2023-04-23 | 2026-10-04 | 181 (hebdo) | 93 215 618 |
| `mast_stack` | 2024-06-11 | 2026-10-05 | 841 | 245 542 875 |
| `stas_stack` | 2024-06-11 | 2026-10-05 | 834 | 15 774 005 017 |
| `resb_stack` | 2023-09-17 | 2026-10-05 | 787 | 59 102 473 275 |
| `plaf_stack` | 2022-12-11 | 2026-10-05 | 804 | 6 186 693 252 |
| `afko_stack` | 2022-11-29 | 2026-10-05 | 1 101 | 5 283 263 152 |
| `afpo_stack` | 2022-11-30 | 2026-10-05 | 1 105 | 5 273 629 596 |
| `mkal_stack` | 2023-04-23 | 2026-10-05 | 281 | 94 283 276 |
| `marc_stack` | 2022-11-29 | 2026-10-05 | 630 | 484 527 007 |
| `marm_stack` | 2023-04-09 | 2026-10-05 | 612 | 229 646 127 |

- Colonne de date : `extraction_timestamp` (+ `ingestion_timestamp`, `file_name`, `file_mode`, `stack_row_id`, `ID`).
  `stas_stack` partitionnée par `extraction_timestamp`.
- Bronze : uniquement des `_latest`, recréés chaque nuit en CTAS, un seul snapshot chacun. `marm_latest` non
  rafraîchi le 2026-10-05.
- `stpo_stack` / `stko_stack` : append `COPY INTO` + `OPTIMIZE` (Z-ORDER `ID`) ; VACUUM rétention 40 j (2025-09-05,
  2026-09-12, 2026-10-03) ; aucun DELETE / TRUNCATE / OVERWRITE.

**Conséquence** : B est reconstructible depuis les stacks (plus de capture urgente). T0 ≥ 2023-09-17.

### Snapshots (S)

- STPO / STKO : 1 extraction par jour d'extraction, pas de variation > 20 % ; 7 semaines manquantes, identiques
  dans les deux tables : 2023-07-03, 2023-08-07, 2023-09-04, 2024-02-12, 2024-03-25, 2025-12-29, 2026-05-11.
- STAS : 13 jours à 2 extractions (volume ×2), MAST : 7 jours (ex. 2024-06-11, 2025-03-06, 2026-01-18).
  → garder la dernière extraction du jour.

### BOM (B) — dernier snapshot

- `stlty` : STPO K 17 227 069 / M 4 715 776 / S 926 ; STKO K 356 455 / M 299 958 / S 922. 961 `stlnr` en K+M.
- MAST : `stlan = '1'` 99,97 % ; 80 lignes `werks` NULL ; 33 575 (matnr, werks, stlan) multi-alternatives (max 26) ;
  219 `stlnr` partagés entre plusieurs (matnr, werks).
- STPO M : 4 715 776 postes, 2 382 988 avec `aennr` (50,5 %), (stlnr, stlkn) unique, 0 `lkenz = 'X'`.
  STAS M : 5 866 203 ; STKO M : 299 958 dont 1 `lkenz = 'X'`. Aucun nœud avec plusieurs `stpoz` (tous snapshots).
- Aucun doublon de clé par snapshot (STPO, STKO, STAS, MAST).
- `menge` : signe moins final (`1.000-`) sur 1 080 postes, espaces finaux, point décimal.
- `ausch > 0` : 1 859 postes (10 % ×752, 30 % ×151, 35 % ×89, 15 % ×82…) ; `avoau` non nul : 5 ; `netau = 'X'` : 10.
- `schgt = 'X'` 125 567 ; `fmeng = 'X'` 69 951 ; `postp` L 4 157 685, R 205 030, Z 125 971, D 106 349, 0 97 734,
  T 22 449, N 507, 1 36, U 8, 4 5, V 1, 2 1.
- `dumps` n'existe pas dans `stpo_stack`. Genie a testé `stkkz` (0) et conclu « pas de fantôme » : **conclusion
  invalide**, à refaire avec `sobsl = '50'` (F4).
- `bmeng` : = 1 pour 96,5 % ; ≠ 1 pour 10 353 BOM (2, 6, 4, 12, 3, 10…) ; jamais 0.
- Unités : 347 163 postes (7,6 %) avec unité ≠ unité de base MARA, dont 53 268 sans conversion MARM.
- `stas_stack` : 29 colonnes, dont `andat`, `datuv`, `lkenz`, `aennr`, `stvkn`.
- Orphelins : 97 STPO sans STAS, 1 STKO sans MAST.
- `prod_silver.production.bom` : 3 228 402 lignes, 186 colonnes, job `W_3_SAP_AS_Design_BOM_DataAsset`
  (id 1007135342276807, CTAS le dimanche ~01h), filtres `stlty = 'M'`, `stlan = '1'`, sans `lkenz`.

### À faire

Questions de suivi F1–F12 (`genie_questions_and_output_tests.md` §1.9), puis blocs 4 à 6.

## 2026-10-06 — Découverte Genie, lot 2 (F1–F7, F11–F12, R1–R4, R6, R9 partiel, W1–W4, M1–M2, M4–M7, D1)

Source : `Audit BOM — Réponses F1…` (PDF Genie). Synthèse et impacts : spec §13.3.

- **file_mode** : FULL seulement pour afko (1 101 dates), afpo (1 105), mast (841), plaf (804), resb (787),
  stas (834), stko (181), stpo (181) ; marc FULL 592 + **DELTA 44** ; marm FULL 184 + **DELTA 500**.
- **resb_stack** : partition `extraction_timestamp`, Delta, 13 020 fichiers, ~1,84 To ; ~4-5 extractions/mois de
  2023-09 à 2024-09, quotidienne depuis 2024-10.
  Dernière extraction (2026-10-05) : 79 516 161 lignes, 38 565 183 « ouvertes » (AR/SB, non `xloek`, non `kzear`, `enmng` = 0).
  2024-09-29 : 30 596 296 / 26 091 438. **2025-03-31 : 40 166 143 / 0** (38,7 M AR/SB avec `xloek = 'X'`, le
  reste `kzear = 'X'`) → anomalie à expliquer (G1).
- plaf / afko / afpo / marc : quasi quotidiens sur 12 mois ; mkal hebdo puis quotidien depuis 2026-06. Tous
  partitionnés par `extraction_timestamp`.
- **Fantômes** : `sobsl` absent de `stpo_stack` ; `marc_latest` `sobsl = '50'` ≈ 17 269 articles (1000 : 7 859,
  2400 : 1 942, 2000 : 1 288, 3000 : 1 257, 2110 : 1 245…) ; `resb_stack.dumps = 'X'` : 4 323 725 lignes (dernière extraction).
- **Rebuts** : `kausf > 0` 350 articles (5 % ×80, 7 % ×49, 10 % ×30) ; `ausss > 0` 32 196 articles (10 % ×12 420,
  5 % ×7 811, 1 % ×7 191) ; RESB AR/SB `ausch > 0` 2 226 869 lignes (7 %, 5 %, 10 %, 30 %, 4 %).
  Gold `material_plant` : `scrap_component_percentage` (kausf, float), `MARC_ausss` (string brut).
- **ECN** : STPO M `vgknt <> '00000000'` 432 815 postes (9,2 %) ; STAS M `lkenz = 'X'` 1 091 013 lignes ;
  aucune `datuv` postérieure à la date d'extraction (STPO, STAS).
- mkal_stack : `stlal`, `stlan`, `adatu`, `bdatu`, `verid` présents. afpo_stack : `plnum` présent. afko_stack :
  `gstri`, `gltri`, `getri`, `gamng`, `gstrp`, `gltrp`, `stlal`, `stlan` présents.
- **RESB par bdart** (dernière extraction) : SB 37 437 821 (+133 110 rsart 1) avec `plnum` ; AR 36 697 888
  (+89 789) avec `aufnr` ; BB 4 939 099 et MR 218 454 sans ordre. Colonnes : tout présent sauf `netau` ; `nomng`,
  `esmng` présents. Clé (rsnum, rspos, rsart) unique.
- `prod_gold.mrp.reservation_mrp_sap` = 79 516 161 lignes, miroir de la dernière extraction RESB.
- RESB AR avec `stlnr` vide (toutes extractions confondues, semble-t-il) : 2300 7,5 %, 1000 8,2 %, 2010 13 %,
  2000 15,8 %, 1900 59,5 %, 2200 99,8 %, autres < 1 %.
- plaf_stack : `plnum`, `matnr`, `plwrk`, `gsmng`, `psttr`, `pedtr`, `paart`, `stlal`, `stlan`, `verid`, `rsnum` présents.
- **work_orders_sap_exposed** (58 col.) : `real_start_date` ≈ GSTRI, `real_end_date` ≈ GETRI (probable),
  `planned_order_link` (= plnum), `production_version` ; pas de `stlal`, pas de date TECO.
  Q1 2026 : 216 couples usine/CP (5000/A2P 7 809, 2400/101 6 194, 4000/BEP 3 448…) ; 5 730 commencés sans fin.
  Statuts : majorité `WO Closed` (ZP01 1 036 400, ZP04 691 753, ZP03 570 372, YP04 538 236, ZP09 372 527,
  ZP05 360 872) ; annulés ZP05 88 475, ZP01 74 336.
- Formats : `work_order_number` et `Work_order` sans zéros ; `resb.aufnr` 12 caractères avec zéros.
- **part_movement_exposed** (42 col.) : `DC_indicator` (= shkzg), `Quantity` STRING, `Unit` = unité de saisie.
  12 mois avec OF : 261 8 276 755 ; 101 373 357 ; 262 36 093 ; 102 3 615 ; 531 2 866 ; 532 24 ; 122 4 ; 521 1.
  701 (10 191) / 702 (11 241) jamais sur OF ; 711/712 absents ; aucun type Z/Y/≥900 sur OF.
  9 648 couples (OF, article) à conso nette négative. Données depuis 2020-01-01 (6,6 M → 9,4 M mouvements/an).
- **material_plant** (390 col.) : `profit_center`, `external_lead_time` (plifz), `internal_lead_time` (wzeit),
  `procurement_special_type` (sobsl), `standard_price_eur_budget`, `material_base_unit`, `familly_std`,
  `classe_std`, `abc_indicator`, `scrap_component_percentage`, `MARC_ausss`. material_exposed (16 col.) :
  description, `familly_std`, `classe_std`, `manuf_process`, `assembly_level`, `commodity`…

Non traités par Genie (repris au lot 3) : F4d, F4f, F8, F9, F10, R5, R9 (comptage), R10, R11, M3, D2, D3, D4, E1–E4.

## 2026-10-06 — Découverte Genie, lot 3 (G1–G10, F4d, F4f, F8, F9, R9, D2–D4, E1, E2, E4)

- **G1 RESB** : trois profils d'extraction qui alternent (fin de mois) :
  A ~30–45 M lignes, `xloek` ~95 %, `kzear` ~70 % (surtout clôturé) ; B ~60–83 M lignes (mélange) ;
  C ~22–37 M lignes, 0 % `xloek`/`kzear`/`enmng > 0` (ouvert seulement : 2023-09, 2024-01, 2024-07 → 09, 2025-11).
  Pas de changement de schéma (que des `COPY INTO` du job `F_0_SAP_Ingestion_V2` + VACUUM). Les lignes clôturées
  sont identiques d'une extraction à l'autre.
  → B choisit, avant chaque 1er du mois, l'extraction la plus récente contenant les besoins ouverts.
- **G2 horizon** (dernière extraction, ouverts) : AR passé 484 640, 0–3 m 422 235, 3–6 m 25 625, 6–13 m 32 814,
  13–24 m 20 240, > 24 m 8 698 ; SB passé 106 651, 0–3 m 3 475 131, 3–6 m 4 667 231, 6–13 m 9 021 709,
  13–24 m 12 513 385, > 24 m 7 786 824. → filtre ≤ 13 mois.
- **G3** : `esmng` = besoin sans rebut composant (`bdmng` = `esmng` × (1 + `ausch`) arrondi à l'entier sup.) ;
  `nomng` = 0. → Prévision 2 = Σ `esmng`.
- **G4** : AFKO `gasmg` (rebut), `gamng`, `igmng`, `iasmg` ; PLAF `avmng`, `gsmng`.
- **G5** : `Quantity` de part_movement est déjà en unité de base (0 écart sur 8,3 M mouvements 261/262) ; pas de
  table Gold de conversion.
- **G6** : 531 sur OF = souvent l'article fictif `SPLIT` (sous-traitance), pas des sous-produits.
- **G7/M3** : mouvements sur l'AF de l'OF : 101 331 472, 261 2 568 (rework), 102 2 508, 262 77, 122 4, 521 1.
- **G8** : `work_order_type_description` existe : Details Parts (ZP01, ZP05, YP03), Assembly Parts (YP04, ZP03,
  ZP04), Rush Orders (ZP09, ZP10, ZP11, ZP19, ZP21, YP09, YP10, YP11).
- **G10** : MARC FULL ~827 k lignes stables ; DELTA 13–36 k lignes, plus aucune depuis 2025-07.
- **F4d** : 11 245 articles `sobsl = '50'` sont composants d'une BOM de la même usine (65 %).
- **F4f** : OF 000006799399 : lignes `dumps = 'X'` = fantômes (S9251393000400, S9251392900100, imbriqués) ; leurs
  composants suivent avec `baugr` = fantôme. SAP éclate donc déjà les fantômes dans RESB.
- **F8** : pas de T418T ; postes `Z` et `0` sont bien consommés (4,3 M mouvements sur OF en 12 mois) → gardés ;
  `D` sans article → exclu.
- **F9** : 53 268 postes sans MARM : IN→M 51 190, MM→M 568, MM2→M2 465, CCM→L 358, IN2→M2 319, CM2→M2 157,
  G→KG 127, FT→M 62… → facteurs ISO constants dans A.
- **R9** : OP par usine : 1000, 1010, 3000, 4000, 5000 ont l'essentiel de leurs OP à plus de 12 mois.
- **D2** : `prod_gold.master_data.plant_master_data_latest_exposed` (Plant, Plant_Description…),
  `prod_gold.finance.profit_center_exposed` (Profit_Center, Short_Description, Long_Description, validité…).
- **D3** : `prod_gold.master_data.material_plant_price_history_exposed` (par période fiscale) ; prix courant dans
  `material_plant.standard_price_eur_budget`.
- **D4** : « PF usage » : `material_plant.quota_usage` ? ou `dev_selfservice.master_data.material_plant.PF_pourcentage_affectation` → à définir avec le métier.
- **E1** : stlnr 00167113 (ARG_FINIT_DOOR_A2, usine 3000) : nœud 00000895 `menge` 1 → 2 entre 2025-04-27 et
  2026-04-26 (autres : 00169610/00000113 93 → 89 ; 00226287/00000030 164 → 66).
- **E2** : OF 6793594 (IS0014623M, usine 2400, OP 991075254, fin réelle 2026-02-04), fantôme IS0021449M.
- **E4** : D5211200300000 : 6 alternatives avec des OF ; D5211200705214 : alternatives 01 et B1, 10 623 OF.
- Non traités : G9 (lineage dates réelles), F10 (job BOM existant), R5, R10, R11, E3 — non bloquants pour le code v1.

## 2026-10-06 — Lab run 1 de `create_gold_bom_item_history` (RED, table non écrite)

- 141 702 198 lignes finales ; PK `bom_item_history_ID` : 4 doublons
  (`20230501/20230601-1000-F5391308100400-1-01-00000556-00000052`).
- Cause : lignes répétées dans les extractions sources utilisées pour 2023-05 et 2023-06 :
  MARC (`F5391312700300` / usine 1900, 2 lignes) et MARM (`F5391312700300`, unités `U` et `X`, 2 lignes chacune).
  Le doublon MARM a dupliqué un poste de BOM à la jointure de conversion d'unité.
- Contrôles OK : STPO, STAS, STKO, MAST, unité de base uniques ; PK non nulle.
- Correction : `keep_latest_row()` (ligne la plus récemment ingérée par clé) sur MARC et MARM, et par précaution
  sur PLAF et AFKO dans `create_gold_order_component_requirement_history`. Cause à remonter à l'équipe d'ingestion.
- Analyse du doublon MARC (`F5391312700300` / 1900, extractions de 2022-11-29 à 2023-05-28) : à **chaque**
  extraction, 2 lignes **dans le même fichier parquet** (`..._SAP-MARC-F_..._0001.parquet`), `stack_row_id`
  différents, valeurs métier identiques (`sobsl` 40, `kausf` 0, `ausss` 0), même `ingestion_timestamp`
  (rechargement du 2024-01-31). Le doublon vient donc de l'extraction SAP, pas de l'ingestion ; le garder une
  seule fois ne change aucune valeur. Cause SAP (mandant, `matnr` avec espace ?) à vérifier si besoin.

## 2026-10-07 — Lab run 2 de `create_gold_bom_item_history` (OK) — contrôles de sortie

Run en job, toutes usines, tout l'historique. Résultats des requêtes de contrôle :
- 141 702 196 lignes, 42 snapshots (2023-05-01 → 2026-10-01), PK unique et non nulle, extraction toujours
  antérieure au snapshot, usage 1 seulement, rétro-datation MAST/STAS limitée à 2023-05 → 2024-06.
- Profil mensuel régulier (3,03 M → 3,69 M lignes), pas de saut en 2024-07 à la fin de la rétro-datation.
- Dernier snapshot : 3 688 354 lignes, 3 631 652 noeuds = 4 715 776 noeuds STPO − 1 079 402 noeuds clos par STAS
  (ECN) − 2 417 sans STAS − 1 448 sans lien MAST usage 1 − 2 inexpliqués. Aucun noeud hors STPO.
  Aucun ancien noeud ECN présent avec son remplaçant (pas de double comptage).
- Ratios cohérents avec STPO (≈ 77 % des noeuds actifs) : rebut composant 1 186 noeuds, quantité fixe 53 413,
  vrac 97 762 ; composants fantômes 10 899 (≈ 11 245 en F4d) ; quantités négatives 881.
- Exemples de référence E1 : 00167113/00000895 1 → 2 au snapshot 2025-11-01 ; 00169610/00000113 93 → 89 au
  2026-03-01 ; 00226287/00000030 164 → 66 au 2025-07-01.
- Conversion d'unité manquante : 6 lignes (G→U, 1 composant ; U sans unité de base).
- Écarts à analyser : `component_base_unit` NULL sur 93 954 lignes (correction : zéros retirés côté
  `material_exposed`) ; seulement 219 AF avec rebut d'ensemble > 0 contre 32 196 articles `ausss > 0` dans
  `marc_latest` (requête de diagnostic envoyée).
- Bug trouvé : la contrainte est stockée en minuscules (`gold_bom_item_history_pk`), la recherche idempotente
  comparait avec `gold_bom_item_history_PK` → le run suivant aurait tenté de recréer la PK. Corrigé avec `lower()`
  (A, B, templates) ; règle ajoutée dans les guidelines.
- Vue `_exposed` pas encore créée.
- Diagnostic ⑦ : les 37 387 composants sans unité de base sont **absents de `material_exposed`** (ni tel quel ni
  sans zéros). → A lit l'unité de base dans `{REFERENCE_READ_ENV}_bronze.sap_latecoere_ecc6.mara_latest`.
  À vérifier pour C : descriptions et attributs des composants viennent de `material_exposed` / `material_plant`.
- Diagnostic ⑧/⑨ : les 32 196 articles `ausss > 0` sont surtout des **achetés** (`beskz = F`, `sobsl = 20`,
  types ROH / CA : écrous, vis, fermetures…). Seuls 219 AF de BOM ont un rebut d'ensemble. Pour un acheté, SAP
  applique `ausss` à ses propres propositions d'achat, pas aux besoins des OF : ce rebut n'entre dans aucune des
  Prévisions 1/2/3. Rebut réellement porté par les BOM / OF : `ausch` (1 186 noeuds actifs), `kausf` (350
  articles), `ausss` des AF (219). → question métier : le « +20 % » est-il ce rebut côté achat ?

## 2026-10-09 — Genie : rebuts (RB1–RB4) et ajustements des OF (RA1–RA2)

- **RB1** : 32 173 articles achetés (`beskz = F`) avec `ausss > 0` → 287 385 lignes de réservation d'OF, `ausch = 0`
  dans 100 % des cas. Le rebut d'ensemble d'un article acheté **n'atteint jamais les OF**. Les 27 804 lignes
  `bdmng > esmng` (sans `ausch`) relèvent de l'arrondi SAP, pas d'un rebut.
- **RB2 / RB3** (OF terminés en 2025, achetés) : consommation / réservation = **0,74** pour les articles avec
  `ausss > 0`, **0,94** pour les autres. L'interprétation de Genie (« réservations gonflées par `ausss` ») est
  contredite par RB1 : les réservations ne sont pas gonflées, ce sont ces articles (visserie, fermetures, type CA)
  qui sont **moins consommés** sur les OF que réservé. Pas de surconsommation atelier = pas de rebut réel visible
  côté OF. À expliquer (sorties hors OF ? vrac ? OF soldés sans sortie ?) → question M17.
- **RB4** : origine du `ausch` des réservations (2,2 M lignes) : `MARC.KAUSF` 82,8 %, aucune correspondance 17,1 %
  (modif. manuelle sur l'ordre, ou `kausf` qui a changé depuis — comparaison faite avec `marc_latest`), poste de
  BOM 58 lignes. → le rebut qui gonfle réellement les OF est le **rebut composant de la fiche article** (cas 2).
  La Gold A applique déjà la règle SAP (`ausch` du poste, sinon `kausf` historisé).
- **RA1 (a)** : lignes d'OF ajoutées à la main (sans `stlnr`), OF terminés 2025 : 0,2 % (2400), 0,5 % (5000),
  0,6 % (2110), 7,4 % (2300), 7,8 % (1000), 13,4 % (2010), 52,9 % (4030), 84,3 % (1900).
- **RA1 (b)** : résultat non exploitable (BOM cherchée dans la dernière extraction au lieu de celle de la création de
  l'OF → 50 % de BOM non trouvées). L'usine 4000 (0,1 % d'écart) montre que les quantités collent quand la BOM est
  la bonne. L'écart quantités OF / BOM sera mesuré directement par le notebook C (Prévision 2 vs Prévision 3).
- **RA2** : 82 OF terminés en 2025 dont l'AF a `ausss > 0` (jusqu'à 20 %) : `gasmg = 0` et `gamng` = quantité
  planifiée dans 100 % des cas → le **rebut d'ensemble des AF n'est pas appliqué** aux OF chez Latécoère.
  Cohérent avec C (rebut d'ensemble non ajouté à la Prévision 3).

## 2026-10-09 — Genie : GQ1–GQ4 et corrections

- **GQ1** : `resb_stack` contient deux familles de fichiers, `SAP-RESB-F-ACT` (réservations mélangées) et
  `SAP-RESB-F-NOACT` (réservations ouvertes). Hypothèse : certaines extractions ne contiennent qu'une famille, ce qui
  explique les profils A/B/C observés. → **B** : l'heuristique « extraction avec le plus de lignes ouvertes » est
  remplacée par la dernière extraction **de chaque famille** avant chaque 1er du mois, union des deux, dédoublonnage
  par réservation (extraction la plus récente gagne), filtre « besoin ouvert » conservé. Un mois sans l'une des
  familles est écarté (`log.warning`). À confirmer : ACT et NOACT ont-ils parfois des `extraction_timestamp` différents ?
- **GQ2** : les composants absents de `material_exposed` sont des articles obsolètes / remplacés. → **A** lit les
  descriptions dans `makt_latest` (français, sinon anglais) : `material_description`, `component_description`.
  **C** : description de `material_exposed` d'abord, sinon celle portée par `bom_item_history`.
- **GQ3** : les mouvements 543 (sous-traitance) expliquent une partie (~9 %) de la sous-consommation de la visserie
  achetée sur les OF. M17 reste une question métier.
- **GQ4** : pas de motif de mouvement (`grund`) dans `part_movement_exposed` (seulement `Storage_location` /
  `Item_text`) → la casse n'est pas identifiable depuis la Gold. Conso 2 = 261/262 (M7 reste ouvert).
- À faire : relancer A (puis B, C) en lab avec les mêmes paramètres de job.
