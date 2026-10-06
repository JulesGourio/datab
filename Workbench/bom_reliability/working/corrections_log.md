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
