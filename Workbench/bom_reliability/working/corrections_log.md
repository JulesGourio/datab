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
