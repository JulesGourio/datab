# Average BOM history (notebook D) — spécification et vérifications

Notebook : `data_asset/production/bom_history/create_gold_average_bom_history.py`
Table : `{env}_gold.production.average_bom_history` (+ vue `_exposed`), partition `snapshot_date`, mensuelle (mêmes dates que `bom_item_history`).

## 1. Ce que c'est

« Average BOM » est le nom interne de la **BOM commune** : une seule BOM par article fabriqué (AF) et usine, la même pour tous les MSN
d'un même CA, que ME met à jour dans SAP (IN_P_0030). La Prévision 3 de UC93 (« as-designed ») est l'historique de cette BOM :
ce que SAP en disait au 1er de chaque mois. Le notebook D la reconstruit à partir de l'historique des BOM (notebook A) et de
l'historique des versions de production (`mkal_stack`).

## 2. Règles reprises du notebook Average BOM existant (`create_proj_average_bom`)

| Règle | Existant (aujourd'hui) | Notebook D (à chaque snapshot) |
|---|---|---|
| Versions candidates | non verrouillées (`mksp`), `Start_Validity <= today < End_Validity` | idem, à la date du snapshot, extraction MKAL strictement antérieure |
| Plusieurs versions valides | version du quota (la plus forte part), sinon la 1re créée | idem : quota, sinon plus petit `adatu`, sinon plus petit `verid` |
| Pas de version utilisable | BOM créée en premier | plus petite alternative (`selection_rule = LOWEST_ALTERNATIVE`) — la date de création de l'en-tête n'est pas gardée par A |
| Fantômes (`sobsl = 50`) | GraphFrames, 7 niveaux | jointures itératives, 7 niveaux, BOM sélectionnée du fantôme (même règle, même date), poste `9999` |
| Vrac, statut usine `05`, `SPLIT` | filtrés | **marqués, pas filtrés** (questions L7 / L9 ouvertes) : `is_bulk_material`, `component_plant_status`, composant `SPLIT` à filtrer dans C |
| Rebut | `× (1 − ausch / 100)` | SAP : rebut du poste, sinon rebut article (`kausf`), appliqué comme facteur `1 + rebut` à chaque niveau (comme C) |

Différence entre les deux notebooks existants (à trancher) : `create_proj_bom_average_multilevel` choisit la plus petite alternative
parmi les versions valides, `create_proj_average_bom` le quota puis la plus ancienne. D applique le second.

## 3. Colonnes

Périmètre : widget `branch`, `IS` par défaut (branche de l'AF dans son usine, `material_master_data_plant_exposed`, valeur actuelle) ; vide = toutes les branches. Les fantômes sont éclatés quelle que soit leur propre branche.

`average_bom_history_ID` (PK) · `snapshot_date` · `plant` · `material_number` · `material_description` · `branch` · `production_version` ·
`BOM_alternative` · `selection_rule` · `nb_active_versions` · `BOM_number` · `BOM_item_number` (`9999` si éclaté) · `BOM_item_category` ·
`BOM_status` · `component_material_number` · `component_description` · `component_base_unit` · `quantity_per_unit` (rebut compris, `0` si
quantité fixe) · `quantity_per_order` (quantité fixe, rebut compris) · `component_scrap_percentage` · `component_plant_status` ·
`is_phantom_item` · `is_bulk_material` · `explosion_status` · `explosion_level` · `phantom_path` · colonnes techniques `_…`.

Prévision 3 dans C = `quantity_per_unit × quantité finale de l'OF + quantity_per_order` (le rebut est déjà dedans).

## 4. Contrôles à faire avant le premier run (et après)

Convention : `LZ` = `prod_landingzone.sap_latecoere_ecc6`, `A` = `dev_lab.lab_jules.bom_item_history` (sortie complète du notebook A, pas la version `_debug`).
Date de test des contrôles 9 à 13 : `2026-10-01` (un snapshot de A existant). Le notebook s'arrête de lui-même, avec la liste des colonnes
manquantes, si une entrée n'a pas la structure attendue (cellule « Required columns »). Les contrôles 2, 5, 6 et 7 disent à l'avance ce qu'il va trouver.

### Avant le run

**1. Piles disponibles** — résultat 2026-11-05 : `mkal_stack`, `marc_stack`, `equk_stack`, `equp_stack` existent. Les quotas sont donc historisés
par le notebook (plus de lecture de l'état actuel).
```sql
SHOW TABLES IN prod_landingzone.sap_latecoere_ecc6 LIKE '*stack';
```

**2. Structure de `mkal_stack`** — attendu : `matnr, werks, verid, stlal, mksp, adatu, bdatu, extraction_timestamp, ingestion_timestamp, file_mode, stack_row_id`.
```sql
DESCRIBE prod_landingzone.sap_latecoere_ecc6.mkal_stack;
```
```sql
SELECT matnr, werks, verid, stlal, mksp, adatu, bdatu
FROM prod_landingzone.sap_latecoere_ecc6.mkal_stack
WHERE extraction_timestamp = (SELECT MAX(extraction_timestamp) FROM prod_landingzone.sap_latecoere_ecc6.mkal_stack)
LIMIT 20;
```
Attendu : `adatu` / `bdatu` en texte `yyyyMMdd` (ex. `20230101`, `99991231`), `mksp` = `X` ou vide, `stlal` du type `01`.
```sql
SELECT mksp, COUNT(*) AS nb,
       SUM(CASE WHEN try_to_date(trim(adatu), 'yyyyMMdd') IS NULL OR try_to_date(trim(bdatu), 'yyyyMMdd') IS NULL THEN 1 ELSE 0 END) AS dates_illisibles,
       SUM(CASE WHEN trim(stlal) = '' THEN 1 ELSE 0 END) AS sans_alternative
FROM prod_landingzone.sap_latecoere_ecc6.mkal_stack
WHERE extraction_timestamp = (SELECT MAX(extraction_timestamp) FROM prod_landingzone.sap_latecoere_ecc6.mkal_stack)
GROUP BY mksp;
```
Attendu : `dates_illisibles = 0`. `sans_alternative` doit rester faible (une version sans alternative ne peut pas donner de BOM : repli sur la plus petite alternative).

**3. Extractions complètes de `mkal_stack`** — le notebook ne lit que `file_mode = 'FULL'`.
```sql
SELECT file_mode, COUNT(DISTINCT extraction_timestamp) AS nb_extractions,
       MIN(extraction_timestamp) AS premiere, MAX(extraction_timestamp) AS derniere
FROM prod_landingzone.sap_latecoere_ecc6.mkal_stack GROUP BY file_mode;
```
Attendu : `FULL` avec ~281 extractions depuis 2023-04-23. Si on ne voit que `DELTA` ou une valeur différente de `FULL`, le notebook lirait 0 ligne : me le dire.

**4. Couverture des snapshots par `mkal_stack`** — chaque snapshot de A doit avoir une extraction MKAL antérieure.
```sql
SELECT s.snapshot_date,
       MAX(CAST(m.extraction_timestamp AS DATE)) AS derniere_extraction_mkal,
       datediff(s.snapshot_date, MAX(CAST(m.extraction_timestamp AS DATE))) AS jours_ecart
FROM (SELECT DISTINCT snapshot_date FROM dev_lab.lab_jules.bom_item_history) s
LEFT JOIN (SELECT DISTINCT extraction_timestamp FROM prod_landingzone.sap_latecoere_ecc6.mkal_stack WHERE file_mode = 'FULL') m
  ON CAST(m.extraction_timestamp AS DATE) < s.snapshot_date
GROUP BY s.snapshot_date ORDER BY s.snapshot_date;
```
Attendu : aucune ligne à NULL, `jours_ecart` ≤ 7 (extractions hebdomadaires ou plus fréquentes). Un écart plus grand = trou dans la pile à signaler.

**5. `marc_stack.mmsta`** (statut usine du composant) — attendu : colonne présente, `05` majoritaire.
```sql
SELECT mmsta, COUNT(*) AS nb
FROM prod_landingzone.sap_latecoere_ecc6.marc_stack
WHERE file_mode = 'FULL'
  AND extraction_timestamp = (SELECT MAX(extraction_timestamp) FROM prod_landingzone.sap_latecoere_ecc6.marc_stack WHERE file_mode = 'FULL')
GROUP BY mmsta ORDER BY nb DESC;
```

**6. Piles des quotas** — attendu `equk_stack` : `qunum, matnr, werks, vdatu, bdatu` ; `equp_stack` : `qunum, qupos, verid, quote` (+ colonnes techniques
`extraction_timestamp, ingestion_timestamp, file_mode, stack_row_id`). Le notebook s'arrête avec la liste des colonnes manquantes sinon.
```sql
DESCRIBE prod_landingzone.sap_latecoere_ecc6.equk_stack;
```
```sql
DESCRIBE prod_landingzone.sap_latecoere_ecc6.equp_stack;
```
```sql
SELECT 'equk' AS pile, file_mode, COUNT(DISTINCT extraction_timestamp) AS nb_extractions, MIN(extraction_timestamp) AS premiere, MAX(extraction_timestamp) AS derniere
FROM prod_landingzone.sap_latecoere_ecc6.equk_stack GROUP BY file_mode
UNION ALL
SELECT 'equp', file_mode, COUNT(DISTINCT extraction_timestamp), MIN(extraction_timestamp), MAX(extraction_timestamp)
FROM prod_landingzone.sap_latecoere_ecc6.equp_stack GROUP BY file_mode;
```
Attendu : des extractions `FULL` régulières ; si `file_mode` vaut autre chose, ou si `equk` et `equp` ont des dates très différentes, me le dire.
```sql
SELECT (SELECT COUNT(DISTINCT trim(qunum)) FROM prod_landingzone.sap_latecoere_ecc6.equk_stack
        WHERE file_mode = 'FULL'
          AND extraction_timestamp = (SELECT MAX(extraction_timestamp) FROM prod_landingzone.sap_latecoere_ecc6.equk_stack WHERE file_mode = 'FULL')
          AND try_to_date(trim(vdatu), 'yyyyMMdd') <= current_date() AND try_to_date(trim(bdatu), 'yyyyMMdd') > current_date()) AS arrangements_pile,
       (SELECT COUNT(DISTINCT quota_number) FROM prod_gold.production.quotas_arrangement_exposed
        WHERE start_validity <= current_date() AND end_validity > current_date()) AS arrangements_gold;
```
Attendu : deux nombres égaux ou très proches (le notebook reconstruit les quotas comme la Gold, à une date près).

**7. Sortie du notebook A** — doit être complète et à jour (35 colonnes, ~42 snapshots, 3,7 M lignes par snapshot).
```sql
DESCRIBE dev_lab.lab_jules.bom_item_history;
```
```sql
SELECT snapshot_date, COUNT(*) AS nb_lignes FROM dev_lab.lab_jules.bom_item_history GROUP BY snapshot_date ORDER BY snapshot_date;
```
Attendu : de `2023-05-01` au 1er du mois courant, sans trou. Si seule la table `_debug` existe, relancer A en `full` d'abord.

**8. Quantité de base des BOM** — une quantité de base nulle ou égale à 0 donnerait une quantité inconnue.
```sql
SELECT COUNT(*) AS lignes, SUM(CASE WHEN BOM_base_quantity IS NULL OR BOM_base_quantity = 0 THEN 1 ELSE 0 END) AS base_nulle
FROM dev_lab.lab_jules.bom_item_history WHERE snapshot_date = DATE'2026-10-01';
```
Attendu : `base_nulle` = 0 ou négligeable.

**Versions de production valides au 2026-10-01 (à réutiliser dans les contrôles 9 à 11)**
```sql
CREATE OR REPLACE TEMP VIEW v_valid_versions AS
SELECT ltrim('0', trim(matnr)) AS material_number, trim(werks) AS plant, trim(verid) AS production_version,
       trim(stlal) AS BOM_alternative, try_to_date(trim(adatu), 'yyyyMMdd') AS valid_from
FROM prod_landingzone.sap_latecoere_ecc6.mkal_stack
WHERE file_mode = 'FULL'
  AND extraction_timestamp = (SELECT MAX(extraction_timestamp) FROM prod_landingzone.sap_latecoere_ecc6.mkal_stack
                              WHERE file_mode = 'FULL' AND CAST(extraction_timestamp AS DATE) < DATE'2026-10-01')
  AND trim(mksp) <> 'X'
  AND try_to_date(trim(adatu), 'yyyyMMdd') <= DATE'2026-10-01'
  AND try_to_date(trim(bdatu), 'yyyyMMdd') > DATE'2026-10-01';
```

**9. L'alternative de la version existe-t-elle dans A ?**
```sql
SELECT COUNT(*) AS versions_valides,
       SUM(CASE WHEN a.BOM_alternative IS NOT NULL THEN 1 ELSE 0 END) AS avec_bom_dans_A
FROM v_valid_versions v
LEFT JOIN (SELECT DISTINCT plant, material_number, BOM_alternative FROM dev_lab.lab_jules.bom_item_history
           WHERE snapshot_date = DATE'2026-10-01') a
  ON a.plant = v.plant AND a.material_number = v.material_number AND a.BOM_alternative = v.BOM_alternative;
```
Attendu : au moins 95 % avec BOM **sur les usines présentes dans A**. Résultat 2026-11-05 : 162 402 versions valides mais 1 seule avec BOM, parce que la table
`dev_lab.lab_jules.bom_item_history` ne contient que ~63 700 lignes par snapshot, de 2025-01-01 à 2026-10-01 (sous-ensemble d'un run de debug), pas toutes les usines.
Refaire le contrôle limité aux usines de A :
```sql
SELECT plant, COUNT(*) AS nb_lignes FROM dev_lab.lab_jules.bom_item_history WHERE snapshot_date = DATE'2026-10-01' GROUP BY plant;
```
```sql
SELECT COUNT(*) AS versions_valides, SUM(CASE WHEN a.BOM_alternative IS NOT NULL THEN 1 ELSE 0 END) AS avec_bom_dans_A
FROM v_valid_versions v
LEFT JOIN (SELECT DISTINCT plant, material_number, BOM_alternative FROM dev_lab.lab_jules.bom_item_history
           WHERE snapshot_date = DATE'2026-10-01') a
  ON a.plant = v.plant AND a.material_number = v.material_number AND a.BOM_alternative = v.BOM_alternative
WHERE v.plant IN (SELECT DISTINCT plant FROM dev_lab.lab_jules.bom_item_history WHERE snapshot_date = DATE'2026-10-01');
```

**10. Combien d'AF ont plusieurs versions valides ?**
```sql
SELECT nb_versions, COUNT(*) AS nb_af
FROM (SELECT plant, material_number, COUNT(*) AS nb_versions FROM v_valid_versions GROUP BY plant, material_number)
GROUP BY nb_versions ORDER BY nb_versions;
```
Mesure l'impact du point D1. Résultat 2026-11-05 : 27 216 AF sur ~86 000 ont plusieurs versions valides (jusqu'à 55) et seulement 535 ont un quota :
la règle « plus ancienne version » décide donc pour plus de 26 000 AF. Les contrôles 17 à 19 mesurent si ces versions donnent vraiment des BOM différentes.

**11. Ces AF sont-elles couvertes par un quota ?**
```sql
SELECT COUNT(*) AS af_plusieurs_versions,
       SUM(CASE WHEN q.material_number IS NOT NULL THEN 1 ELSE 0 END) AS avec_quota
FROM (SELECT plant, material_number FROM v_valid_versions GROUP BY plant, material_number HAVING COUNT(*) > 1) m
LEFT JOIN (SELECT DISTINCT ltrim('0', material_number) AS material_number, plant
           FROM prod_gold.production.quotas_arrangement_exposed
           WHERE Start_Validity <= DATE'2026-10-01' AND End_Validity > DATE'2026-10-01') q
  ON q.material_number = m.material_number AND q.plant = m.plant;
```
Les AF sans quota prennent la version la plus anciennement valide.

**12. Fantômes : nombre, sans BOM, imbriqués, boucles**
```sql
SELECT COUNT(*) AS lignes_fantomes, COUNT(DISTINCT component_material_number) AS fantomes_distincts
FROM dev_lab.lab_jules.bom_item_history WHERE snapshot_date = DATE'2026-10-01' AND is_phantom_item;
```
```sql
WITH p AS (SELECT DISTINCT plant, component_material_number AS phantom FROM dev_lab.lab_jules.bom_item_history
           WHERE snapshot_date = DATE'2026-10-01' AND is_phantom_item),
b AS (SELECT DISTINCT plant, material_number, MAX(CASE WHEN is_phantom_item THEN 1 ELSE 0 END) AS has_phantom_child
      FROM dev_lab.lab_jules.bom_item_history WHERE snapshot_date = DATE'2026-10-01' GROUP BY plant, material_number)
SELECT COUNT(*) AS fantomes,
       SUM(CASE WHEN b.material_number IS NULL THEN 1 ELSE 0 END) AS sans_bom,
       SUM(CASE WHEN b.has_phantom_child = 1 THEN 1 ELSE 0 END) AS imbriques
FROM p LEFT JOIN b ON b.plant = p.plant AND b.material_number = p.phantom;
```
```sql
-- boucles : un fantôme qui se contient lui-même (directement ou via un autre fantôme)
SELECT COUNT(*) AS boucles FROM (
  SELECT x.plant, x.material_number, x.component_material_number
  FROM dev_lab.lab_jules.bom_item_history x
  JOIN dev_lab.lab_jules.bom_item_history y
    ON y.snapshot_date = x.snapshot_date AND y.plant = x.plant AND y.material_number = x.component_material_number
  WHERE x.snapshot_date = DATE'2026-10-01' AND x.is_phantom_item AND y.is_phantom_item
    AND (y.component_material_number = x.material_number OR x.component_material_number = x.material_number)
);
```
Attendu : `boucles = 0`. `sans_bom` : ces fantômes resteront signalés `PHANTOM_WITHOUT_BOM`. Si les imbriqués descendent à plus de 7 niveaux, me le dire.

**13. Plusieurs numéros de BOM pour la même alternative (tailles de lot)**
```sql
SELECT COUNT(*) AS alternatives_multi_bom FROM (
  SELECT plant, material_number, BOM_alternative FROM dev_lab.lab_jules.bom_item_history
  WHERE snapshot_date = DATE'2026-10-01' GROUP BY plant, material_number, BOM_alternative
  HAVING COUNT(DISTINCT BOM_number) > 1);
```
Attendu : 0 ou très peu. Sinon, lignes dupliquées dans la prévision : la règle D5 est à décider avant d'utiliser le résultat.

**17. Ces versions multiples donnent-elles des BOM différentes ?** Plusieurs versions qui pointent vers la même alternative donnent la même BOM : le
choix ne compte alors pas.
```sql
SELECT COUNT(*) AS af_plusieurs_versions,
       SUM(CASE WHEN nb_alternatives > 1 THEN 1 ELSE 0 END) AS af_alternatives_differentes
FROM (SELECT plant, material_number, COUNT(*) AS nb_versions, COUNT(DISTINCT BOM_alternative) AS nb_alternatives
      FROM v_valid_versions GROUP BY plant, material_number HAVING COUNT(*) > 1);
```

**18. Pourquoi y a-t-il plusieurs versions ?** Tailles de lot (`bstmi` / `bstma`), statut de contrôle (`prfg_s`) : une version SAP est choisie par l'ordre selon sa taille de lot.
```sql
SELECT trim(prfg_s) AS statut_controle, COUNT(*) AS nb,
       SUM(CASE WHEN try_cast(regexp_replace(trim(bstma), '-$', '') AS DOUBLE) > 0 THEN 1 ELSE 0 END) AS avec_borne_haute_lot
FROM prod_landingzone.sap_latecoere_ecc6.mkal_stack
WHERE file_mode = 'FULL'
  AND extraction_timestamp = (SELECT MAX(extraction_timestamp) FROM prod_landingzone.sap_latecoere_ecc6.mkal_stack WHERE file_mode = 'FULL' AND CAST(extraction_timestamp AS DATE) < DATE'2026-10-01')
  AND trim(mksp) <> 'X'
GROUP BY trim(prfg_s) ORDER BY nb DESC;
```
```sql
SELECT plant, material_number, COUNT(*) AS nb_versions, COUNT(DISTINCT BOM_alternative) AS nb_alternatives
FROM v_valid_versions GROUP BY plant, material_number ORDER BY nb_versions DESC LIMIT 5;
```
Puis, pour un des 5 AF (remplacer `<usine>` et `<article>`), le détail de ses versions :
```sql
SELECT trim(verid) AS verid, trim(stlal) AS alternative_bom, trim(alnal) AS alternative_gamme, trim(bstmi) AS lot_min, trim(bstma) AS lot_max,
       trim(prfg_s) AS statut_controle, trim(text1) AS texte, trim(adatu) AS valide_du, trim(bdatu) AS valide_au
FROM prod_landingzone.sap_latecoere_ecc6.mkal_stack
WHERE file_mode = 'FULL'
  AND extraction_timestamp = (SELECT MAX(extraction_timestamp) FROM prod_landingzone.sap_latecoere_ecc6.mkal_stack WHERE file_mode = 'FULL' AND CAST(extraction_timestamp AS DATE) < DATE'2026-10-01')
  AND trim(mksp) <> 'X' AND trim(werks) = '<usine>' AND ltrim('0', trim(matnr)) = '<article>'
ORDER BY trim(adatu), trim(verid);
```

**19. Contrôle 12 et 13 sur toutes les usines.** Résultats 2026-11-05 sur le sous-ensemble de A : 886 fantômes, 0 sans BOM, 217 imbriqués, 0 boucle, 0 alternative avec plusieurs
numéros de BOM. À refaire après un run complet de A (toutes les usines), ou en limitant A au debug de l'usine de test.

**20. A complet ou debug ?** Le notebook D en debug lit `bom_item_history_debug`, en run complet `bom_item_history`. Avant de lancer D : soit relancer A en `full` (toutes
usines, depuis 2023-05), soit le relancer avec les paramètres de debug pour créer `bom_item_history_debug`.

**21. Branche des articles** — le notebook filtre sur `Branch` (`IS` par défaut). Attendu : colonnes `Material_Number, Plant, Branch`, valeurs `IS` / `AS`, un seul `Branch` par article et usine.
```sql
SELECT Branch, COUNT(*) AS nb FROM prod_gold.master_data.material_master_data_plant_exposed GROUP BY Branch;
```
```sql
SELECT COUNT(*) AS articles_usines_avec_plusieurs_branches FROM (
  SELECT Material_Number, Plant FROM prod_gold.master_data.material_master_data_plant_exposed
  GROUP BY Material_Number, Plant HAVING COUNT(DISTINCT Branch) > 1);
```
```sql
SELECT COUNT(DISTINCT concat_ws('-', a.plant, a.material_number)) AS af_dans_A,
       COUNT(DISTINCT CASE WHEN m.Branch = 'IS' THEN concat_ws('-', a.plant, a.material_number) END) AS af_IS,
       COUNT(DISTINCT CASE WHEN m.Branch IS NULL THEN concat_ws('-', a.plant, a.material_number) END) AS af_sans_branche
FROM dev_lab.lab_jules.bom_item_history a
LEFT JOIN prod_gold.master_data.material_master_data_plant_exposed m
  ON ltrim('0', m.Material_Number) = a.material_number AND m.Plant = a.plant
WHERE a.snapshot_date = DATE'2026-10-01';
```
Attendu : 0 article en double branche ; `af_sans_branche` négligeable.

### Après le premier run (debug : `debug_plant = 2400`, `snapshot_from_date = 2025-01-01`)

**14. Règles appliquées et fantômes**
```sql
SELECT selection_rule, COUNT(DISTINCT concat_ws('-', plant, material_number)) AS nb_af
FROM dev_lab.lab_jules.average_bom_history_debug
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM dev_lab.lab_jules.average_bom_history_debug)
GROUP BY selection_rule;
```
```sql
SELECT explosion_status, COUNT(*) AS nb_lignes
FROM dev_lab.lab_jules.average_bom_history_debug
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM dev_lab.lab_jules.average_bom_history_debug)
GROUP BY explosion_status;
```
Attendu : `LOWEST_ALTERNATIVE` faible, `PHANTOM_WITHOUT_BOM` cohérent avec le contrôle 12, `PHANTOM_DEPTH_EXCEEDED` = 0.

**15. Le notebook reproduit-il l'indicateur existant ?** Au dernier snapshot, la version et l'alternative choisies doivent être celles de la table `average_bom` de production (branche IS seulement, d'où la jointure).
```sql
WITH d AS (SELECT DISTINCT plant, material_number, production_version, BOM_alternative
           FROM dev_lab.lab_jules.average_bom_history_debug
           WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM dev_lab.lab_jules.average_bom_history_debug)),
p AS (SELECT DISTINCT Plant AS plant, Material_Number AS material_number, Production_Version AS production_version, Alternative AS BOM_alternative
      FROM prod_proj.manuf_engineering.average_bom WHERE horizon = 12)
SELECT COUNT(*) AS af_communes,
       SUM(CASE WHEN d.production_version = p.production_version THEN 1 ELSE 0 END) AS meme_version,
       SUM(CASE WHEN d.BOM_alternative = p.BOM_alternative THEN 1 ELSE 0 END) AS meme_alternative
FROM d JOIN p ON d.plant = p.plant AND d.material_number = p.material_number;
```
Attendu : au moins 95 % de même version. Les écarts restants viennent du décalage de date et des AF sans version unique : me donner 5 exemples pour les expliquer.

**16. Trois AF témoins** — donner `plant` + `material_number` de 3 AF dont tu connais la BOM : je compare leurs lignes (`quantity_per_unit`, `explosion_status`) avec ce que tu attends.

## 5. Points ouverts

| # | Question | Défaut du notebook |
|---|---|---|
| D1 | Plusieurs versions valides : quota puis la plus ancienne (average_bom) ou plus petite alternative (multilevel) ? | quota puis la plus ancienne |
| D2 | Quotas non historisés | résolu : historisés depuis `equk_stack` / `equp_stack` |
| D3 | AF sans version utilisable : plus petite alternative ou exclure de la mesure ? | plus petite alternative, marquée |
| D0 | Périmètre | `IS` (décision 2026-11-05) |
| D4 | Vrac, statut usine `05`, `SPLIT`, articles `49…` : exclus de la Prévision 3 ? (= L7, L9) | gardés, marqués |
| D5 | Plusieurs numéros de BOM pour la même alternative (plages de taille de lot `losvn` / `losbs`) : lequel ? | tous gardés (contrôle AMBER) |
| D6 | Rebut du poste fantôme lui-même : inclus dans la quantité des composants éclatés ? | oui (facteur du fantôme multiplié, comme C) |

## 6. Suite

1. Lancer D en debug, vérifier §4.
2. Faire lire `average_bom_history` par C pour la Prévision 3 (à la place de `bom_item_history` + alternative de l'OF / OP).
3. Retirer de C le calcul de la quantité par pièce et l'explosion des fantômes devenus inutiles.
