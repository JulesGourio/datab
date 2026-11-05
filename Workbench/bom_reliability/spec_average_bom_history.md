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

`average_bom_history_ID` (PK) · `snapshot_date` · `plant` · `material_number` · `material_description` · `production_version` ·
`BOM_alternative` · `selection_rule` · `nb_active_versions` · `BOM_number` · `BOM_item_number` (`9999` si éclaté) · `BOM_item_category` ·
`BOM_status` · `component_material_number` · `component_description` · `component_base_unit` · `quantity_per_unit` (rebut compris, `0` si
quantité fixe) · `quantity_per_order` (quantité fixe, rebut compris) · `component_scrap_percentage` · `component_plant_status` ·
`is_phantom_item` · `is_bulk_material` · `explosion_status` · `explosion_level` · `phantom_path` · colonnes techniques `_…`.

Prévision 3 dans C = `quantity_per_unit × quantité finale de l'OF + quantity_per_order` (le rebut est déjà dedans).

## 4. Vérifications à faire sur Databricks (rien n'a pu être exécuté depuis le dépôt)

Noms de champs SAP supposés : `mkal_stack` (`matnr`, `werks`, `verid`, `stlal`, `mksp`, `adatu`, `bdatu`, `file_mode`), `marc_stack.mmsta`,
`quotas_arrangement_exposed` (`quota_number`, `quota`, `material_number`, `plant`, `production_version`, `Start_Validity`, `End_Validity`).

```sql
DESCRIBE prod_landingzone.sap_latecoere_ecc6.mkal_stack;
```
```sql
SELECT file_mode, COUNT(DISTINCT extraction_timestamp) AS nb_extractions, MIN(extraction_timestamp), MAX(extraction_timestamp)
FROM prod_landingzone.sap_latecoere_ecc6.mkal_stack GROUP BY file_mode;
```
```sql
SELECT mksp, COUNT(*) FROM prod_landingzone.sap_latecoere_ecc6.mkal_stack
WHERE extraction_timestamp = (SELECT MAX(extraction_timestamp) FROM prod_landingzone.sap_latecoere_ecc6.mkal_stack)
GROUP BY mksp;
```
```sql
SELECT mmsta, COUNT(*) FROM prod_landingzone.sap_latecoere_ecc6.marc_stack
WHERE extraction_timestamp = (SELECT MAX(extraction_timestamp) FROM prod_landingzone.sap_latecoere_ecc6.marc_stack WHERE file_mode = 'FULL')
GROUP BY mmsta ORDER BY 2 DESC;
```
```sql
DESCRIBE prod_gold.production.quotas_arrangement_exposed;
```

Après un premier run en debug (`debug_plant = 2400`, `snapshot_from_date = 2025-01-01`) :
- répartition de `selection_rule` (la part de `LOWEST_ALTERNATIVE` doit rester faible) ;
- nombre de lignes `explosion_status` différent de `DIRECT` / `EXPLODED` ;
- comparaison avec la BOM actuelle : pour 3 AF témoins, `average_bom_history` du dernier snapshot contre `current_average_bom` / la BOM en cours.

## 5. Points ouverts

| # | Question | Défaut du notebook |
|---|---|---|
| D1 | Plusieurs versions valides : quota puis la plus ancienne (average_bom) ou plus petite alternative (multilevel) ? | quota puis la plus ancienne |
| D2 | Quotas non historisés : accepter l'état actuel ? | oui, signalé |
| D3 | AF sans version utilisable : plus petite alternative ou exclure de la mesure ? | plus petite alternative, marquée |
| D4 | Vrac, statut usine `05`, `SPLIT`, articles `49…` : exclus de la Prévision 3 ? (= L7, L9) | gardés, marqués |
| D5 | Plusieurs numéros de BOM pour la même alternative (plages de taille de lot `losvn` / `losbs`) : lequel ? | tous gardés (contrôle AMBER) |
| D6 | Rebut du poste fantôme lui-même : inclus dans la quantité des composants éclatés ? | oui (facteur du fantôme multiplié, comme C) |

## 6. Suite

1. Lancer D en debug, vérifier §4.
2. Faire lire `average_bom_history` par C pour la Prévision 3 (à la place de `bom_item_history` + alternative de l'OF / OP).
3. Retirer de C le calcul de la quantité par pièce et l'explosion des fantômes devenus inutiles.
