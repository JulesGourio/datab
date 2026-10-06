# BOM reliability — Questions à poser à Genie et tests des sorties

Complète `Spec Output/spec_technique_historisation_bom.md`. Le §1 est la découverte à faire **avant** le build :
chaque réponse lève un [À VÉRIFIER] ou alimente un [TBD] de la spec. Le §2 contient les tests à lancer sur les
tables produites en lab (`dev_lab.lab_jules`). Les résultats vont dans `working/corrections_log`.

---

## 1. Questions à poser à Genie

### 1.0 Consigne à coller en tête de chaque session Genie

> Réponds uniquement avec : (1) la requête SQL que tu as **exécutée**, avec les noms complets catalog.schema.table,
> (2) le résultat brut (tableau), (3) une phrase de constat factuel. Pas de code PySpark, pas de proposition de
> notebook ni d'architecture, pas d'extrapolation. Si une table ou une colonne n'existe pas, dis-le explicitement
> au lieu de la remplacer par une autre. Si le résultat est tronqué, indique le nombre total de lignes.

Pourquoi : sa première réponse mélangeait constats et décisions de conception fausses. On ne lui demande que des faits
vérifiables, la conception reste dans la spec.

### 1.1 Inventaire des historiques (bloquant pour toute la suite)

| # | Question à poser | Ce que ça décide |
|---|---|---|
| H1 | Liste toutes les tables de `prod_landingzone.sap_latecoere_ecc6` dont le nom contient `stack`, avec pour chacune : min et max de la date d'extraction, nombre de dates distinctes, nombre de lignes. | Profondeur réelle de l'historique de A |
| H2 | Existe-t-il, dans `prod_landingzone` ou `prod_bronze`, une table historisée (stack, append, `_history`, table sans suffixe `_latest`) pour **RESB, PLAF, AFKO, AFPO, MKAL, MARC, MARM** ? Pour chacune : nom complet, min/max de la date d'extraction ou d'ingestion. | **Backfill possible de B ou capture à démarrer d'urgence** (spec §4, §6.7) |
| H3 | Dans `prod_bronze.sap_latecoere_ecc6`, les tables `resb`, `stpo`, `stko`, `mast`, `stas` existent-elles **sans** le suffixe `_latest` ? Si oui, leurs colonnes `_meta_*` et min/max de `_meta_extraction_timestamp`. | Un bronze en append serait déjà un historique |
| H4 | Les stacks sont-ils purgés ? Compare le min de la date d'extraction de `stpo_stack` à celui retourné il y a quelques semaines (ou regarde l'historique Delta : `DESCRIBE HISTORY` sur `stpo_stack`, opérations DELETE / VACUUM / OPTIMIZE et leur date). | Overwrite complet de A acceptable ou non (spec §5.10) |
| H5 | Quel est le nom exact de la colonne de date d'extraction dans chaque stack (`extraction_timestamp`, `_meta_extraction_timestamp`…) et dans `resb_latest`, `plaf_latest`, `afko_latest`, `afpo_latest` ? À quelle heure ces tables `_latest` sont-elles rafraîchies (max de `_meta_ingestion_timestamp` sur les 7 derniers jours) ? | Date de snapshot de B et horaire du job |

### 1.2 Régularité des snapshots

| # | Question | Ce que ça décide |
|---|---|---|
| S1 | Pour `stpo_stack`, `stko_stack`, `stas_stack` et `mast_stack` : par date d'extraction, le nombre d'extractions distinctes (timestamps) et le nombre de lignes. Signale les jours avec plus d'une extraction, les semaines sans extraction et les écarts de volume de plus de 20 % d'une date à l'autre. | Dédoublonnage intra-jour, détection des trous |
| S2 | STKO/STPO sont-ils extraits tous les samedis sans exception depuis avril 2023 ? Liste les dates manquantes. | Coupures artificielles de versions |
| S3 | Date exacte du premier snapshot de `mast_stack` et de `stas_stack`. | Constantes de rétro-datation (spec §5.5) |

### 1.3 BOM SAP (STKO / STPO / STAS / MAST)

Sur le **dernier** snapshot de chaque stack, sauf mention contraire.

| # | Question | Ce que ça décide |
|---|---|---|
| B1 | Répartition des lignes de `stpo_stack` et `stko_stack` par `stlty`. | Filtre `M` partout |
| B2 | Un même `stlnr` apparaît-il avec plusieurs `stlty` différents dans `stpo_stack` ? Combien de cas ? | Preuve du fan-out de Genie (spec D2) |
| B3 | Dans `prod_bronze.sap_latecoere_ecc6.mast_latest` : répartition par `werks` × `stlan`. | Utilisations à garder (D12) |
| B4 | Combien de couples (`matnr`, `werks`, `stlan`) ont plusieurs `stlal` dans `mast_latest` ? Donne 5 exemples. | Importance de l'alternative de l'OF |
| B5 | Combien de `stlnr` sont rattachés à plusieurs (`matnr`, `werks`) dans `mast_latest` ? | Duplication volontaire des postes par AF |
| B6 | Dans `stpo_stack` (stlty = M) : nombre de postes, nombre avec `aennr` renseigné, nombre de (`stlnr`, `stlkn`) distincts, nombre de (`stlnr`, `stlkn`, `stpoz`) distincts, nombre avec `lkenz = 'X'`. Même chose pour `stas_stack` et `stko_stack` (`stkoz`). | **Usage des numéros de modification** : la résolution de validité de la spec §5.8 est-elle utile ? |
| B7 | Donne 3 matériels dont la BOM a au moins un poste avec 2 versions (même `stlnr`, `stlkn`, `stpoz` différents) et un `aennr` renseigné, avec toutes les versions (`stpoz`, `datuv`, `lkenz`, `aennr`, `idnrk`, `menge`, `vgknt`, `vgpzl`). | Golden examples de validité (test A5) |
| B8 | Unicité de la clé dans un snapshot : doublons de (`stlty`, `stlnr`, `stlkn`, `stpoz`) dans `stpo_stack`, de (`stlty`, `stlnr`, `stlal`, `stkoz`) dans `stko_stack`, de (`stlty`, `stlnr`, `stlal`, `stlkn`, `stasz`) dans `stas_stack`, de (`matnr`, `werks`, `stlan`, `stlnr`, `stlal`) dans `mast_stack`. | Contrôles RED de Prep |
| B9 | Exemples de valeurs brutes de `menge`, `ausch`, `avoau`, `bmeng`, `ewahr` contenant un `-`, une `,` ou des espaces. Nombre de `menge` négatifs. | Fonction de signe SAP (spec §5.4) |
| B10 | Distribution de `ausch` (rebut composant) : nombre de postes à 0, à NULL, > 0, top 10 des valeurs. Même chose pour `avoau`. Nombre de postes avec `netau = 'X'`. | Ampleur réelle des rebuts dans les BOM |
| B11 | Nombre de postes avec `dumps = 'X'` (fantôme), `schgt = 'X'` (vrac), `fmeng = 'X'` (quantité fixe), et répartition par `postp`. | Volume des cas particuliers |
| B12 | Nombre de composants fantômes dont la BOM fantôme contient elle-même un fantôme (profondeur ≥ 2). Profondeur max. | `MAX_PHANTOM_DEPTH` |
| B13 | Pour les composants fantômes : ont-ils une entrée `mast_latest` ? Avec quelle `stlan` et quelle `stlal` ? | Alternative de la BOM fantôme (spec §8.6) |
| B14 | Distribution de `bmeng` dans `stko_stack` : nombre à 1, à 0 ou NULL, autres valeurs. | Contrôle `BOM_base_quantity > 0` |
| B15 | Nombre de postes STPO dont `meins` diffère de l'unité de base du composant (`mara.meins`), et parmi eux combien n'ont pas de ligne correspondante dans `prod_bronze.sap_latecoere_ecc6.marm_latest`. | Conversion d'unité (spec D8) |
| B16 | La colonne `andat` existe-t-elle dans `stas_stack` ? Colonnes complètes de `stas_stack`. | Rétro-datation STAS |
| B17 | Combien de postes STPO (stlty = M) n'ont aucune ligne STAS correspondante sur le même snapshot ? Combien d'alternatives STKO n'ont pas de lien MAST ? | Pertes des jointures INNER (AMBER) |
| B18 | `prod_silver.production.bom` : colonnes, nombre de lignes, filtres appliqués (stlty, stlan, lkenz, validité). Quel notebook la produit ? | Règle zéro : réutiliser ses règles |

### 1.4 Besoins des OP/OF (RESB, PLAF, AFKO, AFPO)

| # | Question | Ce que ça décide |
|---|---|---|
| R1 | Dans `prod_bronze.sap_latecoere_ecc6.resb_latest` : répartition par `bdart` × `rsart`, avec pour chacune le nombre de lignes avec `aufnr` renseigné et avec `plnum` renseigné. | Les besoins des OP sont-ils dans RESB ? (spec Q8) |
| R2 | Colonnes de `resb_latest` : `ausch`, `avoau`, `netau`, `dumps`, `baugr`, `schgt`, `xloek`, `kzear`, `postp`, `posnr`, `stlty`, `stlnr`, `stlkn`, `stpoz`, `vornr` existent-elles ? | Mapping Prep1 de B |
| R3 | Colonnes de `prod_gold.mrp.reservation_mrp_sap` et nombre de lignes par type d'ordre (OP / OF). Contient-elle les besoins des OP ? Quels filtres applique-t-elle par rapport à `resb_latest` (compare les nombres de lignes) ? | Capturer depuis le Gold ou depuis le bronze |
| R4 | Unicité de (`rsnum`, `rspos`, `rsart`) dans `resb_latest`. | Clé naturelle de B |
| R5 | Pour un OF terminé il y a 1, 6, 12, 24 mois (un exemple de chaque) : ses lignes RESB existent-elles encore ? À partir de quelle ancienneté les réservations disparaissent-elles (archivage) ? | Faisabilité du proxy du mode dégradé (spec §6.7) |
| R6 | Proportion des lignes RESB d'OF sans `stlnr` (composants ajoutés à la main), par usine. | Différence entre la BOM et la réservation |
| R7 | Pour une ligne RESB fantôme (`dumps = 'X'`) : donne l'OF et toutes ses lignes RESB, avec `posnr`, `baugr`, `matnr`, `bdmng`. Comment les composants éclatés sont-ils rattachés au fantôme ? | Exclusion des fantômes et poste « 9999 » (spec §8.4) |
| R8 | `afpo_latest` : taux de remplissage de `plnum` sur les OF créés depuis 2024. Existe-t-il des OF avec plusieurs `posnr` ? | Lien OP → OF |
| R9 | `plaf_latest` : colonnes `gsmng`, `psttr`, `pedtr`, `paart`, `stlal`, `stlan`, `verid`, `rsnum`, `plwrk`. Nombre d'OP par usine et par horizon (`psttr` dans 0–3, 3–6, 6–12 mois). | Volume quotidien à capturer |
| R10 | Une même ligne RESB d'OP change-t-elle souvent ? Si un historique existe (H2), nombre moyen de versions par (`rsnum`, `rspos`) sur 30 jours, en excluant `enmng` et `kzear`. | Volume de la table SCD2 B |
| R11 | `bdmng` : comment se compare-t-il à `stpo.menge / stko.bmeng × quantité de l'ordre × (1 + ausch/100)` sur 20 OF sans ajout manuel ? Existe-t-il un champ de quantité « sans rebut » dans RESB (`nomng`, `esmng`…) ? | Formule de la Prévision 2 (D4) |
| R12 | Le rebut d'ensemble (`marc.ausss`) est-il renseigné pour des AF ? Combien ? Est-il exposé dans `prod_gold.master_data.material_plant` ? | Formule de la Prévision 2 |

### 1.5 En-tête OF et périmètre

| # | Question | Ce que ça décide |
|---|---|---|
| W1 | Colonnes de `prod_gold.production.work_orders_sap_exposed`. Quels champs SAP sont derrière `real_start_date`, `real_end_date`, `creation_date`, `quantity_planned`, `quantity_delivered`, `quantity_scrapped` ? Existe-t-il une colonne pour l'OP d'origine (`plnum`), l'alternative (`stlal`), la version de production (`verid`), la date TECO ? | Périmètre T1/T2, rattachement à T0 (spec §8.3) |
| W2 | Nombre d'OF commencés après le 2026-01-01 et terminés avant le 2026-04-01, par usine et par centre de profit. Même chose sans date de fin réelle. | Volume par période, OF jamais clôturés |
| W3 | Répartition des statuts et des types d'ordre des OF. Lesquels sont annulés / à exclure ? | Filtre du périmètre |
| W4 | Format de `work_order_number` (zéros non significatifs ?) comparé à `Work_order` de `part_movement_exposed` et `aufnr` de `resb_latest`. | Jointures |

### 1.6 Mouvements de stock (consommations)

| # | Question | Ce que ça décide |
|---|---|---|
| M1 | Colonnes de `prod_gold.supply_chain_logistic.part_movement_exposed`. Existe-t-il une colonne débit/crédit (`SHKZG`) ? La quantité est-elle en unité de base ou en unité de saisie ? | Signe et unité de la Conso |
| M2 | Pour les mouvements avec `Work_order` renseigné : répartition par `Movement_type` (nombre, quantité totale, nombre d'OF), sur 12 mois. | Liste des Conso 1 / 2 (D6) |
| M3 | Parmi ces mouvements, combien portent sur le matériel fabriqué par l'OF lui-même (entrée de l'AF) ? Par type de mouvement. | Exclusion de l'AF |
| M4 | Mouvements d'inventaire (701/702, 711/712, autres) : sont-ils imputés à un OF ou seulement à un magasin / centre de coût ? Volume par usine. | Méthode de la Conso 3 (D7) |
| M5 | Combien de couples (OF, composant) ont une consommation nette négative (262 > 261) ? | Règle de la conso négative (D8) |
| M6 | Profondeur de l'historique : min de `Posting_date`, nombre de mouvements par année. | Périodes calculables |
| M7 | Existe-t-il des types de mouvement « casse » ou de rebut spécifiques à Latécoère (types Z) imputés aux OF ? | Définition de la Conso 2 |

### 1.7 Données de référence pour le rapport

| # | Question | Ce que ça décide |
|---|---|---|
| D1 | Colonnes de `prod_gold.master_data.material_plant` et `prod_gold.master_data.material_exposed` : centre de profit, délai d'approvisionnement (`plifz`, `wzeit`), type d'approvisionnement spécial (`sobsl`), prix standard, unité de base, famille, classe, code ABC / 0 / L. | Colonnes de la page 2 |
| D2 | Quelle table Gold donne la description des divisions et des centres de profit ? Nom complet et colonnes. | Libellés |
| D3 | Quelle table donne le prix des composants en EUR ? Avec quelle date de valeur ? | Colonne prix |
| D4 | Que signifie « PF usage » dans les données existantes ? Une colonne porte-t-elle ce nom ? | D11 |

### 1.8 Golden examples à demander (pour la Test Definition)

| # | Question |
|---|---|
| E1 | Un AF dont la BOM a changé (quantité ou rebut) entre avril 2025 et avril 2026, avec la date du changement et les valeurs avant/après dans le stack. |
| E2 | Un OF terminé récemment, issu d'un OP (`afpo.plnum` renseigné), avec un composant fantôme dans sa BOM. Donne ses lignes RESB et ses mouvements. |
| E3 | Un OF avec au moins un composant consommé qui n'était pas réservé, et un composant réservé jamais consommé. |
| E4 | Un AF avec plusieurs alternatives de BOM et des OF sur au moins deux alternatives différentes. |
| E5 | Pour un centre de profit choisi par le métier : nombre d'AF et d'OF sur la période par défaut (T0 = J−6 mois, P1 = P2 = 3 mois). |

Chaque exemple est ensuite vérifié dans SAP (CS03 à date, CO03, MB51, MD04) par le métier : c'est la référence des
tests A5, B6 et C8 ci-dessous.

---

## 2. Tests des sorties (lab : `dev_lab.lab_jules`)

Convention : chaque test a un **résultat attendu** ; tout écart est consigné dans le corrections_log avec l'action
prise. `LAB = dev_lab.lab_jules`.

### 2.1 A — `dev_lab.lab_jules.bom_item_history`

| # | Test | Attendu |
|---|---|---|
| A1 | PK unique et non nulle | 0 doublon, 0 NULL |
| A2 | Pas de chevauchement : pour une même clé de version, `recorded_from_date < recorded_to_date` et aucun intervalle ne commence avant la fin du précédent | 0 ligne fautive |
| A3 | **Reconstitution d'un snapshot** : pour 3 dates de snapshot S (la première, une au milieu, la dernière), les lignes avec `recorded_from_date <= S < recorded_to_date` sont égales, après les mêmes filtres, aux lignes du stack à la date S | Différence vide dans les deux sens (`EXCEPT`) |
| A4 | `is_current = true` ⇔ `recorded_to_date = '9999-12-31'` ; nombre de lignes courantes = lignes du dernier snapshot joint | Égalité |
| A5 | BOM résolue « connue à K, valide à D » pour les golden examples E1/E4 = CS03 à la date D | Mêmes composants, quantités, rebuts |
| A6 | Au plus une ligne par (usine, AF, utilisation, alternative, nœud) après la résolution de validité, pour 10 dates aléatoires | 0 doublon |
| A7 | Quantités négatives conservées : nombre de `component_quantity < 0` = nombre de `menge` négatifs du stack (B9) | Égalité |
| A8 | Aucune ligne `stlty ≠ M`, aucune utilisation hors `BOM_USAGES` | 0 |
| A9 | Taux de NULL de `component_quantity_in_base_unit` | Cohérent avec B15 |
| A10 | Lignes `_is_backdated_link = true` : aucune avec `recorded_from_date` < date de création du lien | 0 |
| A11 | Compression : nombre moyen de versions par nœud, et rapport lignes du stack / lignes de A | Ordre de grandeur cohérent avec B6 (forte compression attendue) |
| A12 | Deux runs successifs sans nouveau snapshot donnent une table identique (même nombre de lignes, même somme de contrôle sur la PK) | Identique |
| A13 | Durée du run et taille du cluster | Notées pour dimensionner le job |

Exemple pour A3 :

```sql
WITH s AS (SELECT DATE'2025-01-04' AS d)
SELECT BOM_number, BOM_node, BOM_item_counter, component_material_number, component_quantity
FROM dev_lab.lab_jules.bom_item_history, s
WHERE recorded_from_date <= s.d AND s.d < recorded_to_date
EXCEPT
SELECT trim(stlnr), trim(stlkn), trim(stpoz), <idnrk sans zéros>, <menge signé>
FROM prod_landingzone.sap_latecoere_ecc6.stpo_stack, s
WHERE to_date(extraction_timestamp) = s.d AND stlty = 'M'
  AND <stlnr/stlkn présents dans STAS et MAST à la même date>;
-- puis la même requête dans l'autre sens
```

### 2.2 B — `dev_lab.lab_jules.order_component_requirement_history`

| # | Test | Attendu |
|---|---|---|
| B1 | PK unique, non nulle | 0 |
| B2 | Au plus une ligne `is_current` par (`reservation_number`, `reservation_item`, `reservation_record_type`) | 0 doublon |
| B3 | **Idempotence** : relancer le notebook le même jour | 0 ligne insérée, 0 ligne fermée (comparer `DESCRIBE HISTORY` : `numTargetRowsInserted/Updated = 0`) |
| B4 | Run du lendemain : lignes fermées + insérées = lignes réellement modifiées dans `resb_latest` (comparaison des deux extractions) | Égalité |
| B5 | Nombre de lignes courantes = nombre de lignes de `resb_latest` après filtres | Égalité |
| B6 | **Garde-fou de complétude** : simuler un snapshot vide (debug sur une usine inexistante) | Le notebook s'arrête en RED, la table n'est pas modifiée |
| B7 | Garde-fou de date : relancer avec une date de snapshot antérieure au max de `recorded_from_date` | Skip propre, table inchangée |
| B8 | Conversion OP → OF (golden E2) : la ligne de l'OP se ferme le jour de la conversion, les lignes de l'OF apparaissent le même jour, `origin_planned_order_number` de l'OF = numéro de l'OP | Vrai |
| B9 | Aucune ligne `recorded_from_date > current_date()` ni `recorded_to_date <= recorded_from_date` | 0 |
| B10 | Répartition `order_category` (OP / OF) et `requirement_type` | Cohérente avec R1 |
| B11 | AF (`material_number`) renseigné après jointure d'en-tête | ≥ 99 % [seuil à confirmer] |
| B12 | `enmng` / `kzear` absents : une sortie de stock sur un OF ne crée pas de nouvelle version | Vrai sur un OF en cours |
| B13 | Après 7 jours de capture : taille de la table et nombre de versions par jour | Noté pour la volumétrie |

### 2.3 C — tables Proj

| # | Test | Attendu |
|---|---|---|
| C1 | PK unique par table | 0 |
| C2 | Périodes : aucune avec `T2_date` > date du run ; une seule période `is_default_period` = (J−6 mois arrondi au 1er du mois, 3, 3) | Vrai |
| C3 | Périmètre : nombre d'OF par période = W2 (même filtre calculé à la main) | Égalité |
| C4 | **Pas de fan-out des consommations** : pour la période par défaut, Σ `consumption_1_quantity` de la table composant = Σ des mouvements des OF du périmètre (hors AF), calculée directement sur `part_movement_exposed` | Égalité exacte |
| C5 | Idem Σ `forecast_1_quantity` = Σ `requirement_quantity` des lignes B prises pour ces OF à T0 (hors fantômes, hors supprimés) | Égalité |
| C6 | Répartition `forecast_source` | Lue et commentée avec le métier (part de `NONE` / `RECONSTRUCTED`) |
| C7 | Aucun composant fantôme dans la sortie (jointure sur `is_phantom_item` de A et `SOBSL = 50`) | 0 |
| C8 | Golden examples E1–E3 : prévisions, consos, erreur et statut calculés à la main dans Excel = sortie | Égalité |
| C9 | `error_percentage_*` dans [0, 1] ; 0 quand prévision = conso ; 1 quand l'une des deux est 0 et pas l'autre | Vrai |
| C10 | Statut cohérent : `OVERSTOCK` ⇔ prévision > conso, `SHORTAGE` ⇔ prévision < conso | 0 incohérence |
| C11 | Fiabilité par AF = 1 − moyenne des erreurs de ses composants (recalcul SQL) | Égalité |
| C12 | Prévision 2 ≤ Prévision 1 sur chaque ligne | 0 exception |
| C13 | Composants présents seulement en prévision ou seulement en conso : bien présents (1 ligne), avec l'autre côté à 0 | Vrai (E3) |
| C14 | Conso 3 entièrement NULL en v1 | Vrai |
| C15 | Comparaison avec le calcul manuel actuel du métier sur 1 AF et 1 période | Écart expliqué ligne à ligne |

### 2.4 Checklist conventions (avant le passage sur Bitbucket)

- Aucun `prod_`, `dev_`, `uat_`, `dev_lab`, `lab_jules` en dur dans les notebooks (recherche texte).
- Aucun `print(`, `.union(`, `.saveAsTable(`, `.cast(` nu ; `try_cast` partout sur les champs SAP.
- Sections LEAP complètes, `#N/A` pour les sections vides, header complet.
- Toutes les vérifications GX dans `# Quality Checks`, avec RED sur les PK et sur l'unicité des clés de jointure.
- Contraintes `gold_bom_item_history_PK`, `gold_order_component_requirement_history_PK` présentes
  (`system.information_schema.table_constraints`).
- Vues `_exposed` créées.
- Cellules de test ad hoc (requêtes de ce fichier) retirées des notebooks.
