# Guide de présentation — notebook `create_gold_bom_item_history` (notebook A)

Fichier : `data_asset/production/bom_history/create_gold_bom_item_history.py`
Table produite : `{env}_gold.production.bom_item_history` (+ vue `bom_item_history_exposed`)

Ce guide suit le notebook **cellule par cellule**. Pour chaque cellule, on trouve :
- **Ce qu'elle fait** ;
- **Pourquoi** elle le fait (la raison métier ou technique) ;
- **Ce qu'il faut savoir** : chiffres mesurés en lab, pièges, et les **questions encore ouvertes pour le métier**.

La synthèse des questions métier est au §12, et une fiche de présentation orale en 10 minutes au §13.

---

## 0. Le notebook en une phrase, et pourquoi il existe

> **Il reconstitue, pour chaque 1er du mois depuis mai 2023, la BOM de production de chaque article fabriqué (AF),
> telle que SAP la connaissait ce jour-là.**

**Pourquoi c'est nécessaire.** Le besoin métier (UC93) est de comparer ce qui a été consommé sur les ordres de
fabrication (OF) avec ce que la BOM prévoyait **avant** la production. Or SAP et les tables Bronze `*_latest` ne
gardent que la BOM **d'aujourd'hui**. Comparer la consommation de mars avec la BOM d'octobre n'a pas de sens : la
BOM a pu changer entre-temps (modifications d'ingénierie, nouveaux postes, quantités corrigées). C'est exactement le
défaut des indicateurs existants (« Average BOM », notebook forecast) : ils utilisent la BOM actuelle.

**Où trouver le passé.** La landing zone garde chaque extraction SAP complète dans des tables `*_stack` (une
partition par `extraction_timestamp`), jamais purgées (vérifié : seulement des `COPY INTO` et des `VACUUM` de
fichiers). C'est la seule source d'historique.

**Son rôle dans le projet.** A fournit la **Prévision 3** (BOM standard). B fournit les Prévisions 1 et 2 (besoins
des OP / OF). C calcule la fiabilité. A est aussi réutilisable seul pour toute question « à quoi ressemblait la BOM
à la date X ».

### Les tables SAP utilisées (vocabulaire indispensable)

| Table SAP | Contenu | Clé utile | Fréquence de la pile |
|---|---|---|---|
| **MAST** | Lien article ↔ BOM : quel AF, dans quelle usine, utilise quelle BOM (`stlnr`) et quelle alternative | `matnr`, `werks`, `stlan`, `stlnr`, `stlal` | quotidienne depuis 2024-06-11 |
| **STKO** | En-tête de BOM par alternative : quantité de base, statut | `stlnr`, `stlal` | hebdomadaire depuis 2023-04-23 |
| **STAS** | Affectation des postes (nœuds) aux alternatives | `stlnr`, `stlal`, `stlkn` | quotidienne depuis 2024-06-11 |
| **STPO** | Postes de BOM : composant, quantité, unité, rebut | `stlnr`, `stlkn` | hebdomadaire depuis 2023-04-23 |
| **MARC** | Données article × usine : fantôme (`sobsl`), rebuts (`kausf`, `ausss`) | `matnr`, `werks` | quotidienne, avec extractions partielles `DELTA` |
| **MARM** | Conversions d'unités propres à l'article | `matnr`, `meinh` | quotidienne, avec `DELTA` |
| **MARA / MAKT** | Unité de base / description de l'article | `matnr` | Bronze `_latest` (pas d'historique) |

Grain de la table : **1 ligne = 1 snapshot (1er du mois) × 1 usine × 1 AF × 1 usage × 1 alternative × 1 poste de BOM**.

---

## 1. En-tête (première cellule markdown)

**Ce qu'elle fait.** Décrit l'objet, les complexités, le job prévu, les tables lues et écrites (format imposé par
les guidelines LEAP, vérifié par le Convention Checker).

**Pourquoi.** Quelqu'un qui reprend le notebook doit comprendre sans lire le code. Les « Highlighted complexities »
résument les 6 pièges de ce notebook : sources = piles et pas Bronze, rétro-datation avant 2024-06-11, changements
d'ingénierie (ECN), `stlnr` unique seulement par catégorie `stlty`, nombres SAP avec signe moins à la fin, fantômes.

## 2. Cellules `%pip install` et `restartPython` (lab seulement)

**Ce qu'elles font.** Installent `great_expectations 0.18.22` puis redémarrent Python.

**Pourquoi.** Le module `great_expectations.dataset` (qui contient `SparkDFDataset`, utilisé par les contrôles qualité
LEAP) n'existe plus à partir de la version 1.0. Sur le calcul serverless, la version installée par défaut est trop
récente. **À retirer avant Bitbucket** : en production, la bibliothèque est déclarée sur la tâche du job.

## 3. Technical debt

**Ce qu'elle fait.** Liste ce qui est connu comme imparfait, avec l'endroit du code concerné.

**Points à présenter :**
- Lecture directe de la landing zone (pas de Bronze historisé) ; le mode `incremental` n'est qu'une optimisation.
- Règle de statut alignée sur la Silver officielle (`02` et vide exclus), statut `12` gardé → **question métier S8**.
- Écarts restants avec la Silver officielle (`cslty = X`, liens MAST sans usine) → **question S9**.
- Fonctions utilitaires dupliquées avec B (`sap_number`, `month_starts`…) → à déplacer dans `leap_utils`.
- Rétro-datation MAST / STAS avant 2024-06-11 : un lien supprimé entre 2023-04 et 2024-06 sans numéro de
  modification manque pour cette période (limite connue, non corrigeable : la donnée n'existe pas).
- Conversions d'unités de même dimension codées en dur (`ISO_UNIT_FACTORS`) car la table SAP T006 n'est pas dans le lakehouse.
- Unité de base et descriptions prises dans la donnée **actuelle** (MARA, MAKT), pas historisées.
- Lignes répétées dans certains fichiers d'extraction MARC / MARM : dédoublonnées (cause : le fichier SAP lui-même).

## 4. Configuration

### 4.1 Config Standard Package Imports
**Fait.** Importe `datetime`, `time`, les fonctions PySpark (toujours via `f.`), `Window`.
**Lab seulement :** `sys.path.insert(...)` (pour trouver `leap_utils` dans un dossier du workspace) et le patch qui
neutralise `persist` / `cache` (refusés sur serverless, appelés par GX). Les deux sont à retirer avant Bitbucket.

### 4.2 Config LEAP Function Imports
**Fait.** Importe `table_utils` (sauvegarde, zéros de tête, clés, vue), `logger`, `gx_validation`, `SparkDFDataset`.
**Pourquoi.** Règle LEAP : si une fonction existe dans `leap_utils`, on l'utilise (une réimplémentation coûte des points au Convention Checker).

### 4.3 Config Widgets (une seule cellule crée tous les paramètres)

| Widget | Défaut | Rôle |
|---|---|---|
| `pipeline_write_env` | `dev` | Environnement d'écriture |
| `pipeline_read_env` | `dev` | Environnement des tables d'un autre data asset du même pipeline |
| `reference_read_env` | `prod` | Environnement des sources (piles, Bronze) |
| `by_pass_quality_checks` | `false` | `true` = les contrôles RED deviennent AMBER (debug / reprise seulement) |
| `lab_target_schema` | vide | En lab : `dev_lab.lab_jules` → toutes les écritures y vont |
| `debug`, `log_level` | `False`, `info` | Standard LEAP |
| `snapshot_from_date` | vide | Ne calcule que les mois à partir de cette date (run court) |
| `debug_plant` | vide | Ne garde qu'une usine (run court) |
| `rebuild_mode` | `full` | `full` = tout reconstruire ; `incremental` = n'ajouter que les mois manquants |

**Pourquoi une seule cellule et la relecture des valeurs.** La règle LEAP impose `removeAll()` avant les widgets.
Sans précaution, relancer la cellule remettait les valeurs par défaut et faisait disparaître les widgets créés dans
une autre cellule. La cellule lit donc les valeurs déjà saisies avant de tout recréer.

### 4.4 Debug Boolean
**Fait.** Lit les valeurs des widgets de debug dans des constantes (`DEBUG`, `SNAPSHOT_FROM_DATE`, `DEBUG_PLANT`, `REBUILD_MODE`).

### 4.5 Config logger
**Fait.** Crée le logger LEAP. **Pourquoi.** `print()` est interdit ; les messages vont dans les logs du job.

### 4.6 Config Output Table
**Fait.** Calcule le nom complet de la table de sortie et décide si le chargement est incrémental.
- En lab, catalogue et schéma viennent de `lab_target_schema`.
- **Run de debug** (`debug_plant` ou `snapshot_from_date` renseigné) → table `bom_item_history_debug`.
  **Pourquoi :** un run partiel ne doit jamais écraser l'historique complet.
- `incremental` n'est actif que si la table existe déjà ; il est **refusé avec `debug_plant`** (on compléterait un
  historique complet avec les données d'une seule usine).

**Pourquoi cette cellule est en Configuration.** Le calendrier (§6.1) a besoin de lire les mois déjà stockés.

### 4.7 Config Business Constants
| Constante | Valeur | Pourquoi |
|---|---|---|
| `BOM_CATEGORY` | `M` | BOM d'article. Le numéro `stlnr` n'est unique que par catégorie : sans ce filtre, une BOM de coût (`K`) se mélangerait à une BOM d'article de même numéro |
| `BOM_USAGES` | `["1"]` | Usage production (99,97 % des liens MAST). Les autres usages (ingénierie, coût…) ne pilotent pas les OF |
| `PHANTOM_SPECIAL_PROCUREMENT` | `50` | `MARC.sobsl = 50` = ensemble fantôme. `55` vérifié (X4) : ce n'est pas un fantôme |
| `LATEST_ROW_ORDER` | `_ingestion_timestamp`, `_stack_row_id` | Pour garder la dernière ligne quand un fichier répète une ligne |
| `FULL_EXTRACTION` | `FULL` | MARC et MARM ont aussi des extractions `DELTA` (seulement les changements du jour) : les lire ferait disparaître les articles non modifiés |
| `EXCLUDED_BOM_STATUS` | `02`, vide | Même règle que la Silver officielle (BOM bloquée / sans statut) |
| `ISO_UNIT_FACTORS` | IN→M, MM→M, G→KG… | Conversions de même dimension que SAP fait sans MARM (table T006) |

### 4.8 Config Helper Functions
| Fonction | Rôle | Pourquoi |
|---|---|---|
| `sap_number` | Texte SAP → nombre | SAP écrit `1.000-` pour −1 ; un `cast` direct donne NULL ou une erreur |
| `sap_flag` | `'X'` → `True`, sinon `False` | Les booléens SAP sont `X` / vide |
| `sap_date` | `yyyyMMdd` → date | `try_to_date` : pas d'erreur sur une date vide ou `00000000` |
| `month_starts` | Liste des 1ers du mois | Le calendrier des snapshots |
| `build_snapshot_mapping` | Pour chaque 1er du mois K : la **dernière extraction strictement avant K** | « Ce que SAP savait au matin du 1er ». Option `backdate_first` pour MAST / STAS |
| `list_partition_timestamps` | Dates d'extraction lues dans les métadonnées (`SHOW PARTITIONS`) | Aucune lecture de données : instantané même sur des milliards de lignes |

---

## 5. Inputs

### 5.1 Landing zone stacks
**Fait.** Déclare les 6 piles (STPO, STKO, STAS, MAST, MARC, MARM). Rien n'est lu à ce moment (Spark est paresseux).
**Pourquoi la landing zone.** Les tables Bronze `_latest` n'ont qu'une extraction. Règle LEAP : chaque source est
déclarée **une seule fois** dans Inputs.

### 5.2 Bronze tables
**Fait.** MARA (unité de base) et MAKT (descriptions).
**Pourquoi pas la Gold `material_exposed`.** 37 387 composants de BOM en sont absents (articles obsolètes ou remplacés,
encore présents dans d'anciennes BOM) — constaté au lab run 2.

### 5.3 Extraction calendars
**Fait.** Liste les dates d'extraction de chaque pile ; pour MARC / MARM, seulement celles des fichiers `FULL`.

---

## 6. Data Preparation

### 6.1 Prep1 — Snapshot calendar
**Fait.** Liste les 1ers du mois depuis la première extraction STPO (2023-04-23 → premier snapshot 2023-05-01)
jusqu'au mois courant ; en `incremental`, retire les mois déjà stockés (et sort proprement s'il n'y a rien à faire).
Puis, pour chaque table, construit la correspondance « snapshot → extraction utilisée ».
- STPO / STKO : extraction hebdomadaire précédente.
- MAST / STAS / MARC / MARM : `backdate_first=True` → avant leur première extraction, on réutilise la première
  (marquée `_is_backdated`).

**Pourquoi le 1er du mois.** Décision métier : snapshots **mensuels** pour l'instant (la demande parle aussi d'une
tendance hebdomadaire → question ouverte). La période T0 de la fiabilité est toujours un 1er du mois.

**`select_extractions`** : ne lit que les partitions utiles (élagage de partitions) et étiquette chaque ligne avec
son `snapshot_date`. Une même extraction peut servir deux mois si une semaine d'extraction manque : voulu.

### 6.2 Prep2 — STPO (postes de BOM)
**Fait.** Garde la catégorie `M`, renomme les champs SAP en noms métier, convertit nombres / dates / indicateurs,
retire les postes pas encore valides au snapshot ou supprimés, enlève les zéros de tête du composant.

| Champ SAP | Colonne | Sens |
|---|---|---|
| `stlnr`, `stlkn` | `BOM_number`, `BOM_node` | Identifiant du poste (nœud) dans la BOM |
| `posnr`, `postp` | `BOM_item_number`, `BOM_item_category` | Numéro de poste affiché, catégorie (`L` stock, `N`, `D` document, `T` texte…) |
| `idnrk` | `component_material_number` | Composant |
| `menge`, `meins` | `component_quantity`, `component_unit` | Quantité pour la quantité de base de l'AF |
| `ausch`, `netau` | `component_scrap_percentage`, `is_net_scrap` | Rebut composant du poste |
| `fmeng` | `is_fixed_quantity` | Quantité fixe par OF (pas proportionnelle à la quantité fabriquée) |
| `schgt` | `is_bulk_material` | Matière en vrac |
| `datuv`, `aennr`, `vgknt` | validité, numéro de modification, nœud précédent | Traçabilité des modifications d'ingénierie |

**Pourquoi les zéros de tête.** SAP stocke `000000000001234567` ; les autres tables Gold `1234567`. Sans nettoyage des
deux côtés, une jointure renvoie silencieusement 0 ligne.

### 6.3 Prep3 — STAS (affectation des postes aux alternatives)
**Fait.** Pour chaque (alternative, nœud), garde le **dernier enregistrement connu au snapshot** (date de validité,
puis compteur) et l'élimine s'il est une suppression (`lkenz = X`). Ignore les enregistrements créés après le
snapshot (utile pour les mois rétro-datés).

**Pourquoi c'est le cœur du notebook.** Une modification d'ingénierie (ECN) ne modifie pas un poste : elle crée un
**nouveau nœud** et clôt l'ancien par un enregistrement STAS de suppression. Sans cette règle, on compterait l'ancien
et le nouveau poste. Vérifié au lab : 1 079 402 nœuds clos par ECN exclus, aucun double comptage.

### 6.4 Prep4 — STKO (en-tête)
**Fait.** Quantité de base (`bmeng`), statut ; dernier en-tête par alternative, statuts `02` / vide et en-têtes
supprimés exclus.
**Pourquoi la quantité de base.** Les quantités des postes sont données pour `BOM_base_quantity` unités d'AF
(3,5 % des BOM ont une base différente de 1). Il faut diviser par elle pour avoir une quantité par pièce (fait dans C).

### 6.5 Prep5 — MAST (article ↔ BOM)
**Fait.** Donne l'AF et l'usine de chaque BOM ; usage `1` seulement ; 80 liens sans usine exclus ; pour les mois
rétro-datés, seulement les liens créés avant le snapshot. Filtre `debug_plant` appliqué ici (toute la suite en hérite).
Garde la plage de taille de lot (`losvn`, `losbs`) : plusieurs alternatives peuvent dépendre de la taille de l'OF.

### 6.6 Prep6 — MARC (article × usine)
**Fait.** Type d'approvisionnement spécial (fantôme), rebut composant de l'article (`kausf`), rebut d'ensemble de
l'AF (`ausss`). Extractions `FULL` seulement, lignes répétées dédoublonnées. Deux versions renommées : côté
composant et côté AF.

**Ce que les données ont montré sur les rebuts (important pour le métier) :**
- SAP prend le rebut du **poste** (`ausch`), sinon celui de la **fiche article** (`kausf`). Sur les réservations
  d'OF : 83 % du rebut vient de `kausf`, 17 % est saisi à la main, quasi rien du poste de BOM.
- Le rebut d'ensemble de l'AF (`ausss`) **n'est jamais appliqué** aux OF chez Latécoère (`gasmg = 0` sur 82 OF testés).
- Le rebut d'ensemble des **articles achetés** n'atteint jamais les OF : il ne sert qu'aux achats.

### 6.7 Prep7 — Conversion d'unités
**Fait.** MARM donne `1 unité alternative = umrez / umren unités de base` ; MARA donne l'unité de base actuelle ;
`ISO_UNIT_FACTORS` couvre les conversions de même dimension.
**Pourquoi.** Réservations et mouvements de stock sont en **unité de base** ; 7,6 % des postes de BOM sont dans une
autre unité (pouces → mètres = 96 % des cas sans MARM). Sans conversion, on comparerait des pouces à des mètres.

### 6.8 Prep8 — Descriptions (MAKT)
**Fait.** Description française, sinon anglaise. **Pourquoi.** Le français est la langue renseignée pour tous les articles.

---

## 7. Data Transformations

| Cellule | Jointure | Type | Pourquoi |
|---|---|---|---|
| Tr. 1 | STPO × STAS sur (snapshot, BOM, nœud) | INNER | Un poste sans affectation n'appartient à aucune alternative (97 sur 4,7 M) |
| Tr. 2 | × STKO sur (snapshot, BOM, alternative) | INNER | Sans en-tête valide (supprimé, statut exclu), l'alternative n'existe pas |
| Tr. 3 | × MAST sur (snapshot, BOM, alternative) | INNER | Donne AF + usine. Une BOM partagée (219 numéros) est répétée par AF / usine : c'est le grain voulu |
| Tr. 4 | × MARC côté composant et côté AF | LEFT | Ajoute fantôme et rebuts sans perdre de poste ; calcule `is_phantom_item` |
| Tr. 5 | × MARA, MARM, ISO | LEFT | `component_quantity_in_base_unit` = quantité × facteur (1 si même unité, sinon MARM, sinon ISO, sinon NULL → AMBER) |
| Tr. 6 | × MAKT (AF et composant) | LEFT | Descriptions |
| Tr. 7 | — | — | Crée la clé `bom_item_history_ID` = date (`yyyyMMdd`) - usine - AF - usage - alternative - BOM - nœud, la met en première colonne, fixe la liste des colonnes |

**Pourquoi des INNER ici alors que la règle LEAP est LEFT par défaut.** Les guidelines autorisent INNER quand une
ligne non trouvée est réellement invalide (cas en-tête / poste) : c'est le cas des trois premières jointures.

**Pourquoi les fantômes ne sont pas éclatés ici.** A décrit la BOM telle qu'elle est dans SAP. L'éclatement (remplacer
un fantôme par ses propres composants) est un choix de calcul propre à la fiabilité : il est fait dans C.

---

## 8. Quality Checks

**RED (bloquant, la table n'est pas écrite) :**
- clé `bom_item_history_ID` non nulle et unique ;
- unicité de la clé de jointure de **chaque** table préparée (STPO, STAS, STKO, MAST, MARC, MARM, MARA, MAKT).
  **Pourquoi :** une clé en double multiplie silencieusement les lignes (« fan-out »). C'est ainsi que le lab run 1
  a trouvé 4 doublons (lignes répétées dans un fichier MARC / MARM) avant toute écriture.

**AMBER (avertissement) :** composant renseigné (≥ 97 %), quantité et quantité de base renseignées, quantité en
unité de base connue (≥ 99 %).

`by_pass_quality_checks = true` transforme les RED en AMBER : uniquement pour un debug ou une reprise.

## 9. Outputs

1. **Écriture** avec `table_utils.save_table` (jamais `saveAsTable` directement — règle bloquante LEAP), partitionnée
   par `snapshot_date` : en `full`, toute la table ; en `incremental`, seulement les mois calculés (`replace_where`).
2. **Clé primaire** `gold_bom_item_history_PK` posée une seule fois : on cherche d'abord si elle existe, en comparant
   en minuscules (Unity Catalog stocke les noms en minuscules ; sans cela, le second run échouait).
3. **Vue `bom_item_history_exposed`** : toutes les colonnes sauf les colonnes techniques (`_…`). C'est elle que lisent
   les utilisateurs et PowerBI.

---

## 10. Résultats et validation (lab)

| Contrôle | Résultat |
|---|---|
| Volume | 141 702 196 lignes, 42 snapshots (2023-05-01 → 2026-10-01) |
| Clé primaire | unique, non nulle |
| Rapprochement avec STPO (un snapshot) | 3 631 652 = 4 715 776 postes − 1 079 402 clos par ECN − 2 417 sans STAS − 1 448 sans MAST − 2 inexpliqués |
| Exemple témoin E1 | les changements de BOM apparaissent au bon mois |
| Double comptage ECN | aucun |

## 11. Limites à connaître

- Avant 2024-06-11, MAST / STAS sont rétro-datés (liens supprimés sans ECN invisibles sur cette période).
- Unité de base et descriptions = valeurs actuelles.
- Mensuel : un changement suivi d'un retour en arrière dans le même mois est invisible.
- Les quantités sont celles de la BOM ; le rebut appliqué par SAP aux OF se lit dans B (réservations).

---

## 12. Questions encore ouvertes pour le métier (à poser pendant la présentation)

| # | Question | Pourquoi elle compte | Ce que fait le notebook en attendant |
|---|---|---|---|
| S8 | Que signifie le statut de BOM `12` (97 238 lignes dans la Silver officielle) ? Faut-il l'exclure comme `02` ? | Une BOM inutilisable ne doit pas servir de référence (Prévision 3) | Gardé |
| S9 | Faut-il exclure les liens MAST `cslty = X` et garder ceux sans usine, comme la Silver officielle ? | Aligner notre historique sur la BOM officielle | `cslty = X` gardé, sans usine exclus |
| H1 | Pas d'historique MAST / STAS avant 2024-06-11 : la rétro-datation est-elle acceptable pour 2023-2024 ? | Les premières périodes de fiabilité en dépendent | Rétro-daté et marqué `_is_backdated_*` |
| F1 | Mensuel suffit-il ? La demande parle d'une tendance « d'une semaine à l'autre » | Le coût et le volume doublent ou quadruplent en hebdomadaire | Mensuel |
| R1 | Le rebut de référence est-il celui de SAP (poste, sinon fiche article `kausf`) ? Le rebut d'ensemble de l'AF (`ausss`) doit-il compter ? | Définit la Prévision 3 « brute » et la Prévision 2 « sans ajustement » | Règle SAP ; `ausss` stocké mais non appliqué |
| A1 | Plusieurs alternatives actives pour un AF : laquelle sert de référence (version de production, quotas, taille de lot) ? | La Prévision 3 doit utiliser la BOM réellement prévue | C prend l'alternative de l'OF / OP à T0, sinon la plus basse |
| P1 | Un fantôme = `sobsl = 50` seulement ? (`55` = approvisionnement spécial, vérifié) | Les fantômes doivent être éclatés, jamais comptés | 50 seulement |
| V1 | Matière en vrac (`schgt`, 4,5 % des lignes de réservation) : dans ou hors du calcul de fiabilité ? | Quantités en vrac approximatives → erreurs artificielles | Gardée et marquée `is_bulk_material` |
| U1 | Usage de BOM `1` seulement (production) : d'autres usages pilotent-ils des OF ? | Périmètre de la Prévision 3 | Usage `1` |

## 13. Fiche de présentation orale (10 minutes)

1. **Le problème** (1 min) : la BOM n'est pas historisée ; on ne peut pas mesurer la fiabilité d'une BOM qui a changé depuis.
2. **La source** (1 min) : les piles de la landing zone gardent chaque extraction SAP, jamais purgées.
3. **Le principe** (2 min) : chaque 1er du mois, la dernière extraction de la veille ; 6 tables SAP assemblées.
4. **Le piège principal** (2 min) : les modifications d'ingénierie créent de nouveaux nœuds → on garde le dernier
   enregistrement STAS et on écarte les suppressions ; vérifié sans double comptage.
5. **La qualité** (1 min) : contrôles bloquants sur la clé et sur chaque jointure ; 141,7 M lignes validées.
6. **Le coût** (1 min) : reconstruction complète possible à tout moment ; mode incrémental mensuel pour la production.
7. **Les décisions à prendre** (2 min) : tableau du §12.
