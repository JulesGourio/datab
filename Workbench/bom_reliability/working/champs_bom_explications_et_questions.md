# BOM : explication des champs, questions et vérifications

| | |
|---|---|
| Date | 2026-10-13 |
| Objet | Expliquer chaque champ qui distingue notre historique (A, `bom_item_history`) de la BOM de prod (`production.BOM` Silver / Gold), dire ce dont on n'est **pas sûrs**, et préparer les questions et les vérifications |
| Complète | `questions_metier_et_architecture.md` (questions générales), `questions_ouvertes.md`, spec v5 §8 |
| Sources | code de A (`data_asset/production/bom_history/create_gold_bom_item_history.py`), copies des notebooks prod (`reference/notebooks_prod/`), `corrections_log.md` |

Légende — **Sûr** : vérifié dans le code ou par une requête · **Supposé** : connaissance SAP générale, pas vérifié chez Latécoère ·
**Inconnu** : personne ne l'a vérifié. **Qui** : **M** métier (ME, Production, Logistique) · **IT** équipe SAP / ingestion · **P** équipe LEAP.

Les requêtes de vérification utilisent `prod_…` (lecture) et la table de lab `dev_lab.lab_jules.bom_item_history` :
elles sont à lancer à la main dans Databricks, elles ne vont pas dans un notebook.

---

## 0. Rappel : comment une BOM est rangée dans SAP

Une BOM (nomenclature) dit : « pour fabriquer N pièces de l'article A dans l'usine U, il faut ces composants, en ces
quantités ». SAP la range dans **4 tables** :

```
MAST  article + usine + usage  ──►  numéro de BOM (stlnr) + alternative (stlal)
STKO  en-tête par (BOM, alternative) : quantité de base, statut, date de validité
STAS  quelle ligne (nœud, stlkn) appartient à quelle alternative
STPO  la ligne elle-même : composant, quantité, unité, rebut, flags
```

| Mot | Sens simple | Champ SAP |
|---|---|---|
| **AF** (article fabriqué) | l'article qu'on fabrique, le « parent » | `MAST.MATNR` |
| **Usine** (plant) | le site SAP | `MAST.WERKS` |
| **Usage** | à quoi sert la BOM : `1` = production. Il en existe d'autres (bureau d'études, coûts…) | `MAST.STLAN` |
| **Alternative** | un même AF peut avoir plusieurs façons d'être fabriqué (alternative 01, 02…) | `STLAL` |
| **Nœud** (node) | identifiant technique d'une ligne de BOM. Une modification (ECN) crée un **nouveau** nœud | `STLKN` |
| **Poste** (item number) | numéro de ligne visible dans SAP (0010, 0020…) | `STPO.POSNR` |
| **Composant** | l'article consommé | `STPO.IDNRK` |
| **Quantité de base** | les quantités des composants sont données pour cette quantité d'AF (souvent 1, parfois 10, 100…) | `STKO.BMENG` |
| **ECN** (numéro de modification) | numéro qui trace un changement de BOM | `AENNR` |

Où le voir dans SAP : **CS03** (afficher une BOM), **MM03** (fiche article, vues MRP 1 à 4), **CS15** (cas d'emploi
d'un composant).

---

## 1. Les rebuts (scrap)

### 1.1 Ce que c'est

Le rebut, c'est la part de pièces qu'on **prévoit de perdre** pendant la fabrication. SAP augmente la quantité
demandée pour compenser. Il y a **trois** endroits où SAP peut le stocker, et ils ne veulent pas dire la même chose :

| Colonne de A | Champ SAP | Où dans SAP | Sens | Exemple |
|---|---|---|---|---|
| `component_scrap_percentage` | `STPO.AUSCH` | CS03, ligne de BOM | rebut du **composant sur cette ligne de BOM** | 5 % : pour 100 pièces prévues, SAP réserve 105 |
| `material_component_scrap_percentage` | `MARC.KAUSF` | MM03, vue MRP 4, par usine | rebut du **composant** défini sur sa fiche article. SAP l'utilise seulement si la ligne de BOM n'a pas son propre rebut | 350 articles en ont un (`corrections_log`) |
| `assembly_scrap_percentage` | `MARC.AUSSS` | MM03, vue MRP 1, par usine | rebut **d'ensemble** de l'AF : on prévoit de rater X % des AF, donc SAP augmente **tous** les composants | 10 % : pour un OF de 100 AF, besoins calculés pour 110 |
| `is_net_scrap` | `STPO.NETAU` | CS03, ligne de BOM | si coché, le rebut d'ensemble de l'AF **n'est pas** appliqué à ce composant | |

**Sûr :** les 4 champs sont lus dans A (Prep2 pour STPO, Prep6 pour MARC), **historisés** (valeur à la date du
snapshot), sans calcul. A ne calcule aucune quantité « avec rebut » : c'est la Proj qui le fait.
**Sûr :** la prod garde `STPO_ausch` et `STPO_netau` dans la Silver (en texte) mais **pas** dans la vue `BOM_exposed`.
La prod n'a ni `kausf` ni `ausss`.

### 1.2 Ce qu'on a constaté (`corrections_log.md`, RB1 à RB4)

- `ausch` (ligne de BOM) est rempli sur seulement **1 186 nœuds actifs**.
- `kausf` (fiche article) : **350 articles**. C'est l'origine de **83 %** du rebut des réservations d'OF.
- `ausss` (rebut d'ensemble) : **32 196 articles**, mais presque tous **achetés** (`beskz = F`). Seuls **219 AF** ont
  un rebut d'ensemble. Pour un article acheté, ce rebut gonfle la proposition d'achat, pas les besoins des OF.
- 17 % du rebut des réservations ne correspond à aucun champ : modification manuelle sur l'OF, ou `kausf` qui a changé
  depuis (comparaison faite avec `marc_latest`, pas avec l'historique).

### 1.3 Ce dont on n'est pas sûrs

- **Supposé :** la règle SAP « `ausch` de la ligne, sinon `kausf` de la fiche ». À confirmer par l'IT ou sur un OF réel.
- **Inconnu :** le métier parle d'un « +20 % » (spec). Est-ce le `ausss` des achetés ? Une marge dans le dashboard
  « Average BOM » ? Autre chose ?
- **Inconnu :** la Prévision 3 (BOM × quantité de l'OF) doit-elle inclure le rebut ou non ? (question R4, D8).

### 1.4 Questions

| # | Question | Qui |
|---|---|---|
| RB-1 | Quand SAP calcule le besoin d'un OF, applique-t-il le rebut de la ligne de BOM (`AUSCH`), sinon celui de la fiche article (`KAUSF`) ? Les deux ne s'additionnent jamais ? | IT |
| RB-2 | Le rebut d'ensemble (`AUSSS`) de l'AF : est-il appliqué aux besoins des composants de l'OF, ou à la quantité de l'OF lui-même (quantité à produire augmentée) ? Exemple réel avec un des 219 AF. | IT + M |
| RB-3 | Pourquoi 32 000 articles **achetés** ont-ils un rebut d'ensemble ? Est-ce volontaire (marge d'achat) ? Est-ce le « +20 % » dont parle le métier ? | M (Logistique) |
| RB-4 | Qui saisit le rebut sur une ligne de BOM (`AUSCH`) et pourquoi c'est rare (1 186 nœuds) ? Le rebut est-il plutôt géré sur la fiche article ? | M (ME) |
| RB-5 | Le planificateur peut-il modifier le rebut directement sur un OF (17 % des cas sans origine) ? C'est un « ajustement » à retirer de la Prévision 2 ? | M (Production) |
| RB-6 | Pour mesurer la fiabilité de la BOM : la Prévision 3 doit-elle être **avec** ou **sans** rebut ? | M |

### 1.5 Vérifications à faire

1. **Dans SAP**, pour 2–3 composants avec rebut : CS03 sur l'AF (voir `AUSCH`), MM03 du composant (vue MRP 4, `KAUSF`),
   puis un OF de cet AF (CO03, onglet composants) : la quantité réservée = quantité BOM × (1 + rebut) ?
2. **Dans Databricks** : comparer A avec la fiche article actuelle.

```sql
-- Composants avec rebut au dernier snapshot de A, comparés à la fiche article actuelle
SELECT h.plant, h.material_number, h.component_material_number,
       h.component_scrap_percentage, h.material_component_scrap_percentage,
       m.kausf AS kausf_actuel, h.is_net_scrap
FROM dev_lab.lab_jules.bom_item_history h
LEFT JOIN prod_bronze.sap_latecoere_ecc6.marc_latest m
  ON ltrim('0', m.matnr) = h.component_material_number AND m.werks = h.plant
WHERE h.snapshot_date = (SELECT max(snapshot_date) FROM dev_lab.lab_jules.bom_item_history)
  AND (h.component_scrap_percentage > 0 OR h.material_component_scrap_percentage > 0)
LIMIT 50;
```

---

## 2. Le flag fantôme (`is_phantom_item`)

### 2.1 Ce que c'est

Un **fantôme** (phantom assembly) est un sous-ensemble qui **n'existe pas physiquement en stock** : c'est un regroupement
de composants dans la BOM (ex. « kit de fixation »). Quand SAP crée un OF, il **remplace** le fantôme par ses propres
composants (il « éclate » le fantôme). Le fantôme lui-même n'est jamais consommé (pas de mouvement 261 sur lui).

Pourquoi c'est important pour la fiabilité : si on compare la BOM (qui contient le fantôme) à la consommation (qui
contient ses composants), le fantôme apparaît comme « prévu, jamais consommé » (erreur 100 %), et ses composants comme
« consommés, non prévus ». Il faut donc éclater les fantômes dans la Prévision 3.

| | Champ SAP | Où |
|---|---|---|
| Fantôme défini sur la fiche article du composant | `MARC.SOBSL = '50'` (clé d'approvisionnement spécial) | MM03, vue MRP 2 |
| Fantôme sur la ligne de BOM | `STPO.DUMPS` | CS03 — **absent de `stpo_stack`** |
| Fantôme sur la réservation de l'OF | `RESB.DUMPS = 'X'` | utilisé par B |

**Sûr :** A calcule `is_phantom_item = MARC.SOBSL du composant = '50'` dans l'usine de la BOM, à la date du
snapshot. Les fantômes sont **gardés** dans A ; l'éclatement est fait par la Proj (C).
**Sûr :** la prod n'a aucun indicateur de fantôme.
**Sûr (F4f, `corrections_log`) :** dans RESB, SAP a déjà éclaté les fantômes (OF 6799399).

### 2.2 Ce dont on n'est pas sûrs

- **Supposé :** `SOBSL = 50` veut dire « fantôme » chez Latécoère. Les clés d'approvisionnement spécial sont du
  paramétrage (table `T460A`), elles peuvent être différentes. La table `t460a` existe en Bronze : à vérifier.
- **Inconnu :** une ligne de BOM peut être marquée fantôme (`STPO.DUMPS`) même si l'article ne l'est pas sur sa fiche.
  Ce cas nous échappe, puisque `dumps` n'est pas extrait dans `stpo_stack`.
- **Inconnu :** pour éclater un fantôme qui a plusieurs alternatives, laquelle prendre ? (on prend la plus petite, M14).

### 2.3 Questions

| # | Question | Qui |
|---|---|---|
| PH-1 | La clé `50` est-elle bien « fantôme » dans le paramétrage Latécoère ? Y a-t-il d'autres clés fantômes ? | IT |
| PH-2 | Les ME marquent-ils parfois une **ligne** de BOM comme fantôme (`STPO-DUMPS`) sans changer la fiche article ? Peut-on ajouter `DUMPS` à l'extraction STPO ? | IT + M (ME) |
| PH-3 | Un fantôme à plusieurs alternatives : quelle alternative SAP utilise-t-il à l'éclatement ? | IT |
| PH-4 | Le métier veut-il voir le fantôme dans le rapport (page 3, poste « 9999 ») ou seulement ses composants ? | M |

### 2.4 Vérifications à faire

```sql
-- Sens de la clé 50 dans le paramétrage
SELECT * FROM prod_bronze.sap_latecoere_ecc6.t460a WHERE sobsl = '50' LIMIT 20;

-- Nombre de lignes fantômes au dernier snapshot, par usine
SELECT plant, count(*) AS lignes, count_if(is_phantom_item) AS lignes_fantomes
FROM dev_lab.lab_jules.bom_item_history
WHERE snapshot_date = (SELECT max(snapshot_date) FROM dev_lab.lab_jules.bom_item_history)
GROUP BY plant ORDER BY plant;
```

Dans SAP : un composant avec `is_phantom_item = true` → MM03 (vue MRP 2, approvisionnement spécial) et CS03 de l'AF
(la ligne est-elle marquée fantôme ?).

---

## 3. MAST : liens sans usine et `cslty`

### 3.1 MAST sans usine

**Ce que c'est :** une ligne MAST relie un article à une BOM. Normalement elle a une usine (`WERKS`). Une ligne **sans
usine** est une BOM qui n'est rattachée à aucun site : par exemple une BOM « de groupe » ou de bureau d'études.

| | Prod (Silver) | Notre A |
|---|---|---|
| Lignes MAST sans usine | **gardées** → 691 lignes avec `plant` NULL dans la Silver (80 liens) | **exclues** |

**Sûr :** les 691 lignes à clé NULL de la Silver sont toutes `plant` NULL (spec §8.3).
**Supposé :** un OF est toujours créé dans une usine, donc il ne peut utiliser qu'une BOM rattachée à cette usine. Les
BOM sans usine ne servent pas à la fiabilité. À confirmer.

| # | Question | Qui |
|---|---|---|
| MU-1 | Que sont ces 80 liens MAST sans usine ? BOM de groupe, BOM d'études, erreurs ? Qui les crée ? | IT + M (ME) |
| MU-2 | SAP peut-il utiliser une BOM sans usine pour un OF (par exemple si l'usine n'a pas de BOM propre) ? | IT |
| MU-3 | Les consommateurs de la Silver (RFQ, Average BOM) ont-ils besoin de ces lignes ? Ou est-ce un oubli de filtre ? | P |

```sql
-- Les 80 liens sans usine : quels articles, quels usages, quelles dates
SELECT matnr, stlan, stlnr, stlal, andat, cslty
FROM prod_bronze.sap_latecoere_ecc6.mast_latest
WHERE werks IS NULL OR trim(werks) = ''
ORDER BY andat DESC;
```

### 3.2 `cslty`

**Ce que c'est :** `MAST.CSLTY` est un indicateur SAP lié aux **articles configurables** (variantes). Le code prod dit
seulement « exclude article with wrong configuration ». **Inconnu :** ce que veut dire exactement `X` chez
Latécoère et combien de lignes sont concernées.

| | Prod (Silver) | Notre A |
|---|---|---|
| `cslty = 'X'` | **exclu** | **gardé** (non aligné, D2) |

| # | Question | Qui |
|---|---|---|
| CS-1 | Que signifie `CSLTY = X` dans SAP chez Latécoère ? Pourquoi la Silver l'exclut-elle (« wrong configuration ») ? Qui a écrit cette règle ? | IT + P (auteur de la Silver) |
| CS-2 | Ces articles ont-ils des OF ? Si oui, doivent-ils compter dans la fiabilité ? | M |

```sql
-- Volume concerné
SELECT cslty, werks IS NULL OR trim(werks) = '' AS sans_usine, count(*) AS liens
FROM prod_bronze.sap_latecoere_ecc6.mast_latest
WHERE stlan = '1'
GROUP BY ALL;
```

---

## 4. Statut de la BOM (`BOM_status`, `STKO.STLST`)

**Ce que c'est :** le statut dit si une BOM est utilisable (active pour la production, pour les coûts, bloquée…). Les
codes sont du **paramétrage** Latécoère (table SAP `T415S`) : `01`, `02`, `12`… n'ont pas de sens universel.

**Sûr :** la Silver exclut `02` et vide ; A fait pareil depuis le 2026-10-13. Statuts restants dans la Silver :
`01` = 3 132 352 · `12` = 97 238 · `07` = 532 · `14` = 108 · `21` = 19 · `03` = 38.
**Supposé :** `01` = actif, `02` = inactif. **Inconnu :** `03`, `07`, `12`, `14`, `21`.

| # | Question | Qui |
|---|---|---|
| ST-1 | Peut-on avoir la liste des statuts BOM avec leur texte (table `T415S` / `T415T`) ? Peut-on l'ajouter à l'extraction ? | IT |
| ST-2 | Le statut `12` (3 % des lignes) : BOM utilisable par les OF ou non ? Faut-il l'exclure ? | M (ME) |
| ST-3 | Les statuts `03`, `07`, `14`, `21` : idem. | M (ME) |
| ST-4 | Pourquoi la Silver exclut-elle seulement `02` ? Règle métier écrite quelque part ? | P |

```sql
-- Exemples de BOM par statut, pour regarder dans CS03
SELECT BOM_status, plant, material_number, BOM_alternative, count(*) AS lignes
FROM dev_lab.lab_jules.bom_item_history
WHERE snapshot_date = (SELECT max(snapshot_date) FROM dev_lab.lab_jules.bom_item_history)
  AND BOM_status <> '01'
GROUP BY ALL ORDER BY BOM_status, lignes DESC;
```

---

## 5. Colonnes de la Gold prod absentes de A

Ces colonnes viennent du référentiel `material_master_data_plant` (Gold prod), sauf indication. Elles ne sont **pas**
dans A. Pour chacune : ce que c'est, et est-ce utile pour la fiabilité ?

### 5.1 `Storage_Location` (magasin)

**Ce que c'est :** le magasin d'où le composant sort pour l'OF. Source prod : `STPO.LGORT` (magasin de production de la
ligne de BOM) ; s'il est vide, le magasin de production de la fiche article (`MDMA.LGPRO`, par zone MRP).
**Utile pour la fiabilité ?** Pas pour le calcul. Peut-être pour analyser les écarts (un magasin qui sort mal).

| # | Question | Qui |
|---|---|---|
| SL-1 | Le magasin aide-t-il à expliquer les écarts (ex. sorties en libre-service depuis un magasin de bord de ligne) ? | M (Logistique) |

### 5.2 `Component_Plant_Status` et `Material_Plant_Status`

**Ce que c'est :** le statut de l'article **dans l'usine** (fiche article). Il dit par exemple qu'un article est bloqué
pour les achats, la production, ou en fin de vie. `Component_Plant_Status` = statut du composant,
`Material_Plant_Status` = statut de l'AF.
**Supposé :** champ SAP `MARC.MMSTA` (statut article spécifique usine, MM03 vue MRP 1). **Pas tracé** dans le code du
référentiel : à confirmer.
**Utile ?** Oui, potentiellement : un composant bloqué ou en fin de vie est souvent remplacé sur l'OF → écart BOM /
consommation qui n'est pas une « mauvaise BOM ». Attention : la prod donne le statut **d'aujourd'hui**, pas celui de la
date du snapshot.

| # | Question | Qui |
|---|---|---|
| PS-1 | Quels sont les statuts article usine utilisés (codes et sens) ? | IT |
| PS-2 | Faut-il exclure de la fiabilité les composants bloqués / en fin de vie, ou au contraire les montrer (ils expliquent des écarts) ? | M |
| PS-3 | Si oui, faut-il le statut à T0 (historisé, depuis `marc_stack`) ou le statut actuel ? | M |

### 5.3 `backflush` (post-consommation, `MARC.RGEKZ`)

**Ce que c'est :** si un composant est en **backflush**, personne ne saisit sa sortie de stock à la main : SAP fait
automatiquement le mouvement 261 quand l'opération de l'OF est confirmée, avec la **quantité prévue** (sauf correction).
**C'est le point le plus important de cette section pour la fiabilité :** pour un composant en backflush, la
consommation est souvent **égale à la prévision par construction**. La fiabilité de ces composants paraîtra excellente
sans rien dire sur la BOM. **Inconnu :** combien de composants sont en backflush.

| # | Question | Qui |
|---|---|---|
| BF-1 | Quels composants sont en backflush (familles, usines) ? | IT + M (Production) |
| BF-2 | Quand un composant en backflush est réellement consommé en plus ou en moins, l'opérateur corrige-t-il le mouvement ? | M (Production) |
| BF-3 | Faut-il montrer à part (ou exclure) les composants en backflush dans le rapport ? | M |

```sql
-- Part des lignes de BOM dont le composant est en backflush (référentiel prod, état actuel)
SELECT b.plant, count(*) AS lignes, count_if(m.rgekz = '1') AS lignes_backflush
FROM dev_lab.lab_jules.bom_item_history b
LEFT JOIN prod_bronze.sap_latecoere_ecc6.marc_latest m
  ON ltrim('0', m.matnr) = b.component_material_number AND m.werks = b.plant
WHERE b.snapshot_date = (SELECT max(snapshot_date) FROM dev_lab.lab_jules.bom_item_history)
GROUP BY b.plant ORDER BY b.plant;
-- Valeur de rgekz à vérifier d'abord : SELECT rgekz, count(*) FROM prod_bronze.sap_latecoere_ecc6.marc_latest GROUP BY 1
```

### 5.4 Postes alternatifs : `alternative_station_group`, `probability` (et `alternative_item`)

**Ce que c'est :** plusieurs lignes de BOM peuvent être **interchangeables** (même fonction, fournisseurs ou références
différents). SAP les regroupe dans un **groupe de postes alternatifs** :

| Colonne prod | Champ SAP | Sens |
|---|---|---|
| `alternative_item` | `STPO.ALPOS` | `X` = cette ligne fait partie d'un groupe alternatif |
| `alternative_station_group` | `STPO.ALPGR` | identifiant du groupe |
| `probability` | `STPO.EWAHR` | probabilité d'utilisation (%) de cette ligne dans le groupe |

Exemple : vis A (60 %) et vis B (40 %), 10 par AF. La BOM brute dit 10 A + 10 B = 20 vis. En réalité on en consomme 10.
**Sûr :** la Gold prod pondère : `quantity = menge × probability / 100` si `alternative_item = X` (190 634 postes
alternatifs dans la Silver).
**Sûr :** **A ne lit pas ces champs.** Sa quantité brute compte chaque ligne du groupe en entier. **C'est un manque de
A, découvert le 2026-10-13** : la Prévision 3 est surestimée pour ces composants.

| # | Question | Qui |
|---|---|---|
| AL-1 | Les probabilités (`EWAHR`) sont-elles tenues à jour par les ME, ou souvent 100 / 0 ? | M (ME) |
| AL-2 | Dans un OF, SAP réserve-t-il chaque ligne au prorata de sa probabilité, ou une seule ligne (stratégie de sélection) ? | IT |
| AL-3 | Pour la fiabilité : comparer composant par composant (A vs B séparés), ou le **groupe** entier (conso A + B contre prévision du groupe) ? | M |

**Action proposée** (pas encore faite) : ajouter `ALPOS`, `ALPGR`, `EWAHR` à A (lecture STPO), et laisser la Proj
décider de la pondération.

### 5.5 `creation_date` et `last_modif`

| Colonne prod | Champ SAP | Sens |
|---|---|---|
| `creation_date` | `STKO.ANDAT` | date de création de l'en-tête de BOM (alternative) |
| `last_modif` | `STPO.AEDAT` | date de dernière modification de la ligne |

**Utile ?** Oui pour la question « ignorer les lignes modifiées récemment » (M15 de `questions_metier_et_architecture.md`) :
`last_modif` dit qu'une ligne a bougé. Dans A, on a l'équivalent par une autre voie : chaque snapshot montre l'état, et
`item_valid_from_date` / `item_change_number` / `previous_BOM_node` montrent les ECN.
**Attention :** la Silver choisit la version d'en-tête par `creation_date` la plus récente ; A la choisit par date de
validité (`DATUV`) puis compteur (`STKOZ`). Les deux peuvent donner une version différente (spec §8, ligne 2).

| # | Question | Qui |
|---|---|---|
| DT-1 | Une ligne de BOM peut-elle être modifiée **sans** ECN (date `AEDAT` qui change, pas de nouveau nœud) ? Dans ce cas, l'historique de A voit la nouvelle valeur au snapshot suivant, mais ne sait pas pourquoi. Acceptable ? | IT + M (ME) |
| DT-2 | Quelle règle de choix de l'en-tête est la bonne : la plus récente créée, ou la plus récente valide ? | IT |

---

## 6. Autres champs de A à faire valider

| Colonne de A | Champ SAP | Sens | Question |
|---|---|---|---|
| `is_bulk_material` | `STPO.SCHGT` | **matière en vrac** (graisse, colle, petite visserie) : prise librement en bord de ligne, souvent **sans sortie sur l'OF** | BK-1 (M) : les articles en vrac ont-ils des sorties 261 sur l'OF ? Sinon, faut-il les exclure de la fiabilité (ils seraient tous « surstock ») ? 97 762 lignes concernées (`corrections_log`) |
| `is_fixed_quantity` | `STPO.FMENG` | la quantité ne dépend pas de la taille de l'OF (ex. 1 gabarit par OF) | FQ-1 (M) : confirmer que la Prévision 3 ne multiplie pas ces lignes par la quantité de l'OF |
| `BOM_base_quantity`, `BOM_base_unit` | `STKO.BMENG`, `BMEIN` | les quantités sont données pour cette quantité d'AF (3,5 % des BOM ≠ 1) | — (règle SAP connue, la Proj divise) |
| `component_quantity_in_base_unit` | calcul (`MARM` + table de facteurs ISO) | quantité convertie dans l'unité de stock du composant, pour la comparer aux mouvements | UN-1 (IT) : la table `T006` (conversions d'unités) peut-elle être extraite ? Elle remplacerait notre table ISO écrite à la main |
| `lot_size_from_quantity`, `lot_size_to_quantity` | `MAST.LOSVN`, `LOSBS` | plage de taille de lot pour laquelle l'alternative est valable | LS-1 (IT) : SAP choisit-il l'alternative selon la taille de l'OF ? Utilisé chez Latécoère ? |
| `BOM_item_category` | `STPO.POSTP` | type de ligne : `L` article en stock, `N` non stocké, `T` texte, `D` document… | IC-1 (M) : quels types garder ? Les lignes `T` (texte) et `D` (document) n'ont pas de consommation |
| `_is_backdated_link`, `_is_backdated_allocation` | — | snapshots avant 2024-06-11 reconstruits avec la première extraction MAST/STAS | BD-1 (M) : acceptable de commencer l'analyse avant juin 2024 avec cette approximation ? |

---

## 7. Règles de choix de version (STKO et STAS)

### 7.1 Ce que c'est

Quand une BOM est modifiée, SAP ne remplace pas la ligne : il ajoute un **nouvel enregistrement** avec une date de
validité (`DATUV`) et un compteur. Il faut donc choisir **le bon enregistrement** pour une date.

| Table | Prod (Silver) | Notre A |
|---|---|---|
| STKO (en-tête) | le plus récent par `creation_date` | le plus récent valide à la date du snapshot (`DATUV`, puis compteur `STKOZ`), puis exclusion s'il est supprimé |
| STAS (lien ligne → alternative) | toutes les lignes, **sauf** si le nœud a au moins une ligne supprimée (`LKENZ = X`) | le plus récent par (alternative, nœud) valide à la date, puis exclusion s'il est supprimé |
| STPO (ligne) | toutes les lignes du nœud | lignes valides à la date (`DATUV` < snapshot), non supprimées |

### 7.2 Ce dont on n'est pas sûrs

- **Inconnu :** si les deux règles donnent le même résultat aujourd'hui. Le dernier snapshot de A (3 688 354 lignes,
  avant l'alignement du statut) et la Silver (3 230 287) n'ont jamais été comparés ligne à ligne à la même date.
- **Supposé :** la Silver peut garder **deux versions** d'un même nœud dans STPO (pas de filtre de date). Elle n'a pas de
  doublon de `business_id`, donc soit ce cas n'arrive pas, soit il est caché : à vérifier.

### 7.3 Questions et vérifications

| # | Question | Qui |
|---|---|---|
| VR-1 | Un même nœud (`STLKN`) peut-il avoir plusieurs lignes STPO (compteur `STPOZ`) ? Dans quel cas ? | IT |
| VR-2 | Après un ECN, l'ancien nœud est-il toujours terminé par une ligne STAS `LKENZ = X` ? (constaté, à confirmer comme règle) | IT |
| VR-3 | La règle de la Silver sur STAS (nœud exclu dès qu'une ligne est supprimée) : voulue, ou simplification ? | P (auteur de la Silver) |

**Réconciliation à faire** (garde-fou 1 de la spec §9.1) : lancer A pour le snapshot le plus proche de la date de la
Silver, puis comparer.

```sql
-- Lignes présentes d'un côté et pas de l'autre (même clé)
WITH a AS (
  SELECT plant, material_number, BOM_alternative AS alternative, BOM_number, BOM_node AS node
  FROM dev_lab.lab_jules.bom_item_history
  WHERE snapshot_date = (SELECT max(snapshot_date) FROM dev_lab.lab_jules.bom_item_history)
), s AS (
  SELECT plant, material_number, alternative, BOM_number, node
  FROM prod_silver.production.bom
  WHERE plant IS NOT NULL
)
SELECT 'seulement_A' AS cote, count(*) FROM (SELECT * FROM a EXCEPT SELECT * FROM s)
UNION ALL
SELECT 'seulement_Silver', count(*) FROM (SELECT * FROM s EXCEPT SELECT * FROM a);
```

Puis prendre 5 exemples de chaque côté et les regarder dans CS03 à la date du snapshot.

---

## 8. Vérifications sur cas réels (à faire avec un ME, 1 heure)

Le plus sûr pour valider tout ce qui précède : prendre **3 AF** et regarder ensemble SAP et nos tables.

| Cas | Choisir | Dans SAP | Dans nos tables | Vérifie |
|---|---|---|---|---|
| 1. AF simple | un AF à 1 alternative, quantité de base 1, sans fantôme | CS03 à une date passée (champ « valide le ») | A au snapshot correspondant | composants, quantités, unités |
| 2. AF avec ECN | un AF avec `previous_BOM_node` non vide | CS03 avant et après la date de l'ECN ; CC03 (le numéro de modification) | A sur deux snapshots | règle de version (§7), `item_change_number` |
| 3. AF avec fantôme et rebut | un AF avec `is_phantom_item` et un rebut | CS03, MM03 du fantôme et des composants ; un OF (CO03, composants) | A, B (réservations de l'OF), C | §1, §2, éclatement, quantités avec / sans rebut |

Ajouter si possible : un AF avec un groupe de postes alternatifs (§5.4) et un composant en backflush (§5.3).

Requête pour trouver des candidats :

```sql
SELECT plant, material_number,
       count(DISTINCT BOM_alternative) AS nb_alternatives,
       max(BOM_base_quantity) AS base,
       count_if(is_phantom_item) AS fantomes,
       count_if(component_scrap_percentage > 0 OR material_component_scrap_percentage > 0) AS lignes_rebut,
       count_if(previous_BOM_node IS NOT NULL AND previous_BOM_node <> '') AS lignes_ecn
FROM dev_lab.lab_jules.bom_item_history
WHERE snapshot_date = (SELECT max(snapshot_date) FROM dev_lab.lab_jules.bom_item_history)
GROUP BY plant, material_number
ORDER BY lignes_ecn DESC, fantomes DESC
LIMIT 50;
```

Noter les résultats dans `corrections_log.md`.

---

## 9. Résumé : ce qui est sûr, ce qui ne l'est pas

| Sujet | Sûr | Pas sûr / à faire |
|---|---|---|
| Rebuts | les 4 champs sont lus et historisés dans A | règle SAP exacte ; « +20 % » ; rebut dans Prévision 3 |
| Fantôme | `SOBSL = 50` lu ; SAP éclate dans RESB | sens de `50` chez Latécoère ; `STPO.DUMPS` non extrait |
| MAST sans usine | 691 lignes Silver, exclues de A | ce qu'elles sont ; décision D3 |
| `cslty` | exclu par la Silver, gardé par A | sens ; volume ; décision D2 |
| Statut | aligné (`02`, vide exclus) | sens de `12` et des autres codes |
| Statuts article usine, magasin | dans la Gold prod, pas dans A | champ source exact ; utilité |
| Backflush | dans la Gold prod, pas dans A | **risque de biais fort** : à mesurer |
| Postes alternatifs | pondérés par la Gold prod | **manque dans A** : à ajouter |
| Matière en vrac | lue dans A | à exclure ou non |
| Choix des versions | règles différentes entre Silver et A | jamais réconcilié à la même date |
