# BOM reliability — Questions à poser (métier, OP/OF, architecture)

| | |
|---|---|
| Date | 2026-10-13 |
| Source | Change request « BOM reliability », version 2 du document de spécification (`BOM_spec_v2.docx`) |
| Complète | `questions_ouvertes.md` (questions de 2026-10-09 et 2026-10-12) |
| Usage | Pour les réunions avec le métier (ME, Logistique, Production), avec l'équipe plateforme LEAP et pour ta propre compréhension |

Légende — **Priorité** : 🔴 bloque le calcul ou le périmètre · 🟠 change les résultats · 🟢 confort.
**Qui** : **M** métier (ME process & tools, ME Operations, Logistique, Production) · **P** équipe plateforme / data LEAP · **T** toi (pour comprendre le sujet).

---

## 0. Ce que dit la spec v2 (résumé pour se repérer)

- Mesurer la **fiabilité de la BOM** par AF (article fabriqué), dans la branche **interconnexion**.
- Aujourd'hui, une **BOM commune** est utilisée pour tous les MSN d'un même CA, calculée à partir de la **moyenne de la consommation passée**, ajustée des configurations futures connues. Elle doit être mesurée.
- Comparaison : demande prévisionnelle connue à T0 (Prévision 1, 2, 3) contre consommation des OF commencés après T1 et terminés avant T2 (Conso 1, 2, 3).
- Erreur par composant : `|prévision − conso| / max`, statut surstock / manquant / ok, fiabilité = 100 % − moyenne des erreurs.
- Période : T0 choisi, T1 = T0 + 6 mois et T2 = T1 + 3 mois par défaut ; option T1 = T0 + 3 mois.
- Pages : résumé par centre de profit et site ; analyse par centre de profit et site ; page optionnelle des composants par poste de nomenclature ; commentaires par poste.
- Systèmes cibles : **INT et PROD** cochés (DEV non coché).
- Valeur attendue : optimiser le stock immobilisé et éviter les ruptures d'approvisionnement.
- Application concernée : **SAP** uniquement.
- **Validation métier : aucune date renseignée** (BODIN Anthony, CONTINENTE Matthieu, LAPORTE Pierre).

Différences avec ce que nous avons implémenté (détail en §5) :

| Point | Spec v2 | Notre état |
|---|---|---|
| Fréquence de T0 | « par exemple 1er janvier, 1er février… » (choix libre) | 1er de chaque mois |
| Tendance | « comparer la tendance d'une semaine à l'autre » | mensuel |
| Environnement cible | INT et PROD | dev (lab), prod non encore |
| Conso 3 | « TBD » | non calculée |
| Page 3 et commentaires | optionnels | non faits |

---

## 1. Questions au métier — la BOM et son usage

### 1.1 Définitions et périmètre

| # | Question | Qui | Priorité | Pourquoi |
|---|---|---|---|---|
| M1 | Que désigne exactement un **CA** (dans « BOM commune pour tous les MSN d'un même CA ») ? Est-ce une famille de produits, un programme avion ? | M | 🔴 | Le périmètre de la BOM commune et l'agrégation de la fiabilité en dépendent |
| M2 | Que désigne un **MSN** (numéro de série avion) dans ce contexte ? La BOM est-elle différente selon le MSN, et si oui, quelle est la différence concrète (configuration client) ? | M | 🔴 | Lien entre une BOM SAP (par AF et usine) et une BOM « commune » par CA |
| M3 | Une **BOM commune** est-elle une BOM SAP existante, ou une BOM construite hors SAP (moyenne de consommation) ? Où est-elle stockée ? | M | 🔴 | Question centrale : **quelle BOM est « la BOM » qu'on mesure ?** |
| M4 | Quelle est la **BOM de référence** actuelle (Prévision 3, « BOM standard ») : la BOM SAP (STPO) ou la BOM moyenne (table `current_average_bom`, dashboard « Average BOM ») ? | M | 🔴 | Si c'est la moyenne, Prévision 3 ne mesure pas la BOM SAP |
| M5 | Comment la **moyenne de consommation passée** est-elle calculée aujourd'hui : quelle période, quels mouvements, quelle quantité par AF, quels ajustements ? | M | 🔴 | Pour reproduire la méthode et savoir ce qui est mesuré |
| M6 | Quelles sont les **configurations futures connues** qui ajustent la BOM ? Comment sont-elles saisies (fichier, SAP, Excel) ? | M | 🟠 | Ces ajustements font partie de la Prévision 2 « sans ajustements » ou non |
| M7 | Le document **IN_P_0030** (procédure de mise à jour de la BOM par ME) : peut-on le lire ? Décrit-il comment une BOM est modifiée, validée, et à quelle fréquence ? | M | 🔴 | Documentation de référence du processus, nécessaire pour interpréter les ECN |
| M8 | Qui **modifie** une BOM (ME process, bureau d'études, autre) ? À quelle fréquence ? | M | 🟠 | Explique les changements d'ECN et les versions |
| M9 | Quelle est la différence entre la **branche interconnexion** et les autres branches (AS, IS) ? Quelles usines, quels centres de profit ? | M | 🟠 | Périmètre du calcul (filtre par division, branche) |
| M10 | Qu'est-ce que la **« gel des données clients »** ? À quelle date a-t-il lieu, et que se passe-t-il après (la BOM n'est plus modifiée) ? | M | 🟠 | Définit la date T0 pertinente pour la BOM commune |

### 1.2 Indicateurs existants

| # | Question | Qui | Priorité | Pourquoi |
|---|---|---|---|---|
| M11 | Comment sont calculés les indicateurs existants : **Global Average BOM** (dashboard Power BI « Average BOM », horizon), **BOM Analysis** (Sigma) ? Quelles sources et quelles règles ? | M + P | 🔴 | Éviter de réinventer un indicateur déjà présent, et comprendre les écarts |
| M12 | Peut-on accéder aux dashboards Power BI et Sigma (lecture) pour voir les définitions, les filtres et les valeurs de référence ? | M | 🟠 | Vérifier que notre calcul donne des résultats cohérents |
| M13 | La **table `dev_proj.supply_chain.current_average_bom`** est-elle la table qui alimente le dashboard « Average BOM » ? Est-elle la « BOM commune » ? | M + P | 🔴 | Ce serait la BOM à mesurer |
| M14 | Quelle est la **valeur attendue** de la fiabilité : un objectif (ex. 90 %) ? Un seuil d'alerte ? | M | 🟠 | Pour interpréter 88,6 % (résultat lab) |

### 1.3 Problème actuel

| # | Question | Qui | Priorité | Pourquoi |
|---|---|---|---|---|
| M15 | « Ignorer les lignes de nomenclature qui ont évolué pendant les derniers mois » : quelle règle exacte appliquent les équipes aujourd'hui ? Un poste modifié est-il exclu, ou seulement sa nouvelle version ? | M | 🔴 | Règle à reproduire : nous traitons les ECN par nœud (ancien et nouveau) |
| M16 | Quelle est la **durée** qui compte comme « récente » (3 mois ? 6 mois ?) ? | M | 🟠 | Influence la période d'analyse |

---

## 2. Questions au métier — OP et OF

### 2.1 Définitions

| # | Question | Qui | Priorité | Pourquoi |
|---|---|---|---|---|
| O1 | Confirmer les définitions : **OP** = ordre planifié (MRP, table PLAF) ; **OF** = ordre de fabrication (AFKO / AFPO). Un OP devient-il toujours un OF (conversion) ? Peut-il être supprimé sans OF ? | M + P | 🔴 | Base de la Prévision 1 et de l'attribution des OF |
| O2 | Quand un OP est **converti** en OF, la quantité change-t-elle ? Qui décide (planificateur, MRP) ? | M | 🟠 | Normalisation de la prévision par la quantité finale |
| O3 | **Période « ignorée » et « période d'analyse »** dans la spec : qu'entend-on par « OF/OP ignorés » ? Ce sont les OF lancés avant T1 (ignorés car trop anciens, ou pas encore finis ?), ou ceux lancés après T2 ? | M | 🔴 | Le schéma de la spec montre deux cas : à confirmer |
| O4 | Un OF « **commencé après T1** » : date de début **réelle** (GSTRI) ou **planifiée** (GSTRP) ? | M | 🔴 | Influence qui est dans le périmètre |
| O5 | Un OF « **terminé avant T2** » : date de fin **réelle** (GETRI), date **TECO** (technically complete), ou **clôture** ? Pour un OF partiellement livré, que fait-on ? | M | 🔴 | Idem |
| O6 | Quels **types d'OF** sont concernés ? Details Parts (ZP01, ZP05, YP03), Assembly Parts (YP04, ZP03, ZP04), Rush Orders (ZP09, ZP10…). Tous, ou seulement certains ? | M | 🟠 | Périmètre de la mesure |
| O7 | Les **OF annulés** : toujours exclus ? Un OF annulé après consommation partielle ? | M | 🟠 | Biais possible sur la consommation |
| O8 | Un OF peut-il être **scindé** ou **fusionné** ? Comment est-il imputé dans ce cas (mouvements sur le nouvel OF) ? | M | 🟠 | Lien OP → OF et consommation |
| O9 | Un OF peut-il être lancé **sans OP** (créé directement) ? Dans quelle proportion ? (Lab, période par défaut : 4 191 OF sans prévision à T0, soit 44 %, à confirmer) | M | 🟠 | Ces OF sont hors fiabilité par défaut |
| O10 | Qu'est-ce qu'un **OF de rework** (mouvements 261 / 262 de l'AF lui-même) ? Doit-on les compter dans la consommation ? | M | 🟢 | Question M10 de la spec v1 |

### 2.2 Besoins, réservations et quantités

| # | Question | Qui | Priorité | Pourquoi |
|---|---|---|---|---|
| O11 | Quelle différence entre une réservation **SB** (besoin d'un OP) et **AR** (besoin d'un OF) ? Un OP a-t-il des réservations **par composant**, identiques à celles de l'OF qui en est issu ? | M + P | 🔴 | Construction de la Prévision 1 par OP et par OF |
| O12 | La **quantité de besoin** (BDMNG, « avec rebut ») : appliquée par SAP selon quelle règle exacte (rebut du poste, rebut de la fiche article, rebut d'ensemble) ? | M + P | 🔴 | Cœur de la Prévision 1 et 2 (cf. questions R1–R4 du 2026-10-09) |
| O13 | Les **modifications manuelles** d'un OF (composant ajouté, quantité changée) : faut-il les considérer comme un « ajustement » exclu de la Prévision 2 ? | M | 🟠 | Définition de « BOM sans les ajustements » (spec : « TBD % de rebut ») |
| O14 | « **BOM des OP/OF sans les ajustements (TBD % de rebut)** » : quel pourcentage de rebut faut-il retirer ? Celui du poste, celui de la fiche article, les deux ? | M | 🔴 | Prévision 2 = définition à figer |
| O15 | Que sont les **quantités fixes** (`FMENG`, quantité par ordre) : leur quantité ne dépend pas de la quantité de l'OF. Confirmer ce traitement. | M | 🟠 | Calcul de la Prévision 3 |
| O16 | Pour un OP, la quantité finale à la conversion (`GAMNG` de l'OF) peut différer de celle à T0. Est-ce une « erreur de volume » que la fiabilité doit corriger (prévision normalisée) ou une erreur de planification à mesurer ? | M | 🔴 | Choix de la normalisation (`volume_scaling_factor`) |

### 2.3 Consommation

| # | Question | Qui | Priorité | Pourquoi |
|---|---|---|---|---|
| O17 | **Conso 1** = « tous les codes de mouvements » : quels codes exactement ? Tous les mouvements imputés à l'OF, y compris les retours (262) et les sorties hors nomenclature ? | M | 🔴 | Définition à figer |
| O18 | **Conso 2** = mouvements « nominaux » (sans la casse). Quels codes sont la casse ? Le motif de mouvement (`grund`) n'est pas dans la Gold : où le trouver (bronze MSEG) ? | M + P | 🔴 | Question M7 de 2026-10-09, toujours ouverte |
| O19 | **Conso 3** = Conso 2 + régularisations d'inventaire au prorata du centre de profit. Quelle méthode exacte ? Quels mouvements d'inventaire (701 / 702, 711 / 712) ? Par composant, par usine, par période ? | M | 🟠 | Non calculée (méthode TBD) |
| O20 | Les **sorties en libre-service** ou sur centre de coût (visserie, fermetures) : elles n'apparaissent pas sur l'OF. Faut-il les compter dans la consommation de l'OF, ou sont-elles hors périmètre ? (Constat RB2–RB3 : consommation / réservation = 0,74 pour les achetés avec rebut d'ensemble) | M | 🔴 | Explique une grande partie des « surstock » |
| O21 | Les **retours** de composants (consommation négative) : doivent-ils réduire la consommation ? Ou sont-ils des erreurs à corriger en amont ? (9 648 cas) | M | 🟠 | Traitement de `is_negative_consumption` |

### 2.4 Périmètre et sélection

| # | Question | Qui | Priorité | Pourquoi |
|---|---|---|---|---|
| O22 | « **Choisir un AF du centre de profit** » : un AF peut-il avoir plusieurs centres de profit (ou la même BOM sur plusieurs centres) ? Quel centre de profit faut-il retenir pour l'OF ? | M + P | 🟠 | Affichage par CP et jointure |
| O23 | Où trouve-t-on la **division** (demandée dans la spec, page 2 et filtre) ? Elle n'est pas dans nos tables actuelles. Source SAP ? Référentiel ? | P | 🔴 | Filtre demandé par la spec |
| O24 | Les OF de **sous-traitance** (mouvements 543) : dans la consommation ou hors périmètre ? (GQ3 : 9 % de la sous-consommation) | M | 🟠 | Influence le surstock affiché |

---

## 3. Questions au métier — la mesure et les pages

### 3.1 Période et calcul

| # | Question | Qui | Priorité | Pourquoi |
|---|---|---|---|---|
| R-1 | **Choix de T0** : toujours le 1er du mois, ou libre (1er février, 1er mars…) ? | M | 🔴 | Notre grille est mensuelle (snapshot au 1er) |
| R-2 | **Tendance hebdomadaire** : la spec demande de « comparer la tendance d'une semaine à l'autre ». Veut-on des T0 **hebdomadaires** (4 fois plus de volume) ou seulement mensuels ? | M | 🔴 | Impact direct sur l'architecture (snapshots) |
| R-3 | **Période « ignorée » et « période d'analyse »** : le schéma de la spec montre T0 → T1 (ignorée) → T2 (analyse). Confirmer : T1 et T2 sont les bornes des OF retenus. | M | 🟠 | Définition de l'intervalle |
| R-4 | **Bonus** : faire varier T1 et T2 librement (T2 jamais dans le futur). Est-ce indispensable pour la v1 ? | M | 🟢 | Non livré à ce jour |
| R-5 | **Erreur moyenne** : moyenne simple des erreurs composants, ou moyenne pondérée par la valeur (prix × quantité) ? | M | 🔴 | Fiabilité = 100 % − moyenne (la spec ne dit pas pondéré) |
| R-6 | Pour un composant **prévu mais jamais consommé** (ou l'inverse), l'erreur vaut 100 % ? Faut-il distinguer « prévu non consommé » et « consommé non prévu » ? | M | 🟠 | Règle de calcul de l'erreur (conso = 0) |
| R-7 | **Tolérance** : un écart de 1 % est-il « OK » ? Quel seuil pour OK / surstock / manquant ? | M | 🟠 | Le statut est aujourd'hui une égalité stricte |
| R-8 | Comment traiter le **volume** : une prévision de 10 pièces pour un OF de 8 pièces. Normaliser (spec : oui, implicitement) ou mesurer l'écart de volume ? | M | 🔴 | Voir O16 |

### 3.2 Pages et filtres

| # | Question | Qui | Priorité | Pourquoi |
|---|---|---|---|---|
| R-9 | **Page 1 (résumé)** : un tableau des centres de profit par site avec la fiabilité. Quels autres indicateurs (nombre d'AF, nombre d'OF pertinents) ? | M | 🟠 | Maquette à valider |
| R-10 | **Page 2 (analyse)** : un graphique d'évolution dans le temps. Quel pas (semaine, mois) ? Quelle période par défaut ? | M | 🟠 | Lié à R-2 |
| R-11 | **Compteur d'OF pertinents** : un OF est « pertinent » s'il est dans le périmètre (début, fin) **et** a une prévision à T0 ? Ou autre définition ? | M | 🟠 | Définition à figer |
| R-12 | **Liste des AF** avec description et site : faut-il une liste filtrée par division, ou tous les AF d'un CP ? | M | 🟢 | Ergonomie |
| R-13 | **Filtrer sur toutes les colonnes** : impossible en DirectQuery pour les moyennes (cf. notes de la Proj). Acceptable de filtrer seulement sur les colonnes de dimension ? | M + P | 🟠 | Contrainte technique Power BI |
| R-14 | Colonnes du composant : quelle **durée d'approvisionnement** (externe, interne, les deux) ? Quel **prix** (standard actuel, à T0) ? Quelle **classification** (TBD : famille / classe / code 0, L ou 0&L) ? Quelle définition de **PF Usage** ? | M | 🟠 | Plusieurs colonnes sans définition |
| R-15 | **Page 3 (optionnelle)** : composants par poste de nomenclature et AF, avec le poste `9999` pour les fantômes. Est-elle nécessaire pour la première version ? | M | 🟢 | Choix de périmètre |
| R-16 | **Commentaires par poste** : où sont-ils stockés (table dédiée, saisie dans Power BI, autre) ? Qui les lit, qui les écrit ? | M + P | 🟢 | Architecture à définir |
| R-17 | **Tests** de la spec : « comparer la tendance d'une semaine à l'autre » (ME Process & Tools) et « identifier les personnes à contacter pour utiliser PLM » (ME). Que veut dire « utiliser PLM » ici ? | M | 🟠 | Cas d'usage non clairs |

### 3.3 Validation et plan d'action

| # | Question | Qui | Priorité | Pourquoi |
|---|---|---|---|---|
| R-18 | Quelle est la **date cible** de livraison ? | M | 🟠 | Priorisation |
| R-19 | Les trois validateurs (BODIN Anthony, CONTINENTE Matthieu, LAPORTE Pierre) : quand valident-ils ? Qui valide le périmètre et le calcul ? | M | 🔴 | Aucune date de validation |
| R-20 | Faut-il une **documentation utilisateur** et une **formation** ? La spec indique « N/A ». | M | 🟢 | Plan d'action vide |
| R-21 | Quelle **communication** auprès des équipes (ME, Logistique, Production) ? | M | 🟢 | Plan d'action vide |

---

## 4. Questions d'architecture et de plateforme (à poser à l'équipe LEAP)

### 4.1 Source et historique

| # | Question | Qui | Priorité | Pourquoi |
|---|---|---|---|---|
| P-1 | Les **stacks** `prod_landingzone.sap_latecoere_ecc6.*_stack` sont-ils garantis **sans purge** dans la durée ? (Constat : VACUUM 40 jours, aucune suppression observée.) | P | 🔴 | Si purge, l'historique est perdu |
| P-2 | Lire les stacks **directement depuis la landing zone** dans un notebook Gold est-il accepté par l'équipe LEAP, ou faut-il des tables `*_history` en bronze ? | P | 🔴 | Déjà noté en H2 |
| P-3 | Existe-t-il un projet pour créer des **tables bronze d'historique** (`*_history`) ? Qui le porte ? | P | 🟠 | Solution cible à plus long terme |
| P-4 | Le **service principal** `job-runner-sa-*` a-t-il le droit de lire `prod_landingzone` ? | P | 🔴 | Déjà confirmé oui (H8), à vérifier pour la prod |
| P-5 | La table `dev_proj.supply_chain.current_average_bom` : qui la maintient, quelle est sa fréquence de mise à jour, et sa source ? | P | 🟠 | Lien avec la BOM commune (M13) |

### 4.2 Choix d'architecture à valider

| # | Question | Qui | Priorité | Pourquoi |
|---|---|---|---|---|
| P-6 | **Séparation** : notre historique BOM dans une table séparée (`bom_item_history`) plutôt que dans la Gold officielle `production.BOM`. Recommandation : séparer. Validation par l'équipe ? | P | 🔴 | Décision D1 de la spec |
| P-7 | **Réconciliation** : notre historique doit-il reproduire la BOM Silver officielle au dernier snapshot, à l'identique ? Quelles règles de différence sont acceptées (statut `12`, `cslty = X`, usine NULL) ? | P + M | 🔴 | Décisions D2 et D3 |
| P-8 | **Fréquence des snapshots** : mensuelle (42 snapshots, 3,7 M lignes par snapshot) ou hebdomadaire (≈ 4 fois plus) ? Coût et temps de calcul acceptables ? | P | 🔴 | Lié à R-2 |
| P-9 | **Environnement cible** : la spec demande INT et PROD. Notre méthode est dev → uat → prod (CLAUDE.md). Quel est l'environnement « INT » : `uat` ? | P | 🔴 | Déploiement |
| P-10 | **Job** : un job dédié (`D_` / `W_` / `Z_` selon la convention de nommage) ou intégré au job Silver existant ? Planification ? | P | 🟠 | Convention LEAP §11 |
| P-11 | **Propriétaire** du produit (Gold et Proj) : équipe Production, Supply Chain, ou plateforme ? | P + M | 🔴 | Gouvernance |
| P-12 | **Numéro de use case** : uc093 est-il confirmé ? Le domaine Proj `supply_chain` est-il le bon ? | P | 🟠 | Nommage des tables et des dossiers |
| P-13 | **Cluster** : job en mode single node (comme le job Silver) suffit-il, ou faut-il un cluster multi-nœuds pour le backfill (STAS seul : 15,8 milliards de lignes dans la landing zone) ? | P | 🟠 | Coût et durée |
| P-14 | **Bibliothèque `leap_utils`** : `sap_number`, `month_starts`, `keep_latest_row` sont dupliqués dans nos notebooks. Qui valide leur ajout dans `leap_utils` ? | P | 🟠 | Dette technique (H9) |
| P-15 | **Commentaires par poste** (R-16) : une table manuelle, une saisie dans Power BI, ou un autre outil (PLM ?) ? | P | 🟢 | Architecture de saisie |

### 4.3 Données et qualité

| # | Question | Qui | Priorité | Pourquoi |
|---|---|---|---|---|
| P-16 | **Lignes à `plant` NULL** dans la Silver (691 lignes, 80 liens MAST) : données SAP incomplètes, ou liens de test ? | P | 🟠 | Voir spec §8 |
| P-17 | **Statut BOM `12`** (97 238 lignes dans la Silver) : signification, et faut-il l'exclure comme le `02` ? | M + P | 🔴 | Décision D9 |
| P-18 | **Doublons** de MARC et MARM dans certaines extractions (ex. article F5391312700300, usine 1900) : correction à la source demandée à l'équipe d'ingestion ? | P | 🟢 | H4 |
| P-19 | **Extractions** : `resb_stack` contient deux familles de fichiers (ACT et NOACT) avec des `extraction_timestamp` voisins. Est-ce stable dans le temps ? | P | 🟢 | Hypothèse de la sélection B |
| P-20 | **Extraction RESB du 2025-03-31** : 38,7 M lignes avec `xloek = X` (anomalie). Explication de l'équipe d'ingestion ? | P | 🟠 | Biais possible sur les besoins |
| P-21 | **Lignes à `file_mode = DELTA`** dans MARC et MARM : elles ne sont pas des snapshots ; confirmer qu'elles ne servent à rien ici. | P | 🟢 | Traitement déjà en place |

### 4.4 Power BI et restitution

| # | Question | Qui | Priorité | Pourquoi |
|---|---|---|---|---|
| P-22 | **Mode de connexion** Power BI : DirectQuery ou Import ? Les moyennes par composant sont pré-calculées (contrainte DirectQuery). Confirmer ce choix. | P + M | 🟠 | Architecture de la Proj |
| P-23 | Faut-il une **rétention** des résultats (historique des fiabilités calculées) pour la courbe d'évolution, ou recalcule-t-on tout à chaque rafraîchissement ? | P | 🟠 | Volume et cohérence dans le temps |
| P-24 | Rafraîchissement du rapport : **mensuel** (décision du 2026-10-12) ou hebdomadaire (R-2) ? | M | 🟠 | Cohérent avec R-2 |

---

## 5. Décisions déjà prises (pour mémoire)

| Date | Décision | Source |
|---|---|---|
| 2026-10-06 | Snapshots mensuels (au lieu du SCD2 bitemporel) | spec v4, commit `ea82192` |
| 2026-10-09 | Prévision 2 = besoin de l'OF sans rebut composant (`ESMNG`) | `questions_ouvertes.md` (R1–R3) |
| 2026-10-09 | Rebut d'ensemble des achetés non pris en compte (jamais appliqué aux OF) | constat RB1 |
| 2026-10-12 | Fréquence des photos mensuelle, `debug_plant` conservé | `questions_ouvertes.md` |
| 2026-10-12 | Défaut P1 = 6 mois, P2 = 3 mois ; option P1 = 3 mois | spec v2 et `questions_ouvertes.md` |
| 2026-10-13 | Notre historique BOM = table séparée (D1 = a) | spec v5 §9.1 |
| 2026-10-13 | Statut BOM aligné sur la Silver (`02` et vide exclus, `12` gardé) | notebook A |
| 2026-10-13 | Runs de debug écrivent dans des tables `*_debug` | notebooks A, B, C |

---

## 6. Ce que je te demande d'expliquer (pour que je comprenne mieux le sujet)

Ces points ne sont pas des questions au métier : ce sont des éléments à me confirmer ou à me montrer pour que la suite soit juste.

| # | Sujet | Ce qu'il me faut |
|---|---|---|
| T-1 | La **BOM commune** par CA | Un exemple réel : un CA, un MSN, un AF, la BOM commune, et la BOM SAP correspondante |
| T-2 | Le **dashboard Average BOM** (Power BI) | Une capture ou un export d'une page, avec les filtres utilisés |
| T-3 | Une **commande OF** réelle | Un OF, son OP d'origine (s'il existe), ses réservations, ses mouvements 261 / 262 : pour vérifier notre compréhension |
| T-4 | Le **document IN_P_0030** | Le document, ou au moins son sommaire |
| T-5 | **Qui fait quoi** dans le processus | Un schéma simple : ME (modifie la BOM), Production (lance les OF), Logistique (sorties de stock) |
| T-6 | Les **codes de mouvements** | La liste des types utilisés sur les OF, avec leur sens (sortie, retour, casse, sous-traitance) |
| T-7 | Les **ECN** | Un exemple réel de modification de BOM avec son numéro de modification, pour valider la règle de versions |

---

## 7. Notes méthodologiques pour toi

- **Pourquoi les OP comptent** : une prévision à T0 vient en grande partie d'OP (lab : environ 5 271 OP contre 2 OF pour la période par défaut). Sans eux, la fiabilité ne mesure presque rien.
- **Pourquoi la BOM Silver et notre A peuvent différer** : elles appliquent des règles différentes (statut, usine, version STAS). Le détail est dans la spec §8.
- **Pourquoi la fréquence (mois ou semaine) change tout** : passer à la semaine multiplie le volume par environ 4 et change la définition de « T0 » dans le rapport.
- **Ce qui n'est pas encore mesuré** : la consommation des achetés (surstock apparent), les sorties hors OF et la casse (Conso 2 dépend d'une définition métier).
