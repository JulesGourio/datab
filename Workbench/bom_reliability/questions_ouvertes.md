# BOM reliability — Questions ouvertes (2026-10-09)

Chaque question indique **à qui la poser**, si elle **bloque**, et **ce que fait le code aujourd'hui** sans réponse
(défaut provisoire). 🔴 bloquant pour avancer / livrer · 🟠 change les résultats, à trancher avant la recette métier ·
🟢 confort, peut attendre.

## 1. Historisation et données — équipe data / plateforme LEAP

| # | Question | Niveau | Défaut actuel |
|---|---|---|---|
| H1 | Les tables `prod_landingzone.sap_latecoere_ecc6.*_stack` sont-elles **garanties sans purge** dans la durée (politique de rétention, archivage) ? Si une purge est prévue, il faut passer à un historique incrémental (on ne pourra plus tout reconstruire). | 🔴 | Reconstruction complète à chaque run (VACUUM 40 j observé, aucune suppression de données) |
| H2 | Lire les stacks de la **landing zone directement depuis un notebook Gold** est-il accepté par l'équipe LEAP, ou faut-il créer des tables bronze `*_history` ? | 🔴 | Lecture directe, déclarée en dette technique |
| H3 | `resb_stack` : deux familles de fichiers `SAP-RESB-F-ACT` / `SAP-RESB-F-NOACT` (GQ1). Les deux familles ont-elles toujours le même `extraction_timestamp` ? | 🟢 | B prend la dernière extraction de chaque famille avant chaque 1er du mois et les unit |
| H4 | Les extractions MARC / MARM contiennent des **lignes en double** dans le même fichier SAP (ex. article F5391312700300, usine 1900). Connu ? À corriger à la source ? | 🟢 | Une seule ligne gardée (valeurs identiques) |
| H5 | Peut-on **réutiliser ou étendre** le job existant `W_3_SAP_AS_Design_BOM_DataAsset` (id `1007135342276807` selon les notes, à confirmer) qui produit `prod_silver.production.bom` et `prod_gold.production.bom` ? Qui en est propriétaire ? | 🟠 | Nouvel actif Gold indépendant, à aligner sur la Silver officielle (spec §8) |
| H6 | Composants absents de `material_exposed` = articles obsolètes / remplacés (GQ2) | ✅ résolu | Unité de base `mara_latest`, descriptions `makt_latest` (FR puis EN) dans A ; C retombe sur ces descriptions |
| H7 | Avant juin 2024, MAST / STAS n'existent pas en stack : on réutilise leur première extraction (juin 2024). Acceptable ? | 🟢 | Oui, flag `_is_backdated_link` |
| H8 | Service principal `job-runner-sa-*` : a-t-il le droit de lire `prod_landingzone` ? | 🔴 pour la prod | Job lab lancé à votre nom |
| H9 | Signature réelle de `table_utils.create_table_view` (vue `_exposed` pas encore créée) et ajout de fonctions communes dans `leap_utils` (`sap_number`, `keep_latest_row`, calendrier de snapshots) : qui valide ? | 🟠 | Fonctions copiées dans chaque notebook (dette technique) |

## 2. Profondeur et fréquence de l'historique — métier + data

| # | Question | Niveau | Défaut actuel |
|---|---|---|---|
| P1 | **Depuis quand** voulez-vous mesurer ? Les données permettent : BOM depuis **mai 2023**, besoins MRP (OP/OF) depuis **octobre 2023**. Avec P1 = 3 et P2 = 3 mois, la première mesure complète porte sur T0 = oct. 2023 (OF terminés avant avril 2024). Avant : impossible. | 🔴 | Tout ce qui est disponible |
| P2 | **Fréquence de T0** : 1er de chaque mois, ou chaque semaine ? (Hebdo = T0 au plus près de la date choisie, mais ~4× plus de volume.) | 🔴 | Mensuel (1er du mois) |
| P3 | ✅ résolu par la nouvelle expression de besoin : (P1, P2) = (6, 3) par défaut, (3, 3) en option. Variation libre de T1 / T2 = bonus non livré. | ✅ | — |
| P4 | **Fréquence de rafraîchissement** du rapport (mensuel suffit ?) | 🟢 | Mensuel |

## 3. Rebuts — métier (méthodes / planification)

| # | Question | Niveau | Défaut actuel |
|---|---|---|---|
| R1 | Quand vous saisissez « **+X % de rebut pour ce composant** », **où** le saisissez-vous dans SAP ? (a) nomenclature CS02, poste, « Rebut composant » ; (b) fiche article MM02 vue MRP 4, « Rebut composant » ; (c) fiche article, « Rebut ensemble ». Constat : (a) 1 186 postes, (b) 350 articles, (c) **~32 000 articles achetés** — et (c) n'agit que sur les achats, pas sur les OF. | 🔴 | (a) et (b) pris en compte, (c) ignoré |
| R2 | Faut-il **afficher** le rebut côté achat (cas c) en colonne d'information ? Et/ou créer une prévision « Prévision 1 + rebut achat » ? | 🟠 | Non |
| R3 | **Prévision 2 = « sans les ajustements »** : on retire seulement le rebut composant ? Aussi le rebut d'ensemble de l'AF ? Et « ajustements » veut-il dire aussi les **modifications manuelles sur l'OF** (composants ajoutés à la main, quantités changées) ? | 🔴 | Prévision 2 = besoin SAP avant rebut composant (`esmng`) ; modifications manuelles conservées |
| R4 | **Prévision 3 (BOM standard)** : avec ou sans le rebut composant ? | 🟠 | Avec |

## 4. Règles de la mesure de fiabilité — métier

| # | Question | Niveau | Défaut actuel |
|---|---|---|---|
| M1 | Pour un OF qui **n'existait pas encore à T0**, la prévision à T0 est-elle celle de l'**ordre planifié (OP)** dont il est issu ? | 🔴 | Oui |
| M2 | OF **sans OP ni OF à T0** (créé directement après T0) : exclure de la fiabilité ? | 🔴 | Exclu, mais listé dans la table des OF |
| M3 | **Écart de volume** : l'OP à T0 était pour 10 pièces, l'OF final pour 8. Doit-on **ramener la prévision à la quantité finale** (sinon l'erreur de volume pollue la fiabilité BOM) ? | 🔴 | Oui, prévision × quantité finale / quantité à T0 |
| M4 | **Quantité finale** de l'OF : quantité planifiée finale, ou livrée + rebutée ? | 🟠 | Quantité planifiée (GAMNG) |
| M5 | « OF **commencés après T1 et terminés avant T2** » : fin = date de fin réelle, ou date TECO / clôture ? | 🟠 | Dates réelles de `work_orders_sap_exposed` |
| M6 | **Types d'OF** dans le périmètre : Details Parts, Assembly Parts, Rush Orders — tous ? | 🟠 | Tous (hors annulés) |
| M7 | **Conso 2 « nominale, sans la casse »** : SAP n'a pas de type de mouvement casse imputé aux OF (seulement 261/262). Comment identifier la casse ? (motif de mouvement, magasin, autre ?) Sinon Conso 1 ≈ Conso 2. GQ4 : pas de `grund` dans `part_movement_exposed`. | 🔴 | Conso 2 = 261/262 |
| M8 | **Conso 3** : les régularisations d'inventaire (701/702) ne sont jamais imputées à un OF. Méthode de répartition : par composant × usine × période, au prorata des consommations des OF ? Par magasin ? Par centre de profit ? | 🟠 | Non calculée (colonne vide) |
| M9 | Mouvements **531/532** sur OF (souvent l'article fictif « SPLIT », sous-traitance) : dans Conso 1 ? | 🟢 | Inclus dans Conso 1 |
| M10 | Consommation de **l'AF par son propre OF** (rework, 2 568 mouvements/an) : exclure ? | 🟢 | Exclue |
| M11 | **Consommation nette négative** (plus de retours que de sorties, 9 648 cas) : ramener à 0 ? exclure ? | 🟢 | Ramenée à 0, flag `is_negative_consumption` |
| M12 | Postes **vrac** (~98 000 postes, jamais sortis sur OF) et **hors stock** (N) : inclus dans la fiabilité ? | 🟠 | Inclus |
| M13 | Catégories de poste propres à Latécoère **Z, 0, 1, 2, 4, U, V** : que signifient-elles ? (Z et 0 sont bien consommés.) | 🟢 | Incluses (sauf D document et T texte) |
| M14 | **Fantômes** : quelle alternative de BOM utiliser pour éclater un fantôme qui en a plusieurs ? | 🟢 | La plus petite alternative |
| M15 | **Tolérance « OK »** : un écart de 0,5 % est-il « OK » ou « surstock » ? Faut-il un seuil (ex. ±5 %) ? | 🟠 | Égalité stricte |
| M16 | **Agrégation** par AF / centre de profit / usine : moyenne simple des erreurs composants, moyenne des fiabilités AF, ou **pondérée par la valeur** ? | 🔴 | Moyenne simple des erreurs composants |

## 5. Contenu du rapport — métier

| # | Question | Niveau | Défaut actuel |
|---|---|---|---|
| C1 | **Classification du composant** : famille / classe / code « 0, L, 0&L » — quelle définition, quelle source ? | 🟠 | `familly_std`, `classe_std`, `abc_indicator` |
| C2 | **« PF usage »** : définition ? (`material_plant.quota_usage` ? `PF_pourcentage_affectation` du selfservice ?) | 🟠 | Non livré |
| C3 | **Prix composant** : prix actuel, ou prix à T0 (historique par période fiscale disponible) ? | 🟢 | Prix standard budget actuel |
| C4 | **Durée d'approvisionnement** : délai externe (achat), interne (fabrication), ou les deux ? | 🟢 | Les deux colonnes |
| C5 | **Page 3** (détail par poste de nomenclature, fantômes en poste 9999) : à faire ? | 🟢 | Non |
| C6 | **Commentaires par poste** : où les saisir et les stocker (write-back Power BI, table manuelle) ? | 🟢 | Non |

## 6. Projet et conventions LEAP — chef de projet / équipe LEAP

| # | Question | Niveau | Défaut actuel |
|---|---|---|---|
| L1 | Nom du dossier `proj/` (numéro de use case = 93, domaine Proj `supply_chain` confirmé) | 🟢 avant Bitbucket | `uc093-bom_reliability` |
| L2 | **DAS + Test Definition** : qui les rédige ? (règle LEAP : pas de build sans eux) Qui valide les exemples de référence dans SAP (CS03, CO03, MB51) ? | 🔴 avant Bitbucket | Spec technique + exemples Genie seulement |
| L3 | Nommage clé primaire / contrainte : `{table}_ID` et `gold_{table}_PK` confirmés ? | 🟢 | Oui (Confluence) |
| L4 | Vue `_exposed` requise pour les deux tables Gold d'historique ? | 🟢 | Oui |

## Détail R1 / R3 (2026-10-09)

Exemple : AF « Porte », 10 vis par pièce, OF de 100. Le rebut peut être saisi à 4 endroits :
1. poste de BOM (CS02, rebut composant) → l'OF réserve 1 100 vis ;
2. fiche de la vis (MM02 MRP 4, rebut composant) → idem ;
3. fiche de l'AF (rebut ensemble) → OF lancé pour 105, tous composants +5 % ;
4. fiche de la vis (rebut ensemble) → **l'OF réserve 1 000**, seuls les achats sont gonflés (~32 000 articles achetés).
Si la perte est réelle, le cas 4 fait ressortir le composant en « manquant ».

Prévision 2 « sans ajustements » : (a) sans rebut composant (code actuel, `esmng`) ; (b) sans rebut d'ensemble
aussi ; (c) sans les modifications manuelles de l'OF (composants ajoutés, quantités changées) — alors ≈ Prévision 3.

Vérifications Genie associées : RB1–RB4 (rebut), RA1–RA2 (ajustements) — texte dans la réponse du 2026-10-09.

## Mise à jour après RB1–RB4 / RA1–RA2 (2026-10-09)

- **R1 — réponse des données** : le rebut qui gonfle les réservations d'OF vient à 83 % du **rebut composant de la
  fiche article** (MM02 MRP 4, `kausf`), à 17 % de saisies sur l'ordre, quasiment jamais du poste de BOM. Le rebut
  d'ensemble n'est appliqué ni aux OF (AF : `gasmg` toujours 0) ni aux réservations des achetés. Question restante
  au métier : le « +20 % » est-il le rebut composant de la fiche article (pris en compte) ou le rebut d'ensemble des
  achetés (ignoré par SAP pour les OF) ?
- **R2** : priorité abaissée 🟢 — les articles avec rebut achat ne sont pas surconsommés sur les OF (au contraire).
- **R3 — simplifiée** : (b) sans objet (rebut d'ensemble jamais appliqué). Reste (a) « sans rebut composant » —
  code actuel — ou (c) « sans modifications manuelles », que la Prévision 3 (BOM standard) couvre déjà.
  Proposition : Prévision 2 = (a), à faire valider.
- **M17 (nouveau) 🟠** : la visserie / fermetures achetées avec rebut d'ensemble est consommée sur les OF à 74 % de
  la réservation (94 % pour les autres achetés). Pourquoi ? (sorties en libre-service ou sur centre de coût,
  vrac, OF soldés sans sortie…) Ces composants ressortiront en « surstock » dans le rapport.


## Décisions du 2026-10-09

- **R1 / R2 fermées** : le « +20 % » de la demande était un exemple, pas une règle métier. On suit SAP : Prévision 1
  = besoin de l'OF avec le rebut appliqué par SAP (rebut composant du poste, sinon de la fiche article) ; le rebut
  d'ensemble (jamais appliqué aux OF) est ignoré ; pas de colonne « rebut achat ».
- **R3 fermée** : Prévision 2 = besoin de l'OF sans rebut composant (`esmng`) — définition actuelle conservée.
- **H1** : historique **incrémental** à prévoir, probablement via des tables **bronze `*_history`** (à clarifier :
  qui les construit, plateforme ou projet).
- **H8** : oui, le service principal lit `prod_landingzone`.
- **H9** : signature de `create_table_view` à relever dans Databricks (commande donnée).

## Mise à jour 2026-10-09 (GQ1–GQ4)

- H3 → 🟢 (familles ACT / NOACT, B corrigé ; reste à confirmer les timestamps).
- H6 → ✅ (MAKT).
- M7 : la Gold ne porte pas de motif de mouvement ; il faudrait lire `grund` dans le bronze MSEG si le métier
  identifie la casse par un motif.
- M17 : les 543 (sous-traitance) expliquent ~9 % de l'écart ; le reste est à expliquer par le métier.

## Décisions 2026-10-12

- Fréquence des photos : **mensuelle** pour l'instant (hebdomadaire plus tard si besoin).
- Paramètre `debug_plant` : **conservé**.
- Historisation incrémentale : pistes Genie (full rebuild depuis les stacks / `replace_where` par `snapshot_date` /
  table Gold `_history_stack` alimentée chaque jour). Choix en attente de la rétention des stacks et de la signature
  réelle de `table_utils.save_table` (`replace_where`, `merge_schema`) — à lire dans le package, pas via Genie.


## Mise à jour 2026-10-12 (expression de besoin v1 révisée)

- Période par défaut P1 = 6, P2 = 3 ; option P1 = 3 (P3 résolu). Avec P1 = 6, la première mesure complète est T0 = oct. 2023, OF terminés avant juillet 2024.
- Nouveaux points ouverts : source de la **division** (page 2), définition de **PF usage**, méthode de la **Conso 3**, famille / classe / code 0 ou L, besoin d'une tendance **hebdomadaire** (cas d'usage « d'une semaine à l'autre ») alors que les snapshots sont mensuels.

## Mise à jour 2026-10-13 (notebooks existants)

- **M7** : piste pour la Conso 2 « nominale » : le notebook budget exclut les consommations sur réservations de catégorie
  de poste 2 à 5 (`RESB_postp`). À chiffrer (Genie X2, X3) et à faire confirmer par le métier avant de l'appliquer.
- **L6** : définition d'un fantôme : `MARC.SOBSL = '50'` chez nous, `procurement_special_type in (50, 55)` et
  `bulk_material = X` dans le notebook forecast existant. À aligner (Genie X4).
- **L7** : composants dont le code commence par `49` exclus dans le notebook forecast existant : raison ? (Genie X5)
- **L8** : `lead_time` unique : délai externe pour un article acheté, interne sinon (règle du notebook existant) :
  à confirmer pour l'écran de détail.
- Analyse complète : `analyse_notebooks_existants.md`. Trois autres notebooks sont à venir.

## Mise à jour 2026-10-13 (revue de la Silver officielle)

| # | Question | Niveau | Défaut actuel |
|---|---|---|---|
| S1 | Aligner notre A sur la BOM Silver officielle (exclusions STKO `02`/vide, MAST `cslty = X`, MAST sans usine), ou garder nos règles ? Voir spec §8 et §9 (D1–D3). | 🔴 avant Bitbucket | Nos règles (validité résolue, pas de filtre statut) |
| S2 | Les 691 lignes Silver à clé NULL sont-elles les liens MAST sans usine (80 liens) ? Requête de contrôle en spec §8.2. | 🟠 | Non vérifié |
| S3 | L'écart de lignes entre notre A (3 688 354 au 2026-10-01) et la Silver (3 230 287 au 2026-10-13) : à mesurer à date égale. | 🟠 | Non expliqué |
| S4 | Sémantique de `quantity` dans `bom_exposed` (pondéré par probabilité / unité de base) : documenter et exposer aussi le brut ? | 🟠 | Non documenté |
| S5 | Job `W_3_SAP_AS_Design_BOM_DataAsset` : YAML reçu le 2026-10-13. Paramètres `pipeline_read_env`, `pipeline_write_env`, `reference_read_env` = **prod** (donc la Silver et la Gold écrivent en prod). Pas de bloc `schedule` dans le YAML : planning à confirmer (UI Databricks ou job appelant). | 🟠 | Résolu pour l'environnement ; planning ouvert |
| S6 | `udf` non importé dans `create_gold_material_master_data_plant` : la version déployée est-elle identique au fichier uploadé ? | 🟠 | Non vérifié |
| S7 | Nommage `BOM` (majuscules) de la Silver et de la Gold officielles, contre CLAUDE.md §8 (snake_case). | 🟢 | Non modifié (hors de notre périmètre) |

## Mise à jour 2026-10-13 (requêtes de contrôle)

| # | Statut |
|---|---|
| S2 | ✅ résolu : les 691 lignes à clé NULL ont `plant` NULL |
| S3 | 🟠 ouvert : la table lab ne contient plus le run complet (63 702 lignes au 2026-10-01). Refaire le run complet avant de comparer. |
| S5 | 🟠 : environnement `prod` confirmé ; planification absente du YAML, à confirmer dans l'UI |
| S8 (nouveau) | 🔴 Sens du statut BOM `12` (97 238 lignes dans la Silver). Exclure comme `02`, ou garder ? |
| S9 (nouveau) | 🟠 Faut-il exclure `cslty = X` et les liens MAST sans usine dans notre A pour aligner sur la Silver ? (D2 / D3 de la spec) |

## Décision 2026-10-13

- Statut BOM : A applique la règle de la Silver (`02` et vide exclus, avant le choix de la dernière version). Le statut `12`
  est **gardé** comme dans la Silver. **Question ouverte au métier** : que signifie le statut `12` (97 238 lignes) ?
- `cslty = X` et lignes MAST sans usine : **pas encore alignés** (question S9 ouverte).

## Renvoi 2026-10-13

La liste complète des questions métier, OP/OF et architecture (spec v2) est dans `questions_metier_et_architecture.md`.

## Mise à jour 2026-10-13 (POC Average BOM)

- **L9** : matière en vrac (`schgt`) exclue du POC existant ; garder ou exclure dans la fiabilité ? (B les marque
  `is_bulk_material`, C les garde.) Même question pour le composant `SPLIT` et le statut usine `05`.
- **L10** : périmètre de la demande : branche **IS**. Filtre de branche à ajouter dans C ? Exclure les ordres « rush »
  (types ZP09, ZP10, ZP11, YP09, YP10, YP11) comme le POC ? (Genie X6)
- **L11** : « PF usage » = `PF_pourcentage_affectation` du composant pour le centre de profit de l'AF ? À confirmer, puis
  ajouter la colonne dans C (Genie X7).
- **M7 / Conso 3** : catégories `RESB_postp` : `1` = pièce manquante, `2`-`5` = surconsommation (manuelles) : piste pour la
  Conso 2. Le pourcentage d'affectation par centre de profit est la piste pour la Conso 3.
- **L6** : le POC existant utilise `sobsl = 50` seulement (comme nous) ; le notebook forecast ajoute `55` et le vrac.

## Mise à jour 2026-10-13 (résultats X1 à X9)

- **L6** : on garde `sobsl = 50` (55 = approvisionnement spécial, 199 articles consommés en 2025). ✅ proposé
- **M7** : exclure les consommations sur réservations 2 à 5 changerait la Conso 2 de moins de 1 % (X3) ; la signification
  de `RESB_postp` est contradictoire (POC contre Genie) : **à demander au métier**, priorité basse.
- **L10** : pas de colonne branche dans `work_orders_sap_exposed` : comment définir le périmètre IS (usines, site, division) ?
  Types ZP09, ZP10, ZP11, YP09, YP10, YP11 (10,6 % des OF de 2025) : « rush » ou retouche, à garder ou exclure ?
- **L11** : « PF usage » : les clés de `PF_pourcentage_affectation` (E2P, E7P…) ne sont pas les centres de profit numériques
  de C : vérifier la correspondance avant d'ajouter la colonne (Genie X10).
- **L9** : vrac = 4,5 % des lignes de réservation (1 299 composants) ; `SPLIT` = 2 783 lignes ; à décider.
- **X1** : `planned_orders_and_work_order_stack_sap` n'est pas un historique : rien à réutiliser pour B.

## Mise à jour 2026-10-13 (X10 et X11)

- **L10** : périmètre IS : `Branch` (IS / AS) existe dans `material_master_data_plant_exposed` ; les 14 usines `2xxx` sont toutes IS,
  mais des articles IS existent aussi dans les usines 1000 et 1900 (partagées avec AS). C filtre maintenant sur la branche
  de l'AF (widget `branch`, `IS` par défaut). Types d'ordre ZP09, ZP10, ZP11, YP09, YP10, YP11 : gardés, à confirmer.
- **L11** : « PF usage » livré en `component_PF_usage_ratio` : les clés du dictionnaire sont des centres de profit
  opérationnels (100, 101, 116, 131, 227, 310…) ; le centre de profit d'un AF peut être un code chapeau (98A, 98M, 98D…)
  absent du dictionnaire (valeur NULL). À confirmer avec le métier : faut-il prendre le centre de profit de l'OF ?

## Décision 2026-10-26

- **L10** : pas de filtre de branche par défaut (toutes les branches). Le widget `branch` de C reste disponible (`IS` ou `AS`).
