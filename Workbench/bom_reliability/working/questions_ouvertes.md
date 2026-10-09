# BOM reliability — Questions ouvertes (2026-10-09)

Chaque question indique **à qui la poser**, si elle **bloque**, et **ce que fait le code aujourd'hui** sans réponse
(défaut provisoire). 🔴 bloquant pour avancer / livrer · 🟠 change les résultats, à trancher avant la recette métier ·
🟢 confort, peut attendre.

## 1. Historisation et données — équipe data / plateforme LEAP

| # | Question | Niveau | Défaut actuel |
|---|---|---|---|
| H1 | Les tables `prod_landingzone.sap_latecoere_ecc6.*_stack` sont-elles **garanties sans purge** dans la durée (politique de rétention, archivage) ? Si une purge est prévue, il faut passer à un historique incrémental (on ne pourra plus tout reconstruire). | 🔴 | Reconstruction complète à chaque run (VACUUM 40 j observé, aucune suppression de données) |
| H2 | Lire les stacks de la **landing zone directement depuis un notebook Gold** est-il accepté par l'équipe LEAP, ou faut-il créer des tables bronze `*_history` ? | 🔴 | Lecture directe, déclarée en dette technique |
| H3 | `resb_stack` mélange plusieurs fichiers SAP (réservations ouvertes / clôturées) selon les jours. **Quels fichiers** (`file_name`) contiennent les besoins ouverts ? | 🟠 | Heuristique : on prend l'extraction qui a le plus de lignes ouvertes avant chaque 1er du mois |
| H4 | Les extractions MARC / MARM contiennent des **lignes en double** dans le même fichier SAP (ex. article F5391312700300, usine 1900). Connu ? À corriger à la source ? | 🟢 | Une seule ligne gardée (valeurs identiques) |
| H5 | Peut-on **réutiliser ou étendre** le job existant `W_3_SAP_AS_Design_BOM_DataAsset` (`prod_silver.production.bom`) plutôt qu'un nouvel actif ? Qui en est propriétaire ? | 🟠 | Nouvel actif Gold indépendant |
| H6 | `prod_gold.master_data.material_exposed` ne contient pas **37 387 composants** de BOM. Quelle est la table de référence complète pour la **description** et les attributs articles (MAKT bronze ?) | 🔴 pour le rapport | Unité de base lue dans `mara_latest` ; descriptions encore lues dans `material_exposed` (trous attendus) |
| H7 | Avant juin 2024, MAST / STAS n'existent pas en stack : on réutilise leur première extraction (juin 2024). Acceptable ? | 🟢 | Oui, flag `_is_backdated_link` |
| H8 | Service principal `job-runner-sa-*` : a-t-il le droit de lire `prod_landingzone` ? | 🔴 pour la prod | Job lab lancé à votre nom |
| H9 | Signature réelle de `table_utils.create_table_view` (vue `_exposed` pas encore créée) et ajout de fonctions communes dans `leap_utils` (`sap_number`, `keep_latest_row`, calendrier de snapshots) : qui valide ? | 🟠 | Fonctions copiées dans chaque notebook (dette technique) |

## 2. Profondeur et fréquence de l'historique — métier + data

| # | Question | Niveau | Défaut actuel |
|---|---|---|---|
| P1 | **Depuis quand** voulez-vous mesurer ? Les données permettent : BOM depuis **mai 2023**, besoins MRP (OP/OF) depuis **octobre 2023**. Avec P1 = P2 = 3 mois, la première mesure complète porte sur T0 = oct. 2023 (OF terminés avant avril 2024). Avant : impossible. | 🔴 | Tout ce qui est disponible |
| P2 | **Fréquence de T0** : 1er de chaque mois, ou chaque semaine ? (Hebdo = T0 au plus près de la date choisie, mais ~4× plus de volume.) | 🔴 | Mensuel (1er du mois) |
| P3 | **Choix de P1 / P2** dans le rapport : seulement 3 / 3 mois, ou une liste (1, 2, 3, 6 mois) ? Chaque combinaison multiplie la taille de la table Proj. | 🟠 | P1 = P2 = 3 mois uniquement |
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
| M7 | **Conso 2 « nominale, sans la casse »** : SAP n'a pas de type de mouvement casse imputé aux OF (seulement 261/262). Comment identifier la casse ? (motif de mouvement, magasin, autre ?) Sinon Conso 1 ≈ Conso 2. | 🔴 | Conso 2 = 261/262 |
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
| L1 | **Numéro de use case** (EPIC JIRA), **domaine** Proj et nom du dossier `proj/` | 🔴 avant Bitbucket | `ucTBD`, `supply_chain` |
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
