# BOM reliability — Briefing complet pour un débutant

| | |
|---|---|
| But du document | Comprendre le sujet à zéro, avant une réunion |
| Date | 2026-10-13 |
| Ce qu'il ne contient pas | Les détails de code, les numéros de versions, les listes de colonnes (voir les autres documents, §12) |

---

## 0. En une phrase

Chez Latécoère, pour fabriquer un produit, on suit une **recette** écrite dans SAP (la **BOM**).
Cette recette dit combien de chaque pièce il faut. Le projet veut savoir : **est-ce que la recette
colle à ce qui est réellement consommé dans l'atelier ?** Si elle est fausse, on prévoit mal les
achats et le stock.

---

## 1. Le problème, avec une image

Imagine une recette de gâteau écrite il y a deux ans : « 200 g de farine, 3 œufs ». Depuis, la
recette a été modifiée plusieurs fois : certains ingrédients ont été remplacés, d'autres ajoutés, et
le boulanger ajuste parfois les quantités. On veut savoir :

1. **Quelle recette était écrite à une date donnée** (la BOM « telle que SAP la connaissait ce jour-là ») ;
2. **Combien de farine a été réellement utilisée** pour les gâteaux faits ensuite ;
3. **Dans quelle mesure la recette a prédit la réalité** : c'est la **fiabilité**.

Dans le monde réel : la BOM est la recette, les gâteaux sont les **OF** (ordres de fabrication), la
farine est un **composant**, et la consommation réelle est sortie du magasin pour fabriquer les OF.

---

## 2. Vocabulaire (à lire une fois)

### 2.1 Les objets de fabrication

| Terme | Explication simple |
|---|---|
| **AF** (article fabriqué) | Le produit qu'on fabrique, par exemple une pièce d'avion. Il a un numéro (`material_number`). |
| **Composant** | Une pièce ou matière utilisée pour fabriquer l'AF : vis, tôle, colle… Il a aussi un numéro. |
| **BOM** (Bill Of Material) | La **nomenclature** : la liste des composants nécessaires pour fabriquer un AF, avec la quantité de chacun. C'est la « recette ». |
| **Nœud / poste** | Une ligne de la BOM, c'est-à-dire un composant avec sa quantité. Chaque poste a un identifiant interne (`node`). |
| **Alternative** | Une BOM peut exister en plusieurs versions de fabrication (alternatives 01, 02…). Pour un même AF, on peut fabriquer de deux façons. |
| **Usine** (`plant`) | Le site où l'AF est fabriqué (ex. 2400). |
| **Usage** (`BOM_usage`) | Le type de BOM : `1` = production. C'est la seule qui nous intéresse. |
| **Quantité de base** | La BOM est écrite « pour N pièces de l'AF ». Exemple : quantité de base = 10, donc les quantités des postes correspondent à 10 AF. |

### 2.2 Les ordres

| Terme | Explication simple |
|---|---|
| **OP** (ordre planifié) | Un **projet de fabrication** créé par le planificateur (MRP) à partir des besoins de vente ou de stock. Il n'est pas encore lancé à l'atelier. |
| **OF** (ordre de fabrication) | Un ordre **lancé** à l'atelier. Un OP peut être **converti** en OF quand on décide de fabriquer. |
| **Réservation** | Quand un OP ou un OF existe, SAP **réserve** les composants nécessaires : c'est la liste de ce qu'il faudra prendre au magasin. |
| **SB** / **AR** | Codes SAP : `SB` = besoin d'un OP ; `AR` = besoin d'un OF. |
| **Mouvement de stock** | Une sortie ou une entrée physique enregistrée dans SAP. La **consommation réelle** est la somme des mouvements imputés à l'OF. |
| **261 / 262** | Codes de mouvement : `261` = sortie du composant pour un OF ; `262` = annulation (retour). |

### 2.3 Les notions d'imperfection

| Terme | Explication simple |
|---|---|
| **Rebut** (scrap) | Pourcentage de pièces prévu comme perdu en fabrication. Exemple : rebut de 10 % = on prévoit 10 % de matière en plus. |
| **Rebut du poste** (`AUSCH`) | Rebut écrit sur une ligne de la BOM. |
| **Rebut composant de l'article** (`KAUSF`) | Rebut écrit sur la fiche du composant. |
| **Rebut d'ensemble** (`AUSSS`) | Rebut écrit sur la fiche de l'AF. Surtout utilisé pour les pièces achetées. |
| **Fantôme** (phantom) | Un sous-ensemble qu'on fabrique **sans jamais le stocker** : ses composants sont consommés directement. Il ne faut pas le compter comme consommé lui-même. Il est repéré par `SOBSL = 50` dans la fiche du composant. |
| **Vrac** | Matière prise en quantité libre, jamais sortie sur un OF. |
| **Quantité fixe** | Quantité par ordre, qui ne dépend pas de la taille de l'OF. |

### 2.4 Les modifications de BOM

| Terme | Explication simple |
|---|---|
| **ECN** (engineering change notice) | Une modification technique officielle de la BOM : un composant est remplacé, une quantité change. Chaque ECN a un **numéro de modification** (`AENNR`). |
| **Nouveau nœud** | Pour une modification, SAP crée un **nouveau poste** et garde un lien vers l'ancien (`VGKNT`). L'ancien est ensuite **fermé**. |
| **Date de validité** (`DATUV`) | Date à partir de laquelle une version de poste ou d'en-tête est valable. |

### 2.5 L'organisation de l'entreprise

| Terme | Explication simple |
|---|---|
| **Division** | Grand secteur d'activité. La spec demande de filtrer par division ; elle n'est pas encore dans nos données. |
| **Centre de profit** (CP) | Unité de gestion financière. Un CP regroupe des AF. |
| **Branche** | Découpage interne (AS, IS, interconnexion). |
| **CA** | Terme interne dont la définition reste à préciser (la spec ne la donne pas). |
| **MSN** | Numéro de série d'un avion. Un même CA peut donner des avions dont la configuration diffère. |
| **Gel des données clients** | Date après laquelle la configuration client est figée. |

### 2.6 Les noms techniques (SAP et Databricks)

| Nom | Ce que c'est |
|---|---|
| **MAST** | Table SAP : relie un AF (et son usine) à une BOM. |
| **STKO** | Table SAP : en-tête de BOM (une par BOM et alternative, avec sa date de validité). |
| **STAS** | Table SAP : dit quels postes appartiennent à quelle alternative. |
| **STPO** | Table SAP : les postes, c'est-à-dire les composants et leurs quantités. |
| **MARC** | Table SAP : données d'un article dans une usine (rebuts, fantôme, délais). |
| **MARM** | Table SAP : conversions d'unités (ex. 1 boîte = 12 pièces). |
| **MARA / MAKT** | Table SAP : données générales de l'article / libellé. |
| **RESB** | Table SAP : réservations (les besoins des OP et des OF). |
| **PLAF** | Table SAP : les OP. |
| **AFKO / AFPO** | Tables SAP : les OF (en-tête et postes). |
| **MSEG** | Table SAP : les mouvements de stock. |
| **Landing zone** | Zone d'arrivée des données SAP, **historique complet**, jamais écrasé. |
| **Bronze** | Données SAP brutes, **dernier état seulement** (recréées chaque nuit). |
| **Silver** | Données nettoyées et renommées, un état courant. |
| **Gold** | Données prêtes pour le métier. |
| **Proj** | Calculs spécifiques à un cas d'usage (ici la fiabilité). |
| **Snapshot** | Une photo de la BOM à une date donnée. |
| **DirectQuery** | Mode Power BI qui interroge les données à la demande (pas de copie). |

---

## 3. Comment une BOM vit dans SAP

Représentation simplifiée :

```
AF "Porte-A2"  (usine 2400, usage production)
   └── Alternative 01  (une façon de fabriquer)
         ├── Poste 0010 : Vis V1      quantité 10     rebut 5 %
         ├── Poste 0020 : Tôle T3     quantité 1      rebut 0
         └── Poste 0030 : Sous-ensemble F7 (fantôme) quantité 1
               └── Poste 9999 (dans F7) : Colle C2   quantité 0,2
   └── Alternative 02  (autre façon)
         └── …
```

Points à retenir :

- Un **AF** a une ou plusieurs **alternatives**.
- Une alternative est une **liste de postes**.
- Un poste peut être **fantôme** : on ne le consomme pas, on consomme ce qu'il contient.
- Quand un poste change (ECN), SAP crée un **nouveau poste** et ferme l'ancien.

Cas concret à connaître : un poste supprimé n'est pas effacé. Il est **fermé** par une ligne dans la table
d'allocation (STAS), avec un indicateur de suppression. Il faut donc regarder la **dernière version** de
chaque poste pour savoir ce qui est encore valable.

---

## 4. La vie d'un OF, étape par étape

```
 1. Besoin de vente ou de stock
        │
 2. Planificateur crée un OP (ordre planifié)    ← table PLAF
        │       → SAP réserve les composants (SB)  ← table RESB
        │
 3. Décision de fabriquer : l'OP devient un OF   ← tables AFKO / AFPO
        │       → les réservations passent en AR
        │
 4. Atelier sort les composants                  ← mouvements 261
        │       (retours éventuels               ← mouvements 262)
        │
 5. OF terminé                                   ← dates réelles
```

Conséquence importante : **une prévision à une date T0 vient surtout d'OP**. Un OF qui existait déjà à T0
est une prévision directe ; un OF qui a été créé après T0 doit être rattaché à l'OP dont il est issu.

---

## 5. La mesure : Prévisions, Consommations, fiabilité

### 5.1 Les trois dates

```
   T0                           T1                          T2
   │                             │                           │
   │  ← date de la prévision     │ ← on ne garde que les     │ ← fin de la fenêtre
   │    (ce qu'on savait)        │   OF commencés après T1   │   (jamais dans le futur)
   │                             │   et finis avant T2       │
   
   Défaut : T1 = T0 + 6 mois, T2 = T1 + 3 mois
   Option : T1 = T0 + 3 mois, T2 = T1 + 3 mois
```

En clair : on se place **à une date passée T0**, on regarde **ce que SAP prévoyait ce jour-là**, et on
compare à **ce qui a été réellement consommé** par les OF lancés dans la fenêtre [T1, T2].

### 5.2 Les trois prévisions (ce que SAP prévoyait)

| Prévision | Définition | Exemple |
|---|---|---|
| **Prévision 1** | Besoin **brut** des OP et OF à T0 (rebut inclus, tel que SAP le calcule) | 12 vis pour 10 pièces à fabriquer, avec 20 % de rebut |
| **Prévision 2** | Prévision 1 **sans** le rebut composant | 10 vis |
| **Prévision 3** | La **BOM standard** à T0 × la quantité finale de l'OF | BOM dit 10 vis par pièce, OF de 8 pièces → 80 vis |

### 5.3 Les trois consommations (ce qui a été réellement sorti)

| Consommation | Définition |
|---|---|
| **Conso 1** | Tous les codes de mouvements imputés à l'OF |
| **Conso 2** | Seulement les mouvements « nominaux » : 261 et 262 (le spec demande de retirer la casse, mais la définition est encore à faire) |
| **Conso 3** | Conso 2 + régularisations d'inventaire réparties au prorata (**méthode non définie**, non calculée) |

### 5.4 Calcul de l'erreur et de la fiabilité

Pour un composant donné, on compare une prévision et une consommation :

```
erreur = |prévision − consommation| / max(prévision, consommation)
```

- si prévision > consommation → **surstock** (on a commandé trop) ;
- si prévision < consommation → **manquant** (on a manqué de matière) ;
- sinon → **ok**.

Pour un AF ou un centre de profit :

```
fiabilité = 100 % − moyenne des erreurs des composants
```

### 5.5 Exemple chiffré (inventé, pour comprendre)

Un AF est prévu à T0 avec 3 composants. Pendant la fenêtre, 2 OF ont été fabriqués.

| Composant | Prévision 2 | Conso 2 | Erreur | Statut |
|---|---|---|---|---|
| Vis V1 | 100 | 90 | 10 / 100 = 10 % | surstock |
| Tôle T3 | 50 | 65 | 15 / 65 ≈ 23 % | manquant |
| Colle C2 | 20 | 20 | 0 % | ok |

Moyenne des erreurs = (10 + 23 + 0) / 3 ≈ 11 %. **Fiabilité ≈ 89 %.**

Ce chiffre est une moyenne simple : il ne tient pas compte de la valeur des composants (question ouverte, §10).

---

## 6. Les données : d'où vient quoi

### 6.1 La chaîne officielle (ce qui existe déjà en production)

```
SAP
 │  ingestion par le job F_0_SAP_Ingestion_V2
 ▼
LANDING ZONE   historique complet, jamais écrasé
 │  CTAS chaque nuit (on ne garde que le dernier état)
 ▼
BRONZE         un seul état courant par table
 │  job W_3_SAP_AS_Design_BOM_DataAsset, task create_BOM_silver
 ▼
SILVER         prod_silver.production.BOM       3 230 287 lignes, 214 colonnes
 │  job W_3_SAP_AS_Design_BOM_DataAsset, task create_BOM_gold
 ▼
GOLD           prod_gold.production.BOM         216 colonnes
   ├── BOM_exposed  (vue, 22 colonnes)  → lue par les consommateurs
   └── bom_qc       (51 colonnes)       → contrôles qualité, job uc27
```

**Le point clé** : la BOM officielle est un **état courant**. Elle ne dit pas ce qu'était la BOM il y a six
mois. C'est pour cela que le projet a besoin de son propre historique.

### 6.2 Notre historique (en construction, en lab)

Nous reconstruisons l'historique **à partir de la landing zone** (qui, elle, garde tout), avec **une
photo par mois** (le 1er de chaque mois) :

| Table | Contenu | Statut |
|---|---|---|
| `bom_item_history` | la BOM à chaque 1er du mois, par AF, usine, alternative et poste | lab, en cours de validation |
| `order_component_requirement_history` | les besoins OP et OF à chaque 1er du mois | lab, première version |
| `uc093_bom_reliability_component` / `_work_order` | la fiabilité calculée | lab |

Pourquoi des photos **mensuelles** : la question métier porte sur des périodes de plusieurs mois. Une
photo par semaine ou par jour multiplierait le volume sans changer la réponse.

### 6.3 Comment on relit la BOM SAP

Une BOM est reconstituée en assemblant quatre tables (MAST → STKO → STAS → STPO), avec des règles de
validité : pour chaque poste, on garde la **dernière version** valable à la date choisie et on retire les
postes fermés.

---

## 7. Ce qui a déjà été fait et ce que ça donne

| Étape | Résultat |
|---|---|
| Lineage de la BOM officielle compris et documenté | `Spec Output/lineage_bom_production.md` |
| Historique de la BOM reconstruit en lab | 42 photos mensuelles, de mai 2023 à octobre 2026 (run du 2026-10-07) |
| Calcul de fiabilité en lab | Période par défaut (T0 janvier 2026, P1 = 6, P2 = 3), usine 2400 : **fiabilité 88,6 %** |
| Répartition des statuts (même période) | ok 6 389 · manquant 2 844 · surstock 1 989 |

**Attention** : un run de test partiel a écrasé la table de lab (sa date exacte n'est pas tracée, il a été
constaté le 2026-10-13). Il faut relancer le run complet avant de présenter un chiffre. Le correctif qui
empêche ce problème est en place.

---

## 8. Les divergences entre notre historique et la BOM officielle

La BOM officielle et notre historique lisent les mêmes tables SAP mais **n'appliquent pas les mêmes règles**.

| Sujet | BOM officielle (Silver) | Notre historique |
|---|---|---|
| Statut BOM `02` | exclu | exclu (aligné depuis le 2026-10-13) |
| Statut BOM `12` | gardé (97 238 lignes, sens inconnu) | gardé |
| Type `cslty = X` | exclu | gardé (**non aligné**) |
| Lignes sans usine (691 lignes) | gardées | exclues (**non aligné**) |
| Dernière version d'une allocation (STAS) | toutes les allocations non fermées | seulement la dernière version par poste |
| Quantité | pondérée par la probabilité, divisée par la quantité de base | brute, avec la quantité de base à part |

Conséquence : on ne peut pas encore dire si nos chiffres sont justes par rapport à la BOM officielle. La
réconciliation (comparer les deux à la même date) reste à faire.

---

## 9. Les décisions déjà prises

| Date | Décision | Pourquoi |
|---|---|---|
| 2026-10-06 | Photos mensuelles au lieu d'un historique jour par jour | Le besoin porte sur des dates de début de mois |
| 2026-10-06 | Historique reconstruit depuis la landing zone | C'est la seule couche qui garde tout l'historique |
| 2026-10-09 | Prévision 2 = besoin sans rebut composant | Le rebut d'ensemble des achetés n'est pas appliqué aux OF |
| 2026-10-12 | Période par défaut : 6 mois puis 3 mois | Demande de la spec v2 |
| 2026-10-13 | Historique dans une table séparée, pas dans la BOM officielle | Ne pas casser les consommateurs existants (RFQ, Supply Chain, DR) |
| 2026-10-13 | Statut `02` exclu comme dans la Silver, `12` gardé | Alignement sur la Silver, sens de `12` à confirmer |

---

## 10. Ce qui n'est pas encore décidé

Les questions complètes sont dans `working/questions_metier_et_architecture.md`. Les plus importantes :

1. **Quelle BOM est « la BOM » à mesurer ?** La BOM SAP, ou la BOM moyenne (`current_average_bom`) qui sert au dashboard « Average BOM » ?
2. **Qu'est-ce qu'un OF « pertinent » ?** Date de début réelle ou planifiée ? Date de fin réelle, TECO ou clôture ?
3. **Quelle consommation est « nominale » (Conso 2) ?** Quels codes de mouvement, et comment isoler la casse ?
4. **La moyenne des erreurs est-elle pondérée par la valeur ?** Un composant cher doit-il compter plus ?
5. **Quelle fréquence de photos ?** Mensuelle (ce que nous faisons) ou hebdomadaire (demandée pour « la tendance d'une semaine à l'autre ») ?
6. **Quel environnement est « INT » ?** La spec demande INT et PROD ; notre méthode passe par dev, uat, prod.
7. **Statut `12` et lignes sans usine** : à exclure ou à garder ?
8. **Qui valide ?** Trois validateurs métier n'ont pas encore donné de date.

---

## 11. Les risques et les limites

- **Pas de validation métier** : aucun validateur n'a signé la spec.
- **Pas d'accès à la BOM moyenne** : on ne sait pas encore comment elle est calculée (méthode orale).
- **Données incomplètes** : la division (demandée dans la spec) n'est pas dans nos données.
- **Volume** : une photo mensuelle fait environ 3,7 millions de lignes. Une photo hebdomadaire multiplierait ce volume par 4.
- **Table de lab fragile** : elle a déjà été écrasée une fois. Le correctif est en place, mais il faut relancer le run complet.
- **Pas d'exécution depuis ce dépôt** : le code n'a été exécuté que sur Databricks, par les runs de lab décrits plus haut.

---

## 12. Où trouver quoi

| Document | Contenu |
|---|---|
| `Spec Output/briefing_debutant.md` | ce document |
| `Spec Output/spec_technique_historisation_bom.md` | spécification technique (v5), notre historique et ses règles |
| `Spec Output/lineage_bom_production.md` | comment la BOM officielle est construite aujourd'hui (Silver, Gold, colonnes) |
| `working/questions_metier_et_architecture.md` | toutes les questions à poser (métier, OP/OF, architecture) |
| `working/questions_ouvertes.md` | questions historiques (2026-10-09 et 2026-10-12) |
| `working/corrections_log.md` | journal de tous les tests, erreurs et corrections |
| `reference/` | copies des notebooks de production, de la configuration du job et de la spec v2 |
| `data_asset/…` et `proj/…` | le code de nos notebooks (historique A, besoins B, fiabilité C) |

---

## 13. Glossaire rapide (à garder sous la main)

| Terme | Une phrase |
|---|---|
| BOM | recette d'un produit : liste des composants avec leurs quantités |
| AF | produit fabriqué |
| Composant | pièce ou matière utilisée pour fabriquer un AF |
| Alternative | une façon possible de fabriquer un AF |
| OP | projet de fabrication planifié (pas encore lancé) |
| OF | fabrication lancée à l'atelier |
| Réservation | liste des composants qu'un OP ou un OF doit consommer |
| Mouvement 261 | sortie de stock pour un OF |
| Rebut | pourcentage de perte prévu en fabrication |
| Fantôme | sous-ensemble fabriqué sans stock : on consomme ce qu'il contient |
| ECN | modification officielle d'une BOM |
| T0 / T1 / T2 | date de prévision / début de la fenêtre / fin de la fenêtre |
| Prévision 1 / 2 / 3 | besoin brut / sans rebut composant / BOM standard |
| Conso 1 / 2 / 3 | tous les mouvements / mouvements nominaux / nominaux + régularisations |
| Erreur | écart relatif entre prévision et consommation pour un composant |
| Fiabilité | 100 % moins la moyenne des erreurs |
| Surstock / manquant / ok | prévision supérieure / inférieure / égale à la consommation |
| Snapshot | photo d'une donnée à une date fixée |
| Landing zone / Bronze / Silver / Gold / Proj | les couches de traitement des données, du brut au métier |
