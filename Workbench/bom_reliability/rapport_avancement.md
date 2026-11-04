# Rapport d'avancement — fiabilité des BOM (UC93), 2026-10-26

## 1. En résumé

La chaîne complète A → B → C est écrite et a tourné en lab (dev_lab.lab_jules). Le premier résultat existe : sur l'usine
2400 (branche IS), la fiabilité Prévision 2 / Conso 2 de la période par défaut est de **88,6 %** (11 222 composants).
Il reste à confirmer plusieurs règles métier, à refaire un run complet avec les derniers correctifs, puis à préparer
la mise en production (job, DAS, Test Definition, PowerBI).

## 2. Ce qui est fait

| Élément | État | Preuve |
|---|---|---|
| A — historique des BOM | ✅ Tourné en complet | 141,7 M lignes, 42 snapshots (2023-05 → 2026-10), clé unique, rapprochement STPO expliqué |
| B — historique des besoins OP / OF | ✅ Tourné (debug usine 2400) | Familles de fichiers RESB ACT / NOACT gérées ; `stalt` de PLAF corrigé |
| C — fiabilité | ✅ Tourné (debug usine 2400, serverless) | 326 888 lignes composant, 303 118 lignes OF, 0 doublon, 30 périodes |
| Mode incrémental A / B | ✅ Écrit | partition `snapshot_date`, `replace_where` ; non testé en lab |
| Tables de debug | ✅ | A, B, C écrivent `*_debug` en mode debug, jamais dans les tables complètes |
| Widgets | ✅ | Une seule cellule, valeurs conservées en relançant |
| Filtre de branche optionnel | ✅ Écrit (widget `branch`, vide par défaut = toutes) | Non testé en lab |
| « PF usage » | ✅ Écrit (`component_PF_usage_ratio`) | Non testé en lab |
| Description du centre de profit | ✅ Corrigé (zéros de tête) | Non testé en lab |
| Analyse des 8 notebooks existants | ✅ | Pas de travail en double ; l'historique de la prévision manquait partout |
| Modèle d'historisation | ✅ | `templates/notebook_gold_history_template.py` |

## 3. Premiers chiffres (lab, usine 2400, T0 de 2025-01 à 2026-04)

- Fiabilité des périodes : 85 % à 90 % ; période par défaut (T0 = 2026-01, P1 = 6, P2 = 3) : 88,6 %.
- Statuts de la période par défaut : OK 6 389, manquant 2 844, surstock 1 989.
- Totaux : Prévision 1 = 2,80 M, Prévision 2 = 2,59 M, consommation = 2,58 M (écart Prévision 2 / conso de 0,3 %).
- **44 % des OF de la période par défaut n'ont aucune prévision à T0** (ni OF ni OP six mois avant) : ils sont hors calcul.

## 4. Ce qui bloque ou reste à décider (métier)

| # | Sujet | Défaut actuel |
|---|---|---|
| M7 | Conso 2 « nominale » : comment identifier la casse ? Exclure les lignes de réservation 1 à 5 (impact < 1 %) ? | 261 / 262 |
| Conso 3 | Méthode de régularisation de stock par centre de profit (piste : `PF_pourcentage_affectation`) | non calculée |
| L10 | Ordres ZP09, ZP10, ZP11, YP09, YP10, YP11 (10,6 % des OF 2025) : rush ou retouche, à exclure ? | gardés |
| L9 | Vrac (4,5 % des lignes de réservation), composant `SPLIT`, statut usine `05` : exclure ? | gardés |
| L11 | « PF usage » vaut NULL quand le centre de profit de l'AF est un code chapeau (98x) : prendre celui de l'OF ? | centre de l'AF |
| L7 | Articles `49…` (outillage, achats hors BOM) : exclure ? | gardés |
| — | OF sans prévision à T0 (44 %) : les exclure ou chercher une autre source ? | exclus |
| S8 | Sens du statut BOM `12` | gardé |
| — | Division, classification (famille / classe / 0 ou L), tendance hebdomadaire | non livrés |

## 5. Prochaines étapes

1. Relancer A, B, C en debug (usine 2400, `branch` vide) pour valider les derniers correctifs, puis en complet.
2. Faire trancher les points du §4 par le métier ; appliquer les règles retenues dans C.
3. Rédiger le DAS et la Test Definition (exemples témoins), obligatoires avant Bitbucket.
4. Écrire le bundle du job (`platform/asset_bundle/`, mensuel : A et B en parallèle, puis C), en `rebuild_mode = incremental`.
5. Retirer les éléments propres au lab (`%pip`, `sys.path`, patch serverless), passer le Convention Checker, ouvrir la PR Bitbucket.
6. Écrire le guide PowerBI (pages résumé, analyse, détail par poste).

## 6. Ce qui n'a pas pu être vérifié

Aucun notebook ne peut être exécuté depuis ce dépôt. Les derniers changements de C (filtre de branche, « PF usage »,
description du centre de profit, lecture des tables `*_debug`), le mode incrémental et le modèle d'historisation
n'ont été vérifiés que par relecture et contrôle de syntaxe.
