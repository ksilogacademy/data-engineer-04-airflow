# Projet 4/7 — Pipeline avec Airflow (orchestration)

Pipeline **orchestré avec Apache Airflow** sur les données **NYC TLC Yellow Taxi** : extraction API, conversion Parquet, dépôt sur **AWS S3**, chargement dans **Snowflake**, transformation **dbt**, le tout enchaîné automatiquement, planifié, rejouable et surveillé.

Ce dépôt fait partie d'une **série pédagogique de 7 projets Data Engineering**. Chaque projet commence là où le précédent s'arrête.

## La série

| #   | Projet            | Statut     |
| --- | ----------------- | ---------- |
| 1/7 | Pipeline API      | ✅ [Voir le dépôt](https://github.com/ksilogacademy/data-engineer-01-pipeline-api) |
| 2/7 | Data Warehouse    | ✅ [Voir le dépôt](https://github.com/ksilogacademy/data-engineer-02-data-warehouse) |
| 3/7 | Pipeline Cloud    | ✅ [Voir le dépôt](https://github.com/ksilogacademy/data-engineer-03-data-akehouse) |
| 4/7 | **Airflow**       | 👉 Tu es ici |
| 5/7 | Big Data Spark    | À venir    |
| 6/7 | Streaming         | À venir    |
| 7/7 | End-to-End        | À venir    |

## Tu n'as pas fait les Projets 1, 2 et 3 ?

Ce projet **n'ajoute pas de nouvelle source de données** : il orchestre ce que tu as déjà construit. Il s'appuie donc sur **trois livrables précédents** :

- le **pipeline du Projet 1** (extraction API, nettoyage, rapport qualité) : [Projet 1/7 — Pipeline API](https://github.com/ksilogacademy/data-engineer-01-pipeline-api) ;
- le **Data Warehouse du Projet 2** (Snowflake + dbt) : [Projet 2/7 — Data Warehouse](https://github.com/ksilogacademy/data-engineer-02-data-warehouse) ;
- l'**architecture cloud du Projet 3** (conversion Parquet, bucket S3, Storage Integration, chargement Bronze, dbt incrémental) : [Projet 3/7 — Pipeline Cloud](https://github.com/ksilogacademy/data-engineer-03-data-akehouse).

Commence par là si ce n'est pas déjà fait.

## Les consignes

Tout ce qu'il faut savoir pour réaliser le projet (objectif, prérequis, sources, directives, livrables, barème, date limite et soumission) se trouve dans [`docs/consignes.md`](docs/consignes.md).

**Date limite : samedi 28 novembre 2026, 23h59 (heure de Dakar).**

## Licence

[MIT](LICENSE)
