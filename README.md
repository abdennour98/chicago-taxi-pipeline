# 🚕 Pipeline de données – Courses de taxi de Chicago

Un petit projet de data engineering, de bout en bout : on télécharge des données brutes,
on les nettoie, on calcule des indicateurs, et on les interroge en SQL.
Tout tourne en local avec une seule commande Docker.

---

## 1. L'idée en deux minutes

On suit l'architecture **médaillon** : les données passent par trois étapes, de plus en plus propres.

```
  API de Chicago
        │  Spark : téléchargement des pages en parallèle
        ▼
 ┌──────────────┐
 │   BRONZE     │  Les données brutes, telles quelles (fichiers Parquet dans MinIO)
 └──────┬───────┘
        │  Spark : typage, doublons, nettoyage
        ▼
 ┌──────────────┐
 │   SILVER     │  Les données propres (table Iceberg  silver.trips)
 └──────┬───────┘
        │  Spark : agrégations et jointures
        ▼
 ┌──────────────┐
 │    GOLD      │  Les indicateurs prêts pour l'analyse (tables Iceberg  gold.*)
 └──────┬───────┘
        │
        ▼
   Trino  →  on interroge tout ça en SQL (ou depuis un outil de BI)
```

**Airflow** lance les étapes dans l'ordre : `bronze → silver → gold → check_gold`.

## 2. Les outils, et à quoi ils servent

| Outil | Rôle | Adresse |
|-------|------|---------|
| **Airflow** | Chef d'orchestre : lance et surveille les étapes | http://localhost:8080 (admin / admin) |
| **Spark** | Moteur de calcul : nettoie et transforme les données | http://localhost:8081 |
| **MinIO** | Stockage de fichiers (comme Amazon S3, mais en local) | http://localhost:9001 (minioadmin / minioadmin) |
| **Apache Iceberg** | Format de tables pour silver et gold (des vraies tables SQL sur des fichiers) | – |
| **Postgres** | Base interne : sert à Airflow et au catalogue Iceberg | – |
| **Trino** | Moteur SQL pour lire les tables Iceberg | http://localhost:8083 |

## 3. Les données

- **Source** : [Taxi Trips 2013-2023 – ville de Chicago](https://data.cityofchicago.org/Transportation/Taxi-Trips-2013-2023-/wrvz-psew)
- Le fichier complet pèse plusieurs dizaines de Go : on n'en télécharge **qu'une tranche**.
- **Période choisie : du 1er janvier au 31 mars 2023** (1er trimestre 2023), filtrée sur `trip_start_timestamp`.

Le téléchargement passe par l'API SODA : on découpe les courses en pages de 50 000 lignes, et Spark
télécharge plusieurs pages en même temps (chaque page devient un fichier Parquet, rangé dans un dossier par mois).
Pour changer de période, modifie la liste `MOIS` en haut de `jobs/ingest_bronze.py`.

## 4. Ce qu'il faut avoir avant de commencer

- **Docker Desktop** installé et démarré
- Environ **8 Go de mémoire** libre pour Docker
- De la patience au premier lancement : Docker télécharge Java, Spark et plusieurs bibliothèques (compte quelques minutes, parfois plus)

## 5. Lancer le projet

Place-toi dans le dossier qui contient `docker-compose.yml`, puis :

```bash
docker compose up -d --build
```

Vérifie que tout est en route :

```bash
docker compose ps
```

C'est normal que le conteneur `airflow-init` soit « Exited (0) » : il prépare la base et les buckets, puis s'arrête.

## 6. Suivre le pipeline

Le DAG **`taxi_pipeline`** démarre tout seul. Ouvre Airflow (http://localhost:8080) et clique dessus :

- 🟩 vert clair = en cours, 🟢 vert foncé = terminé, 🟥 rouge = échec
- Clique sur une tâche puis **Logs** pour voir ce qu'elle raconte.

| Tâche | Ce qu'elle fait | Ce que tu dois voir dans les logs |
|-------|-----------------|-----------------------------------|
| `bronze` | Télécharge les données (en parallèle) et les dépose dans MinIO | « Mois 2023-01 : … courses à télécharger », puis « Bronze terminé » |
| `silver` | Nettoie et crée `silver.trips` | Le nombre de lignes bronze et silver |
| `gold` | Crée les trois tables d'indicateurs | Pas d'erreur |
| `check_gold` | Vérifie que les tables gold ne sont pas vides | Le nombre de lignes par table |

Pour relancer le pipeline : bouton **Trigger** dans Airflow, ou

```bash
docker compose exec airflow-scheduler airflow dags trigger taxi_pipeline
```

Pour lancer seulement le téléchargement (sans Airflow) :

```bash
docker compose exec airflow-scheduler spark-submit /opt/airflow/jobs/ingest_bronze.py
```

## 7. Interroger les données

### Avec Trino (du SQL simple)

```bash
docker compose exec trino trino
```

```sql
SHOW TABLES FROM iceberg.silver;
SELECT * FROM iceberg.silver.trips LIMIT 10;
SELECT * FROM iceberg.gold.daily_kpis ORDER BY pickup_date;
SELECT * FROM iceberg.gold.area_activity ORDER BY pickups DESC LIMIT 10;
```

Tape `quit` pour sortir. Tu peux aussi brancher un outil comme DBeaver, Superset ou Metabase sur Trino
(hôte `localhost`, port `8083`, catalogue `iceberg`, pas de mot de passe).

### Regarder les fichiers bruts

Dans MinIO (http://localhost:9001) : le bucket `bronze` contient les fichiers Parquet du téléchargement,
le bucket `warehouse` contient les fichiers des tables Iceberg (silver et gold).

## 8. Ce que fait chaque étape

**Bronze** – on garde les données brutes, sans rien modifier (toutes les colonnes en texte).
Un dossier par mois : `bronze/taxi_trips/month=2023-01/`. Si on relance, seuls les mois téléchargés sont remplacés.

**Silver** – on rend les données fiables :
- les colonnes reçoivent le bon type (dates, nombres…) ;
- les courses sans identifiant ou sans date sont supprimées ;
- une course = une ligne (suppression des doublons) ;
- les courses aberrantes sont écartées (durée nulle, distance ou montant négatif) ;
- les valeurs manquantes sont remplacées (`Unknown` pour l'entreprise et le moyen de paiement, 0 pour les pourboires).

**Gold** – on calcule des indicateurs utiles :

| Table | Contenu |
|-------|---------|
| `gold.daily_kpis` | Par jour : nombre de courses, chiffre d'affaires, panier moyen, distance moyenne |
| `gold.hourly_demand` | Nombre de courses par heure de la journée |
| `gold.area_activity` | Par quartier : départs et arrivées (deux calculs reliés par une jointure) |

## 9. Organisation des fichiers

```
docker-compose.yml          → toute l'infrastructure (à lancer avec docker compose)
dags/Taxi_Pipeline.py       → le DAG Airflow (l'ordre des étapes)
jobs/
  ingest_bronze.py          → étape 1 : téléchargement
  silver_trips.py           → étape 2 : nettoyage
  gold_kpis.py              → étape 3 : indicateurs
conf/spark-defaults.conf    → réglages de Spark (MinIO, Iceberg) : rien à configurer dans le code
trino/catalog/iceberg.properties → dit à Trino où sont les tables Iceberg
init-iceberg.sql            → crée la base Postgres du catalogue Iceberg
src/spark_session.py        → petit outil Python optionnel pour ouvrir Spark
```

## 10. Arrêter et nettoyer

```bash
docker compose down          # arrête tout, garde les données
docker compose down -v       # arrête tout ET efface toutes les données (à utiliser pour repartir de zéro)
```

## 11. Quand ça ne marche pas

| Problème | Piste |
|----------|-------|
| Une tâche est rouge dans Airflow | Ouvre la tâche → **Logs**, lis les dernières lignes |
| `Catalog 'iceberg' not found` dans Trino | Le fichier `trino/catalog/iceberg.properties` manque ou est mal placé, puis `docker compose up -d --force-recreate trino` |
| `Table … does not exist` dans Trino | L'étape `silver` ou `gold` n'a pas encore fini dans Airflow |
| Le DAG n'apparaît pas | `docker compose exec airflow-scheduler airflow dags list-import-errors` |
| La base `iceberg` n'existe pas dans Postgres | Le script `init-iceberg.sql` ne s'exécute que sur un volume vide : `docker compose down -v` puis relance |
| Les ports 8080 / 8081 / 9001 sont déjà pris | Ferme le programme qui les utilise, ou change le port dans `docker-compose.yml` |

---

## 12. Prochaine optimisation : ingestion bronze avec le partitionnement Spark

**Le problème.** Une ingestion écrite en Python simple télécharge les pages de l'API **une par une** :
c'est lent, car on attend la réponse de chaque page avant de demander la suivante, et un seul processeur travaille.

**L'idée.** Découper le travail en petits morceaux indépendants et laisser Spark les faire **en parallèle** :

```
 Avant (séquentiel)                      Après (partitionné avec Spark)

 page 1 → page 2 → page 3 → ...          page 1 ┐
 (un seul travailleur)                   page 2 ├─► plusieurs cœurs en même temps
                                         page 3 ┘   (1 page = 1 partition Spark)
```

**Comment ça marche.**
1. Le driver demande à l'API combien de courses il y a chaque mois, puis calcule la liste des pages (une page = 50 000 lignes).
2. Cette liste est distribuée à Spark (`parallelize`), avec **une partition par page**.
3. Chaque cœur télécharge « ses » pages en même temps que les autres.
4. Le résultat est écrit en Parquet avec `partitionBy("month")` : un dossier par mois (`month=2023-01/`), un fichier par page.
5. En relançant, seuls les mois concernés sont remplacés (`partitionOverwriteMode=dynamic`) : pas de doublons.



## 13. Migration vers le cloud : GCP (Google Cloud Platform)

### Pourquoi migrer ?

En local, tout dépend de ton ordinateur : sa mémoire, son disque, sa connexion.
Dans le cloud, le stockage est presque illimité, on peut lancer beaucoup plus de cœurs Spark quand il le faut
(et ne payer que pendant le calcul), et le pipeline peut tourner sans que ton ordinateur soit allumé.

### Correspondance entre le local et GCP

| Aujourd'hui (local) | Sur GCP | Pourquoi |
|---------------------|---------|----------|
| MinIO | **Cloud Storage** (GCS) | Même rôle : stocker les fichiers (`gs://...` à la place de `s3a://...`) |
| Spark (master + worker) | **Dataproc** (option *Serverless for Apache Spark*) | Spark géré : le cluster démarre pour le job, puis disparaît |
| Airflow (Docker) | **Cloud Composer** | Airflow géré par Google |
| Postgres (catalogue Iceberg) | **Cloud SQL for PostgreSQL** (ou le métastore BigLake) | Base gérée, sauvegardée |
| Trino | **BigQuery** (tables Iceberg via BigLake) | SQL rapide sur les tables, sans serveur à gérer |
| (rien) | **Looker Studio** | Tableaux de bord sur le gold |
| Mots de passe dans le compose | **Secret Manager** + **IAM** (comptes de service) | Plus aucun mot de passe dans le code |

### Nouvelle architecture

```
  API de Chicago
        │  Spark (Dataproc Serverless) : téléchargement des pages en parallèle
        ▼
 ┌──────────────────────────┐
 │  BRONZE                  │  Cloud Storage  gs://<projet>-bronze/taxi_trips/month=AAAA-MM/
 │  Parquet bruts           │
 └────────────┬─────────────┘
              │  Spark (Dataproc Serverless) : typage, doublons, nettoyage
              ▼
 ┌──────────────────────────┐
 │  SILVER                  │  Tables Iceberg  silver.trips
 │  données propres         │  fichiers dans Cloud Storage  gs://<projet>-warehouse/
 └────────────┬─────────────┘
              │  Spark (Dataproc Serverless) : agrégations et jointures
              ▼
 ┌──────────────────────────┐
 │  GOLD                    │  Tables Iceberg  gold.*
 │  indicateurs             │  (même bucket warehouse)
 └────────────┬─────────────┘
              │
              ▼
       BigQuery  ──►  Looker Studio (tableaux de bord)  /  requêtes SQL


 Autour du pipeline :
   Cloud Composer (Airflow)   → lance et surveille bronze → silver → gold → check
   Cloud SQL (Postgres)       → catalogue des tables Iceberg
   Secret Manager + IAM       → accès et mots de passe
   Cloud Logging / Monitoring → logs et alertes
   Terraform                  → tout l'environnement décrit en code
```
