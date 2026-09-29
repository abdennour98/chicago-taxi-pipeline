# ÉTAPE 2 - SILVER : lire le brut, mettre les bons types, enlever les lignes mauvaises
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("silver_trips").getOrCreate()
spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")

# 1. Lire les fichiers Parquet du bronze (tout est en texte pour l'instant)
brut = spark.read.parquet("s3a://bronze/taxi_trips/")
brut.createOrReplaceTempView("brut")   # permet d'utiliser du SQL sur ce DataFrame

# 2. Typer les colonnes et filtrer avec du SQL
propre = spark.sql("""
    SELECT
        trip_id,
        COALESCE(company, 'Unknown')                AS company,
        COALESCE(payment_type, 'Unknown')           AS payment_type,
        CAST(trip_start_timestamp AS TIMESTAMP)     AS trip_start,
        CAST(trip_start_timestamp AS DATE)          AS pickup_date,
        CAST(trip_seconds AS INT)                   AS trip_seconds,
        CAST(trip_miles AS DOUBLE)                  AS trip_miles,
        CAST(pickup_community_area AS INT)          AS pickup_community_area,
        CAST(dropoff_community_area AS INT)         AS dropoff_community_area,
        CAST(fare AS DOUBLE)                        AS fare,
        COALESCE(CAST(tips AS DOUBLE), 0)           AS tips,
        CAST(trip_total AS DOUBLE)                  AS trip_total
    FROM brut
    WHERE trip_id IS NOT NULL
      AND trip_start_timestamp IS NOT NULL
      AND CAST(trip_seconds AS INT) > 0
      AND CAST(trip_miles AS DOUBLE) >= 0
      AND CAST(trip_total AS DOUBLE) >= 0
""")

# 3. Enlever les doublons (même trip_id)
propre = propre.dropDuplicates(["trip_id"])

# 4. Écrire la table silver (Iceberg)
propre.writeTo("silver.trips").createOrReplace()

# 5. Petit contrôle
print("Lignes dans bronze :", brut.count())
print("Lignes dans silver :", spark.table("silver.trips").count())