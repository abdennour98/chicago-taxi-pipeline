# ÉTAPE 3 - GOLD : calculer des indicateurs à partir de silver
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("gold_kpis").getOrCreate()
spark.sql("CREATE NAMESPACE IF NOT EXISTS gold")

# Table 1 : indicateurs par jour
par_jour = spark.sql("""
    SELECT
        pickup_date,
        COUNT(*)                     AS trips,
        ROUND(SUM(trip_total), 2)    AS revenue,
        ROUND(AVG(trip_total), 2)    AS avg_total,
        ROUND(AVG(trip_miles), 2)    AS avg_miles
    FROM silver.trips
    GROUP BY pickup_date
""")
par_jour.writeTo("gold.daily_kpis").createOrReplace()

# Table 2 : nombre de courses par heure de la journée
par_heure = spark.sql("""
    SELECT
        HOUR(trip_start)  AS hour,
        COUNT(*)          AS trips
    FROM silver.trips
    GROUP BY HOUR(trip_start)
""")
par_heure.writeTo("gold.hourly_demand").createOrReplace()

# Table 3 : départs et arrivées par quartier (on relie les deux avec un JOIN)
par_quartier = spark.sql("""
    WITH departs AS (
        SELECT pickup_community_area AS area, COUNT(*) AS pickups
        FROM silver.trips
        WHERE pickup_community_area IS NOT NULL
        GROUP BY pickup_community_area
    ),
    arrivees AS (
        SELECT dropoff_community_area AS area, COUNT(*) AS dropoffs
        FROM silver.trips
        WHERE dropoff_community_area IS NOT NULL
        GROUP BY dropoff_community_area
    )
    SELECT
        COALESCE(departs.area, arrivees.area)  AS area,
        COALESCE(departs.pickups, 0)           AS pickups,
        COALESCE(arrivees.dropoffs, 0)         AS dropoffs
    FROM departs
    FULL OUTER JOIN arrivees ON departs.area = arrivees.area
""")
par_quartier.writeTo("gold.area_activity").createOrReplace()