import runpy
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

def run_bronze():
    # Executes the script as if launched with `python /opt/airflow/jobs/ingest_bronze.py`
    runpy.run_path("/opt/airflow/jobs/ingest_bronze.py", run_name="__main__")


# Le DAG se lance une seule fois au démarrage (schedule="@once"), puis à la main avec "Trigger".
with DAG(
    dag_id="taxi_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False,
    is_paused_upon_creation=False,
):
    # Étape 1 : bronze = télécharger les données brutes
    bronze = PythonOperator(
        task_id="bronze",
        python_callable=run_bronze,
    )

      # Étape 2 : silver = nettoyer
    silver = SparkSubmitOperator(
        task_id="silver",
        application="/opt/airflow/jobs/silver_trips.py",
        conn_id="spark_default",
    )

    # Etape 3: gold: aggregats
    gold = SparkSubmitOperator(
        task_id="gold",
        application="/opt/airflow/jobs/gold_kpis.py",
        conn_id="spark_default",
    )

    bronze >> silver >> gold