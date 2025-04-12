import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
import logging
import subprocess
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.hooks.base_hook import BaseHook
from dotenv import load_dotenv
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define the function that will be used in the Airflow task
def load_env(file_path):
    load_dotenv(dotenv_path=file_path)
    print(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), 
    os.getenv("sql_username"), 
    os.getenv("sql_host"))

def create_spark_session():
    
    return (SparkSession.builder
            .appName("Spark-Postgres-Data-Pipeline") \
            .config("spark.jars", "/opt/spark/jars/gcs-connector-hadoop3-latest.jar,/opt/spark/jars/postgresql-42.6.0.jar") \
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", os.getenv("GOOGLE_APPLICATION_CREDENTIALS")) \
            .getOrCreate())

default_args = {
    'owner': 'airflow',
    'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 9),  # Adjust as necessary
}

def upload_to_gcs(**kwargs):
    # Retrieve variables from Airflow
    gcs_path = kwargs['dag_run'].conf.get('gcs_path', 'gs://terraform-fotmob-terra-bucket-kg/')
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/terraform_keys.json"

    
    # Load environment variables
    env_file_path = kwargs['dag_run'].conf.get('env_file', '/../../.env')
    load_env(env_file_path)
    storage_client = storage.Client()
    
    # Create Spark session
    spark = create_spark_session()

    try:
        # Set up credentials (GCP, PostgreSQL, etc.)
        gcp_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        bucket = storage_client.bucket("terraform-fotmob-terra-bucket-kg")
        blob = bucket.blob("load_to_staging.py")
        blob.upload_from_filename(f"/opt/airflow/dags/load_to_staging.py")
        gcs_path = "gs://terraform-fotmob-terra-bucket-kg/"
        blob = bucket.blob("deps.zip")
        blob.upload_from_filename(f"/tmp/deps.zip")

        print("File uploaded successfully")

        # Read the raw JSON data from GCS
       
        # raw_df.toPandas().to_csv('mycsv.csv')

    except Exception as e:
        logger.error(f"Error running Spark job: {str(e)}")
        raise
    finally:
        spark.stop()

with DAG(
    dag_id="spark_postgres_gcs_pipeline",
    default_args=default_args,
    schedule_interval="@once",  # Set your schedule or leave it for manual triggering
    catchup=False,
) as dag:
    
    # Copy pyspark to GCS for submitting Pyspark job
    submit_job = PythonOperator(
        task_id="copy_pyspark_job_to_gcs",
        python_callable = upload_to_gcs
    )

    pyspark_job = {
        "reference": {"project_id": "rare-habitat-447201-d6"},
        "placement": {"cluster_name": "etl-cluster"},
        "pyspark_job": {
            "main_python_file_uri": "gs://terraform-fotmob-terra-bucket-kg/load_to_staging.py",
            "python_file_uris": ["gs://terraform-fotmob-terra-bucket-kg/deps.zip"]
        }
    }
    # Create the Airflow task using PythonOperator
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/terraform_keys.json"
    create_dataproc = DataprocSubmitJobOperator(
        task_id="run_spark_job",
        job=pyspark_job,
        region="us-central1",
        project_id="rare-habitat-447201-d6",
        gcp_conn_id="google_cloud_default",

    )
    



    # Set the task sequence (if more tasks are defined)
    submit_job >> create_dataproc