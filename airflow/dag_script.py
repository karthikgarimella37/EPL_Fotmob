import os
import logging
import subprocess
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base_hook import BaseHook
from dotenv import load_dotenv
from google.cloud import storage


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define the function that will be used in the Airflow task
def load_env(file_path):
    load_dotenv(dotenv_path=file_path)

def create_spark_session():
    # from pyspark.sql import SparkSession
    return (SparkSession.builder
            .appName("Spark-Postgres-Data-Pipeline") \
            .config("spark.jars", "/opt/spark/jars/gcs-connector-hadoop3-latest.jar,/opt/spark/jars/postgresql-42.6.0.jar") \
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", os.getenv("GOOGLE_APPLICATION_CREDENTIALS")) \
            .getOrCreate())

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 9),  # Adjust as necessary
}

def run_spark_job(**kwargs):
    # Retrieve variables from Airflow
    gcs_path = kwargs['dag_run'].conf.get('gcs_path', 'gs://terraform-fotmob-terra-bucket-kg/')
    
    # Load environment variables
    env_file_path = kwargs['dag_run'].conf.get('env_file', '/../../.env')
    load_env(env_file_path)
    # Assuming GOOGLE_APPLICATION_CREDENTIALS is set
    storage_client = storage.Client()

    
    # Create Spark session
    spark = create_spark_session()

    try:
        # Set up credentials (GCP, PostgreSQL, etc.)
        gcp_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        gcs_path = os.getenv('GCS_PATH', 'gs://terraform-fotmob-terra-bucket-kg/')
        logger.info(f"Looking for team data in {gcs_path}...")

        # Read the raw JSON data from GCS
        raw_df = spark.read.option("multiline", "true").json(f"{gcs_path}*.json")
        
        # Print the first 100 rows of the dataframe for inspection
        print(raw_df.head(100))

        # Optionally, you could write this data to Postgres, GCS, or any other target
        # Example: raw_df.write.jdbc(url=jdbc_url, table="your_table", properties=postgres_args)
        
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
    submit_job = BashOperator(
        task_id="copy_pyspark_job_to_gcs",
        bash_command="\
            gsutil cp /opt/airflow/dags/load_to_staging.py gs://terraform-fotmob-terra-bucket-kg/"
    )
    # Create the Airflow task using PythonOperator
    run_spark_task = BashOperator(
        task_id="run_spark_job",
        bash_command="""
            gcloud dataproc jobs submit pyspark gs://terraform-fotmob-terra-bucket-kg/load_to_staging.py \
            --cluster="etl-cluster" \
            --region="us-central1" \
            --project="rare-habitat-447201-d6"
            """
    )

    # Set the task sequence (if more tasks are defined)
    submit_job >> run_spark_task