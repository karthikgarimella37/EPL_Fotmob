import os
from dotenv import load_dotenv
from google.cloud import storage
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime, timedelta

from create_sql import postgres_connection, create_all_tables_sequences
from extract_data import main as extract_data_main
from load_to_staging import main as load_to_staging_main
from truncate_stg import truncate_stg


default_args = {
    'owner': 'airflow',
    'retries': 0,
}

dag = DAG(
    dag_id = 'epl_data_pipeline',
    default_args = default_args,
    description = 'EPL Data Pipeline',
    schedule = None,
    start_date = datetime(2025, 1, 1)
)

def create_tables():
    file_path = os.path.join(os.path.dirname(__file__), '../.env')
    engine = postgres_connection(file_path)
    create_all_tables_sequences(engine)

def upload_to_gcs():
    env_path = os.path.join(os.path.dirname(__file__), '../.env')
    load_dotenv(env_path)
    storage_client = storage.Client()
    bucket = storage_client.bucket('terraform-fotmob-terra-bucket-kg')
    blob = bucket.blob('load_to_staging.py')
    blob.upload_from_filename(f"/opt/airflow/dags/load_to_staging.py")
    blob = bucket.blob('deps.zip')
    blob.upload_from_filename(f"/tmp/deps.zip")
    blob = bucket.blob('.env')
    blob.upload_from_filename(env_path)
    blob = bucket.blob('postgresql-42.6.0.jar')
    blob.upload_from_filename(f"/opt/airflow/dags/postgresql-42.6.0.jar")
    print("PySpark Script, deps.zip, .env, and PostgreSQL JDBC driver JAR uploaded to GCS")

    # print("PySpark Script uploaded to GCS")


create_sql_task = PythonOperator(
    task_id = 'create_sql',
    python_callable = create_tables,
    dag = dag
)

extract_data_task = PythonOperator(
    task_id = 'extract_data',
    python_callable = extract_data_main,
    dag = dag
)

truncate_stg_task = PythonOperator(
    task_id = 'truncate_stg',
    python_callable = truncate_stg,
    dag = dag
)

# Task to upload files to GCS
upload_gcs_task = PythonOperator(
    task_id='upload_dependencies_to_gcs',
    python_callable=upload_to_gcs,
    dag=dag,
)

pyspark_job = {
        "reference": {"project_id": "alert-rush-458419-c2"},
        "placement": {"cluster_name": "etl-cluster"},
        "pyspark_job": {
            "main_python_file_uri": "gs://terraform-fotmob-terra-bucket-kg/load_to_staging.py",
            "python_file_uris": ["gs://terraform-fotmob-terra-bucket-kg/deps.zip"],
            "file_uris": ["gs://terraform-fotmob-terra-bucket-kg/.env"],
            "jar_file_uris": ["gs://terraform-fotmob-terra-bucket-kg/postgresql-42.6.0.jar"],
            "properties": {
                "spark.dataproc.pip.packages": "numpy==1.26.4 pyspark==3.5.1 pandas==2.2.3"
            }
        }
    }
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/opt/airflow/secrets/gcp_credentials.json"
load_to_staging_task = DataprocSubmitJobOperator(
        task_id="load_to_staging",
        job=pyspark_job,
        region="us-central1",
        project_id="alert-rush-458419-c2",
        gcp_conn_id="gcp_credentials",
        dag=dag

    )

# create_sql_task >> extract_data_task >> upload_gcs_task
truncate_stg_task >> load_to_staging_task
# upload_gcs_task >> load_to_staging_task
