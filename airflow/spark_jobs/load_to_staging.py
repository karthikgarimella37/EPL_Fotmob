from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit
import argparse
import logging
import os
import requests
from dotenv import load_dotenv
from google.cloud import storage
from sqlalchemy import create_engine, text
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_env(file_path):
    load_dotenv(dotenv_path=file_path)

def aws_credentials(file_path):
    '''
    AWS Credentials for Rotating IP addresses to not get blocked by the API
    '''
    # env_path = file_path# os.path.join(os.path.dirname(__file__), '../../.env')
    load_env(dotenv_path=file_path)
    aws_access_key_id = os.getenv("aws_access_key_id")
    aws_secret_access_key = os.getenv("aws_secret_access_key")
    aws_default_region = os.getenv("aws_default_region")

def gcp_credentials(file_path):
    """Retrieve GCP credentials from .env file"""
    load_env(file_path)
    return os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), os.getenv("GCS_BUCKET_NAME")

def postgres_credentials(file_path):
    '''
    
    '''
    load_env(file_path)
    sql_username = os.getenv("sql_username")
    sql_password = os.getenv("sql_password")
    sql_host = os.getenv("sql_host")
    sql_port = os.getenv("sql_port")
    sql_database = os.getenv("sql_database")

    return sql_username, sql_password, sql_host, sql_port, sql_database

def parse_arguments():
    parser = argparse.ArgumentParser(description="Load Teams Json data from GCS to Postgres")
    parser.add_argument('--gcs-path', required=True, help='GCS path with JSON files (gs://bucket/path/)')
    parser.add_argument('--supabase-host', required=True, help='Supabase Postgres host')
    parser.add_argument('--supabase-port', default='5432', help='Supabase Postgres port')
    parser.add_argument('--supabase-db', required=True, help='Supabase database name')
    parser.add_argument('--supabase-user', required=True, help='Supabase database user')
    parser.add_argument('--supabase-password', required=True, help='Supabase database password')
    parser.add_argument('--dim-table', default='dim_stg', help='Dimension staging table name')
    parser.add_argument('--processing-date', default=datetime.now().strftime('%Y-%m-%d'), 
                        help='Processing date in YYYY-MM-DD format')
    return parser.parse_args()

def create_spark_session():
    return (SparkSession.builder
            .appName("SparkGCPtoPostgres")
            .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.6.0.jar")
            .getOrCreate())


def main():
    args = parse_arguments()

    spark = create_spark_session()

    try:

        postgres_args = {
            "user": args.supabase_user,
            "password": args.supabase_password,
            "driver": "org.postgresql.Driver",
            "url": f"jdbc:postgresql://{args.supabase_host}:{args.supabase_port}/{args.supabase_db}",

        }

        logger.info(f"Looking for team data in {args.gcs_path}...")

        raw_df = spark.read.option("multiline", "true").json(f"{args.gcs_path}*.json")
        print(raw_df.head(100))
        raw_df.toPandas().to_csv('mycsv.csv')

    
    except Exception as e:
        logger.error(f"Loading To Stage Failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
