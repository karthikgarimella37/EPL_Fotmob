from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit
import argparse
import logging
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_arguments():
    parser = argparse.ArgumentParser(description="Load Teams Json data from GCS to Postgres")

    return parser.parse_args()

def create_spark_session():
    return (SparkSession.builder
            .appName("")
            .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.6.0.jar")
            .getOrCreate())