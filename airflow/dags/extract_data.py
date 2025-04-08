# extract data for EPL
# first check if all matches and players are available
# dump the json data into gcp bucket using terraform
# use the json to insert the data into stg tables
# insert into dim and facts after dbt transformations
# else get latest match inserted and extract from there
# functions for each action
# create a df for each table, normalize it to insert it efficiently


import os
import json
import requests
import psycopg2 as psy
from dotenv import load_dotenv
from google.cloud import storage
from sqlalchemy import create_engine, text
from requests_ip_rotator import ApiGateway, EXTRA_REGIONS

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

def fotmob_ip_rotator(matchID):
    '''
    
    '''
    # Initialize the ApiGateway
    gateway = ApiGateway(f'https://www.fotmob.com/api/matchDetails?matchId={matchID}', regions = EXTRA_REGIONS)
    gateway.start()

    # Start session
    session = requests.Session()
    session.mount(f'https://www.fotmob.com/api/matchDetails?matchId={matchID}', gateway)
    return session


def postgres_connection(file_path):
    sql_username, sql_password, sql_host, sql_port, sql_database = postgres_credentials(file_path)
    connection_string = f'postgresql+psycopg2://{sql_username}:{sql_password}@{sql_host}:{sql_port}/{sql_database}'
    engine = create_engine(connection_string)

    return engine

def check_existing_matches(engine):
    """Check if all matches exist in dim_match. If not, get the latest match inserted"""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM dim_match"))
        count = result.scalar()
        if count == 0:
            return None  # No matches exist, fetch all
        else:
            last_match = conn.execute(text("SELECT MAX(match_id) FROM dim_match")).scalar()
            return last_match
        

def fetch_all_season_matches(start_year=2010, end_year=2024):
    """Fetch all EPL matches from the given range of seasons"""
    all_matches = []
    for year in range(start_year, end_year):
        url = f"https://www.fotmob.com/api/fixtures?id=47&season={year}%2F{year+1}"
        response = requests.get(url)
        if response.status_code == 200:
            matches = response.json()
            for match in matches:
                all_matches.append(match['id'])
    return all_matches

def fetch_match_details(match_id):
    """Fetch details of a given match"""
    url = f"https://www.fotmob.com/api/matchDetails?matchId={match_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return None

def upload_to_gcs(bucket_name, data, destination_blob_name):
    """Uploads JSON data to Google Cloud Storage directly"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Upload JSON data as a string
    blob.upload_from_string(json.dumps(data), content_type="application/json")
    print(f"Uploaded {destination_blob_name} to {bucket_name}")


def main():
    """Orchestrate ETL pipeline"""
    file_path = os.path.join(os.path.dirname(__file__), '../../.env')

    # Load GCP Credentials
    GOOGLE_APPLICATION_CREDENTIALS, GCS_BUCKET_NAME = gcp_credentials(file_path)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

    # PostgreSQL Connection
    engine = postgres_connection(file_path)

    # Check existing data
    last_match_id = check_existing_matches(engine)

    # Fetch matches
    all_matches = fetch_all_season_matches() if last_match_id is None else [last_match_id + 1]

    for match_id in all_matches:
        match_data = fetch_match_details(match_id)
        if match_data:
            # Upload JSON directly to GCP
            upload_to_gcs(GCS_BUCKET_NAME, match_data, f"match_{match_id}.json")

if __name__ == "__main__":
    main()