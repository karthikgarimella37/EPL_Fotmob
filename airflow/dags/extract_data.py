import os
import json
import requests
from datetime import datetime
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
    load_dotenv(dotenv_path=file_path) # Ensure .env is loaded
    # The requests_ip_rotator library uses boto3, which will automatically
    # pick up credentials from environment variables like AWS_ACCESS_KEY_ID,
    # AWS_SECRET_ACCESS_KEY, and AWS_DEFAULT_REGION.
    # We just need to ensure they are loaded from the .env file if they are set there.
    # No need to return them explicitly if they are set as env vars.
    os.environ['AWS_ACCESS_KEY_ID'] = os.getenv("aws_access_key_id", "")
    os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv("aws_secret_access_key", "")
    os.environ['AWS_REGION_NAME'] = os.getenv("aws_default_region", "us-east-1") # requests-ip-rotator might use AWS_REGION_NAME
    os.environ['AWS_DEFAULT_REGION'] = os.getenv("aws_default_region", "us-east-1") # boto3 uses this

def gcp_credentials(file_path):
    """Retrieve GCP credentials from .env file"""
    load_env(file_path)
    return os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), os.getenv("GCS_BUCKET_NAME")

def postgres_credentials(file_path):
    '''
    Get the credentials for the postgres database
    '''
    load_env(file_path)
    sql_username = os.getenv("sql_username")
    sql_password = os.getenv("sql_password")
    sql_host = os.getenv("sql_host")
    sql_port = os.getenv("sql_port")
    sql_database = os.getenv("sql_database")

    return sql_username, sql_password, sql_host, sql_port, sql_database

def create_fotmob_session(aws_config_loaded=True):
    '''
    Creates and returns a requests session configured with an IP rotator for Fotmob.
    Ensure AWS credentials are loaded before calling this if aws_config_loaded is True.
    '''
    if not aws_config_loaded:
        print("Warning: AWS credentials might not be loaded for IP rotator.")

    # Use the base URL for the ApiGateway and session.mount
    fotmob_base_url = "https://www.fotmob.com"
    
    # Initialize the ApiGateway
    # You might need to provide your AWS access keys directly to ApiGateway
    # if environment variables are not picked up, or configure boto3 session.
    # Example: gateway = ApiGateway(fotmob_base_url, access_key_id=os.getenv("aws_access_key_id"), access_key_secret=os.getenv("aws_secret_access_key"))
    
    # Ensure EXTRA_REGIONS has valid regions if you use it
    # For simplicity, starting with default regions picked by ApiGateway
    try:
        gateway = ApiGateway(site=fotmob_base_url, regions=EXTRA_REGIONS) # Using EXTRA_REGIONS as in original code
        gateway.start(force=True) # force=True can help if gateway is stuck

        # Start session
        session = requests.Session()
        session.mount(fotmob_base_url, gateway)
        print("Fotmob IP rotator session created successfully.")
        return session
    except Exception as e:
        print(f"Error creating Fotmob IP rotator session: {e}")
        print("Falling back to a regular requests session.")
        # Fallback to a regular session if rotator fails to initialize
        return requests.Session()


def postgres_connection(file_path):
    '''
    Create a connection to the postgres database
    '''
    sql_username, sql_password, sql_host, sql_port, sql_database = postgres_credentials(file_path)
    connection_string = f'postgresql+psycopg2://{sql_username}:{sql_password}@{sql_host}:{sql_port}/{sql_database}'
    engine = create_engine(connection_string)

    return engine

def check_current_year_vs_(engine):
    '''
    Check if all matches exist in dim_match. If not, get the latest match inserted
    '''
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM team_dim"))
        count = result.scalar()
        if count == 0:
            print("No Matches")
            return None  # No matches exist, fetch all
        else:
            last_match = conn.execute(text("SELECT MAX(match_id) FROM team_dim")).scalar()
            return last_match
        

def fetch_all_season_matches(session):
    '''
    Fetch all EPL matches from the given range of seasons using the provided session
    '''
    all_matches = []
    
    # header has to be modularized or needs to be changed for each new run (once header key expires, I guess)
    headers = {
"X-Mas": "eyJib2R5Ijp7InVybCI6Ii9hcGkvZGF0YS9maXh0dXJlcz9pZD0xMTEmc2Vhc29uPTIwMjElMkYyMDIyIiwiY29kZSI6MTc1MzMzODEyMzExOSwiZm9vIjoicHJvZHVjdGlvbjplNTkwMTg4ZTVjZWZkMTkyN2Y1OTcxNzAwYzVlODE3NWRiNzI5Mjg1LXVuZGVmaW5lZCJ9LCJzaWduYXR1cmUiOiIzMjVFRjdCRUMwODRDMEQ5MUFENTMzNEIxQ0NFNjMzRSJ9"
                }
    # Read all league ids from the file
    league_ids = []
    with open('all_league_ids.txt', 'r') as file:
        for line in file:
            league_ids.append(line.strip())
    for league_id in league_ids:                
        league_url = f"https://www.fotmob.com/api/data/leagues?id={league_id}"
        league_response = session.get(league_url, headers=headers, timeout=30)
        json_response = league_response.json()
        if league_response.status_code == 200 and json_response is not None and \
        json_response['allAvailableSeasons']:
            print(f"League found for league {league_id}: {json_response['details']['name']}")
        else:
            print(f"League not found for league {league_id}")
            continue
        for season in json_response['allAvailableSeasons']:
            url = f"https://www.fotmob.com/api/fixtures?id={league_id}&season={season}"
            try:
                season_response = session.get(url, headers=headers, timeout=30) # Added timeout
                print(f"Request to {url} returned status code {season_response.status_code}")
                season_response.raise_for_status() # Raise an exception for bad status codes
                matches_data = season_response.json()
                # print("Raw response: ", response.text) # Large responses
                # print("Matches: ", matches_data)
                if isinstance(matches_data, list): # Ensure matches_data is a list
                    for match in matches_data:
                        if isinstance(match, dict) and 'id' in match: # Ensure match is a dict and has 'id'
                            all_matches.append(match['id'])
                        else:
                            print(f"Skipping invalid match data: {match}")
                else:
                    print(f"Unexpected data format for matches: {matches_data}")
            except requests.exceptions.RequestException as e:
                print(f"Error fetching season matches: {e}")
                continue # Continue to next year if an error occurs
        print(f"All Matches for {league_id} fetched; {len(all_matches)} matches found")
    print(f"fetch_all_season_matches function done, found {len(all_matches)} matches.")
    return all_matches

def fetch_match_details(session, match_id):
    '''
    Fetch details of a given match using the provided session
    '''
    headers = {
    "X-Mas": "eyJib2R5Ijp7InVybCI6Ii9hcGkvZGF0YS9maXh0dXJlcz9pZD0xMTEmc2Vhc29uPTIwMjElMkYyMDIyIiwiY29kZSI6MTc1MzMzODEyMzExOSwiZm9vIjoicHJvZHVjdGlvbjplNTkwMTg4ZTVjZWZkMTkyN2Y1OTcxNzAwYzVlODE3NWRiNzI5Mjg1LXVuZGVmaW5lZCJ9LCJzaWduYXR1cmUiOiIzMjVFRjdCRUMwODRDMEQ5MUFENTMzNEIxQ0NFNjMzRSJ9"
                    }
    url = f"https://www.fotmob.com/api/matchDetails?matchId={match_id}"
    try:
        response = session.get(url, headers=headers, timeout=30) # Added timeout
        print(f"Fetching details for match {match_id} with URL: {url}")
        print(f"Response status code: {response.status_code}")
        response.raise_for_status() # Raise an exception for bad status codes
        # print(f"Raw response for match {match_id}: {response.text}") # Be careful with printing large responses
        print(f"Successfully fetched match {match_id}")
        return response.json()
    except requests.exceptions.SSLError as e:
        print(f"SSL Error fetching match {match_id}: {e}. This might be a persistent issue with the IP or server.")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch match {match_id}, status code: {getattr(e.response, 'status_code', 'N/A')}, error: {e}")
        return None

def upload_to_gcs(bucket_name, data, destination_blob_name):
    '''
    Uploads JSON data to Google Cloud Storage directly
    '''
    storage_client = storage.Client()

    client = storage.Client()
    buckets = list(client.list_buckets())
    print("Buckets visible to the client:")
    for b in buckets:
        print("-", b.name)

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Upload JSON data as a string
    blob.upload_from_string(json.dumps(data), content_type="application/json")
    print(f"Uploaded {destination_blob_name} to {bucket_name}")
    


def main():
    '''
    Orchestrate ETL pipeline
    '''
    file_path = os.path.join(os.path.dirname(__file__), '../.env')

    # Load AWS Credentials for IP Rotator
    aws_credentials(file_path) # Call this early

    # Load GCP Credentials
    GOOGLE_APPLICATION_CREDENTIALS, GCS_BUCKET_NAME = gcp_credentials(file_path)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

    # Create Fotmob session with IP rotator
    fotmob_session = create_fotmob_session()

    # PostgreSQL Connection
    engine = postgres_connection(file_path)

    # Check existing data
    last_match_id = None #check_existing_matches(engine)

    # Fetch matches
    all_matches = fetch_all_season_matches(fotmob_session) if last_match_id is None else [last_match_id + 1]
    print("Done with fetching all_matches")

    for current_match_id in all_matches:
        print(f"Fetching {current_match_id} Details")
        match_data = fetch_match_details(fotmob_session, current_match_id)
        if match_data:
            # Upload JSON directly to GCP
            print("Uploading to GCS bucket")
            upload_to_gcs(GCS_BUCKET_NAME, match_data, f"match_{current_match_id}.json")

if __name__ == "__main__":
    main()