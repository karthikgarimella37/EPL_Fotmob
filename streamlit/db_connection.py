import os
import streamlit as st
from dotenv import load_dotenv
from urllib.parse import quote
from sqlalchemy import create_engine

# Load environment variables from .env file for local development
DEFAULT_ENV_PATH = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=DEFAULT_ENV_PATH)

def get_db_credentials():
    """
    Retrieves database credentials.
    - On Streamlit Cloud, it uses st.secrets.
    - Locally, it uses environment variables from a .env file.
    """
    # Deployed on Streamlit Cloud
    if hasattr(st, 'secrets') and "connections" in st.secrets and "postgresql" in st.secrets["connections"]:
        return st.secrets["connections"]["postgresql"]
    
    # Local development
    return {
        "username": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
        "database": os.getenv("POSTGRES_DB"),
    }

def postgres_connection():
    """Establishes a connection to the PostgreSQL database."""
    creds = get_db_credentials()

    if not all(creds.values()):
        st.error("One or more database environment variables are missing.")
        st.info("For local development, check your .env file. For deployment, check your Streamlit Cloud secrets.")
        st.stop()
        
    connection_string = (
        f'postgresql+psycopg2://{quote(creds["username"])}:{quote(creds["password"])}@'
        f'{creds["host"]}:{creds["port"]}/{creds["database"]}'
    )
    
    try:
        engine = create_engine(connection_string, future=True, pool_pre_ping=True)
        # Test the connection
        with engine.connect():
            pass
        return engine
    except Exception as e:
        st.error(f"Failed to connect to the database: {e}")
        st.stop() 