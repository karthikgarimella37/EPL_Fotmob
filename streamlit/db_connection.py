import os
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Define the default path for the .env file relative to this script
DEFAULT_ENV_PATH = os.path.join(os.path.dirname(__file__), '.env')

def postgres_credentials(file_path=DEFAULT_ENV_PATH):
    """Loads database credentials from an environment file."""
    load_dotenv(file_path)
    sql_username = os.getenv("SQL_USERNAME")
    sql_password = os.getenv("SQL_PASSWORD")
    sql_host = os.getenv("SQL_HOST")
    sql_port = os.getenv("SQL_PORT")
    sql_database = os.getenv("SQL_DATABASE")
    
    return sql_username, sql_password, sql_host, sql_port, sql_database

def postgres_connection(file_path=DEFAULT_ENV_PATH):
    """Establishes a connection to the PostgreSQL database."""
    sql_username, sql_password, sql_host, sql_port, sql_database = postgres_credentials(file_path)

    if not all([sql_username, sql_password, sql_host, sql_port, sql_database]):
        st.error("One or more database environment variables are missing.")
        st.info(f"Check .env file in the 'streamlit' directory.")
        st.stop()
        
    connection_string = f'postgresql+psycopg2://{sql_username}:{sql_password}@{sql_host}:{sql_port}/{sql_database}'
    
    try:
        engine = create_engine(connection_string, future=True, pool_pre_ping=True)
        # Test the connection
        with engine.connect() as conn:
            pass
        return engine
    except Exception as e:
        st.error(f"Failed to connect to the database: {e}")
        st.stop() 