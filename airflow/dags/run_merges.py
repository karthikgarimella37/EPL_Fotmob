import os
from sqlalchemy import create_engine, text
from create_sql import postgres_connection
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def execute_sql_from_file(engine, file_path):
    """
    Reads a SQL file and executes its content against the database.
    """
    try:
        with open(file_path, 'r') as f:
            sql_query = f.read()
        
        with engine.connect() as connection:
            with connection.begin():
                connection.execute(text(sql_query))
        
        logger.info(f"Successfully executed merge statement from: {file_path}")
    except FileNotFoundError:
        logger.error(f"SQL file not found at: {file_path}")
    except Exception as e:
        logger.error(f"An error occurred while executing {file_path}: {e}")
        raise

def main():
    """
    Main function to connect to the database and run all merge operations.
    """
    # Base directory where the script and SQL files are located
    dags_dir = os.path.dirname(__file__)
    
    # Path to the .env file
    env_file_path = os.path.join(dags_dir, '../.env')

    # List of merge SQL scripts to be executed in order
    merge_scripts = [
        'merge_team_dim.sql',
        'merge_player_dim.sql',
        'merge_league_dim.sql',
        'merge_match_dim.sql',
        'merge_match_lineup_fact.sql',
        'merge_player_shotmap_fact.sql',
        'merge_player_stats_fact.sql'
    ]

    try:
        # Establish database connection
        engine = postgres_connection(env_file_path)
        logger.info("Successfully connected to the database.")

        # Execute each merge script
        for script_name in merge_scripts:
            sql_file_path = os.path.join(dags_dir, script_name)
            execute_sql_from_file(engine, sql_file_path)
            
        logger.info("All merge operations completed successfully.")

    except Exception as e:
        logger.error(f"A critical error occurred in the main process: {e}")

if __name__ == "__main__":
    main() 