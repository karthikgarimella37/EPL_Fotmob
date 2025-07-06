import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from create_sql import postgres_credentials, postgres_connection

def truncate_stg(engine):
    with engine.connect() as connection:
        with connection.begin():
            connection.execute(text('''\
                TRUNCATE TABLE team_dim_stg;
                TRUNCATE TABLE player_dim_stg;
                TRUNCATE TABLE match_dim_stg;
                TRUNCATE TABLE league_dim_stg;
                TRUNCATE TABLE match_lineup_fact_stg;
                TRUNCATE TABLE player_shotmap_fact_stg;
                TRUNCATE TABLE player_stats_fact_stg;
                '''))
    print("Truncated all tables")

if __name__ == "__main__":
    file_path = os.path.join(os.path.dirname(__file__), '../.env')
    engine = postgres_connection(file_path)
    truncate_stg(engine)




