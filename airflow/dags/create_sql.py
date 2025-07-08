# sql connections
# create db if not exists
#create tables if not exists
# try to incorporate terraform
# #create sequences for dim and fact tables

import os
# import json
# import requests
import psycopg2 as psy
from dotenv import load_dotenv
# from google.cloud import storage
from sqlalchemy import create_engine, text

# SQL DB Keys
def postgres_credentials(file_path):
    '''
    Loads the environment variables from the .env file
    '''
    load_env(file_path)
    sql_username = os.getenv("sql_username")
    sql_password = os.getenv("sql_password")
    sql_host = os.getenv("sql_host")
    sql_port = os.getenv("sql_port")
    sql_database = os.getenv("sql_database")

    return sql_username, sql_password, sql_host, sql_port, sql_database


def postgres_connection(file_path):
    '''
    Creates a connection to the postgres database
    '''
    sql_username, sql_password, sql_host, sql_port, sql_database = postgres_credentials(file_path)
    connection_string = f'postgresql+psycopg2://{sql_username}:{sql_password}@{sql_host}:{sql_port}/{sql_database}'
    engine = create_engine(connection_string)

    return engine

def check_existing_matches(engine):
    """
    Check if all matches exist in dim_match. If not, get the latest match inserted
    """
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM dim_match"))
        count = result.scalar()
        if count == 0:
            return None  # No matches exist, fetch all
        else:
            last_match = conn.execute(text("SELECT MAX(match_id) FROM dim_match")).scalar()
            return last_match

def load_env(file_path):
    load_dotenv(dotenv_path=file_path)

def create_all_tables_sequences(engine):
    '''
    Creates all tables and sequences
    '''
    with engine.connect() as connection:
        with connection.begin():
            connection.execute(text("""\
                          

-- Sequences for Dim tables
CREATE SEQUENCE IF NOT EXISTS seq_dim_team_id START 1 INCREMENT BY 1 NO maxvalue NO cycle;
CREATE SEQUENCE IF NOT EXISTS seq_dim_player_id START 1 INCREMENT BY 1 NO maxvalue NO cycle;
CREATE SEQUENCE IF NOT EXISTS seq_dim_match_id START 1 INCREMENT BY 1 NO maxvalue NO cycle;
CREATE SEQUENCE IF NOT EXISTS seq_dim_date_id START 1 INCREMENT BY 1 NO maxvalue NO cycle;
CREATE SEQUENCE IF NOT EXISTS seq_dim_league_id START 1 INCREMENT BY 1 NO maxvalue NO cycle;

CREATE SEQUENCE IF NOT EXISTS seq_fact_match_lineup_id START 1 INCREMENT BY 1 NO maxvalue NO cycle;
CREATE SEQUENCE IF NOT EXISTS seq_fact_player_shotmap_id START 1 INCREMENT BY 1 NO maxvalue NO cycle;
CREATE SEQUENCE IF NOT EXISTS seq_fact_player_stats_id START 1 INCREMENT BY 1 NO maxvalue NO cycle;                     

-- DIM and FACT STG Tables
CREATE TABLE IF NOT EXISTS team_dim_stg (                          
    TeamID INT PRIMARY KEY,
    TeamName VARCHAR(255),
    ImageUrl VARCHAR(255),
    PageUrl VARCHAR(255),
    TeamColor VARCHAR(255),
    LeagueID INT
);

CREATE TABLE IF NOT EXISTS player_dim_stg (                        
    PlayerID BIGINT PRIMARY KEY,
    PlayerName VARCHAR(255),
    FirstName VARCHAR(100), 
    LastName VARCHAR(100),  
    Age INT,
    CountryName VARCHAR(255),
    CountryCode VARCHAR(10),        
    ProfileUrl VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS match_dim_stg (                         
    MatchID BIGINT PRIMARY KEY, 
    MatchName VARCHAR(255),     
    MatchTimeUTC TIMESTAMP,     
    MatchRound VARCHAR(100),    
    LeagueID INT,               
    HomeTeamID INT,             
    AwayTeamID INT,             
    SeasonName VARCHAR(100),
    StadiumName VARCHAR(255),   
    Attendance INT,             
    RefereeName VARCHAR(255),
    MatchLatitude FLOAT,
    MatchLongitude FLOAT,
    MatchHighlightsUrl VARCHAR(255),
    MatchQAQuestion TEXT,
    MatchQAAnswer TEXT,
    MatchCountryCode VARCHAR(10),
    PlayerOfTheMatchID BIGINT,
    Momentum TEXT
);

-- -- not needed
-- CREATE TABLE IF NOT EXISTS date_dim_stg (                          
--     DateID INT PRIMARY KEY,
--     FullDate DATE UNIQUE,
--     Year INT,
--     Month INT,
--     Day INT,
--     DayName VARCHAR(20),
--     Weekday VARCHAR(20),
--     Quarter INT,
--     MonthName VARCHAR(20),
--     LeapYear BOOLEAN,
--     DayOfYear INT,
--     DayOfWeek INT,
--     Week INT
-- );

CREATE TABLE IF NOT EXISTS league_dim_stg (                        
    LeagueID INT PRIMARY KEY,       
    LeagueName VARCHAR(255),        
    ParentLeagueID INT,             
    ParentLeagueName VARCHAR(255),  
    CountryCode VARCHAR(10)
);
                                                                      

CREATE TABLE IF NOT EXISTS match_lineup_fact_stg (
    MatchID BIGINT,
    TeamID INT,
    PlayerID BIGINT,
    IsStarter BOOLEAN,
    PositionID INT,
    ShirtNumber INT,
    PlayerRating FLOAT,
    PlayerOfTheMatch VARCHAR(255),
    IsCaptain BOOLEAN,
    PRIMARY KEY (MatchID, TeamID, PlayerID)
);

CREATE TABLE IF NOT EXISTS player_shotmap_fact_stg (
    MatchID BIGINT,
    ShotMapID BIGINT,
    EventType VARCHAR(255),
    PlayerID BIGINT,
    xPosition FLOAT,
    yPosition FLOAT,
    Minute INT,
    IsBlocked BOOLEAN,
    IsOnTarget BOOLEAN,
    BlockedXPosition FLOAT,
    BlockedYPosition FLOAT,
    GoalCrossedYPosition FLOAT,
    GoalCrossedZPosition FLOAT,
    ExpectedGoals FLOAT,
    ExpectedGoalsOnTarget FLOAT,
    ShotType VARCHAR(255),
    Situation VARCHAR(255),
    Period VARCHAR(255),
    IsOwnGoal BOOLEAN,
    OnGoalShotX FLOAT,
    OnGoalShotY FLOAT,
    IsSavedOffLine BOOLEAN,
    TeamColor VARCHAR(255),
    IsPenalty BOOLEAN,
    NewScore VARCHAR(255),
    AssistString VARCHAR(255),
    AssistPlayerID BIGINT,
    PRIMARY KEY (MatchID, ShotMapID, PlayerID)
    );

CREATE TABLE IF NOT EXISTS player_stats_fact_stg (
    PlayerID BIGINT,
    TeamID INT,
    MatchID BIGINT,
    IsGoalkeeper BOOLEAN,
    FotmobRating FLOAT,
    MinutesPlayed INT,
    GoalsScored INT,
    Assists INT,
    TotalShots INT,
    AccuratePasses INT,
    ChancesCreated INT,
    ExpectedGoals FLOAT,
    ExpectedGoalsOnTarget FLOAT,
    ExpectedAssists FLOAT,
    xGandxA FLOAT,
    FantasyPoints FLOAT,
    FantasyBonusPoints FLOAT,
    ShotsOnTarget INT,
    BigChancesMissed INT,
    BlockedShots INT,
    HitWoodwork INT,
    Touches INT,
    TouchesinOppBox INT,
    SuccessfulDribbles INT,
    PassesintoFinalThird INT,
    Dispossessed INT,
    xGNonPenalty FLOAT,
    TacklesWon INT,
    Clearances INT,
    HeadedClearances INT,
    Interceptions INT,
    DefensiveActions INT,
    Recoveries INT,
    DribbledPast INT,
    DuelsWon INT,
    DuelsLost INT,
    GroundDuelsWon INT,
    AerialDuelsWon INT,
    WasFouled INT,
    FoulsCommitted INT,
    ShotMapID BIGINT,
    FunFact VARCHAR(255),
    PRIMARY KEY (PlayerID, TeamID, MatchID)
);

-- DIM and FACT Tables

CREATE TABLE IF NOT EXISTS team_dim (                          
    TeamWID INT DEFAULT nextval('seq_dim_team_id'),     
    TeamID INT PRIMARY KEY,
    TeamName VARCHAR(255),
    ImageUrl VARCHAR(255),
    PageUrl VARCHAR(255),
    TeamColor VARCHAR(255),
    LeagueID INT,
    InsertDate TIMESTAMP,
    UpdateDate TIMESTAMP
);



CREATE TABLE IF NOT EXISTS player_dim (      
    PlayerWID INT DEFAULT nextval('seq_dim_player_id'),                  
    PlayerID BIGINT PRIMARY KEY,
    PlayerName VARCHAR(255),
    FirstName VARCHAR(100), 
    LastName VARCHAR(100),  
    Age INT,
    CountryName VARCHAR(255),
    CountryCode VARCHAR(10),        
    ProfileUrl VARCHAR(255),
    InsertDate TIMESTAMP,
    UpdateDate TIMESTAMP
);


CREATE TABLE IF NOT EXISTS league_dim ( 
    LeagueWID INT DEFAULT nextval('seq_dim_league_id'),                        
    LeagueID INT PRIMARY KEY,       
    LeagueName VARCHAR(255),        
    ParentLeagueID INT,             
    ParentLeagueName VARCHAR(255),  
    CountryCode VARCHAR(10),
    InsertDate TIMESTAMP,
    UpdateDate TIMESTAMP
);


CREATE TABLE IF NOT EXISTS match_dim (          
    MatchWID INT DEFAULT nextval('seq_dim_match_id'),               
    MatchID BIGINT PRIMARY KEY, 
    MatchName VARCHAR(255),     
    MatchTimeUTC TIMESTAMP,     
    MatchRound VARCHAR(100),    
    LeagueID INT,               
    HomeTeamID INT,             
    AwayTeamID INT,             
    SeasonName VARCHAR(100),
    StadiumName VARCHAR(255),   
    Attendance INT,             
    RefereeName VARCHAR(255),
    MatchLatitude FLOAT,
    MatchLongitude FLOAT,
    MatchHighlightsUrl VARCHAR(255),
    MatchQAQuestion TEXT,
    MatchQAAnswer TEXT,
    MatchCountryCode VARCHAR(10),
    PlayerOfTheMatchID BIGINT,
    Momentum TEXT,
    InsertDate TIMESTAMP,
    UpdateDate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS date_dim (      
    DateWID INT DEFAULT nextval('seq_dim_date_id'),                    
    DateID INT PRIMARY KEY,
    FullDate DATE UNIQUE,
    Year INT,
    Month INT,
    Day INT,
    DayName VARCHAR(20),
    Weekday VARCHAR(20),
    Quarter INT,
    MonthName VARCHAR(20),
    LeapYear BOOLEAN,
    DayOfYear INT,
    DayOfWeek INT,
    Week INT,
    InsertDate TIMESTAMP,
    UpdateDate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS match_lineup_fact (
    MatchLineupWID INT DEFAULT nextval('seq_fact_match_lineup_id '),
    MatchID BIGINT,
    TeamID INT,
    PlayerID BIGINT,
    IsStarter BOOLEAN,
    PositionID INT,
    ShirtNumber INT,
    PlayerRating FLOAT,
    PlayerOfTheMatch VARCHAR(255),
    IsCaptain BOOLEAN,
    InsertDate TIMESTAMP,
    UpdateDate TIMESTAMP,
    PRIMARY KEY (MatchID, TeamID, PlayerID)    
);

CREATE TABLE IF NOT EXISTS player_shotmap_fact (
    PlayerShotmapWID INT DEFAULT nextval('seq_fact_player_shotmap_id'),
    MatchID BIGINT,
    ShotMapID BIGINT,
    EventType VARCHAR(255),
    PlayerID BIGINT,
    xPosition FLOAT,
    yPosition FLOAT,
    Minute INT,
    IsBlocked BOOLEAN,
    IsOnTarget BOOLEAN,
    BlockedXPosition FLOAT,
    BlockedYPosition FLOAT,
    GoalCrossedYPosition FLOAT,
    GoalCrossedZPosition FLOAT,
    ExpectedGoals FLOAT,
    ExpectedGoalsOnTarget FLOAT,
    ShotType VARCHAR(255),
    Situation VARCHAR(255),
    Period VARCHAR(255),
    IsOwnGoal BOOLEAN,
    OnGoalShotX FLOAT,
    OnGoalShotY FLOAT,
    IsSavedOffLine BOOLEAN,
    TeamColor VARCHAR(255),
    IsPenalty BOOLEAN,
    NewScore VARCHAR(255),
    AssistString VARCHAR(255),
    AssistPlayerID BIGINT,
    InsertDate TIMESTAMP,
    UpdateDate TIMESTAMP,
    PRIMARY KEY (MatchID, ShotMapID, PlayerID)
);

CREATE TABLE IF NOT EXISTS player_stats_fact (
    PlayerStatsWID INT DEFAULT nextval('seq_fact_player_stats_id'),
    PlayerID BIGINT,
    TeamID INT,
    MatchID BIGINT,
    IsGoalkeeper BOOLEAN,
    FotmobRating FLOAT,
    MinutesPlayed INT,
    GoalsScored INT,
    Assists INT,
    TotalShots INT,
    AccuratePasses INT,
    ChancesCreated INT,
    ExpectedGoals FLOAT,
    ExpectedGoalsOnTarget FLOAT,
    ExpectedAssists FLOAT,
    xGandxA FLOAT,
    FantasyPoints FLOAT,
    FantasyBonusPoints FLOAT,
    ShotsOnTarget INT,
    BigChancesMissed INT,
    BlockedShots INT,
    HitWoodwork INT,
    Touches INT,
    TouchesinOppBox INT,
    SuccessfulDribbles INT,
    PassesintoFinalThird INT,
    Dispossessed INT,
    xGNonPenalty FLOAT,
    TacklesWon INT,
    Clearances INT,
    HeadedClearances INT,
    Interceptions INT,
    DefensiveActions INT,
    Recoveries INT,
    DribbledPast INT,
    DuelsWon INT,
    DuelsLost INT,
    GroundDuelsWon INT,
    AerialDuelsWon INT,
    WasFouled INT,
    FoulsCommitted INT,
    ShotMapID BIGINT,
    FunFact VARCHAR(255),
    InsertDate TIMESTAMP,
    UpdateDate TIMESTAMP,
    PRIMARY KEY (PlayerID, TeamID, MatchID)
);
    
                        """))

if __name__ == "__main__":
    file_path = os.path.join(os.path.dirname(__file__), '../.env')
    engine = postgres_connection(file_path)
    create_all_tables_sequences(engine)
    with engine.connect() as conn:
        query_result = conn.execute(text("""select count(*) from player_stats_fact;"""))
        print(query_result.scalar())
    print("Tables and Sequences Created!")