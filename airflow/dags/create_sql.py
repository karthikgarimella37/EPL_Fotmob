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
    
    '''
    load_env(file_path)
    sql_username = os.getenv("sql_username")
    sql_password = os.getenv("sql_password")
    sql_host = os.getenv("sql_host")
    sql_port = os.getenv("sql_port")
    sql_database = os.getenv("sql_database")

    return sql_username, sql_password, sql_host, sql_port, sql_database


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

def load_env(file_path):
    load_dotenv(dotenv_path=file_path)

def create_all_tables_sequences(engine):
    with engine.connect() as conn:
        conn.execute(text("""\
                          
-- Create DB                          
-- CREATE DATABASE IF NOT EXISTS EPL_DB;

-- Sequences for Dim tables
CREATE SEQUENCE IF NOT EXISTS seq_team_id START 1 INCREMENT BY 1 NO maxvalue NO cycle;
CREATE SEQUENCE IF NOT EXISTS seq_player_id START 1 INCREMENT BY 1 NO maxvalue NO cycle;
CREATE SEQUENCE IF NOT EXISTS seq_match_id START 1 INCREMENT BY 1 NO maxvalue NO cycle;
CREATE SEQUENCE IF NOT EXISTS seq_date_id START 1 INCREMENT BY 1 NO maxvalue NO cycle;
CREATE SEQUENCE IF NOT EXISTS seq_league_id START 1 INCREMENT BY 1 NO maxvalue NO cycle;
CREATE SEQUENCE IF NOT EXISTS seq_event_type_id START 1 INCREMENT BY 1 NO maxvalue NO cycle;

CREATE SEQUENCE IF NOT EXISTS seq_factplayermatch_id START 1 INCREMENT BY 1 NO maxvalue NO cycle;
CREATE SEQUENCE IF NOT EXISTS seq_factmatchevents_id START 1 INCREMENT BY 1 NO maxvalue NO cycle;
CREATE SEQUENCE IF NOT EXISTS seq_factteammatch_id START 1 INCREMENT BY 1 NO maxvalue NO cycle;
CREATE SEQUENCE IF NOT EXISTS seq_factplayerseason_id START 1 INCREMENT BY 1  NO maxvalue NO cycle;                          
CREATE SEQUENCE IF NOT EXISTS seq_factteamseason_id START 1 INCREMENT BY 1 NO maxvalue NO cycle;                          

-- DIM and FACT STG Tables
CREATE TABLE IF NOT EXISTS DimTeamStg (                          
    TeamID INT PRIMARY KEY,
    TeamName VARCHAR(255),
    TeamColorHome VARCHAR(50),
    TeamColorAway VARCHAR(50),
    TeamFontColorHome VARCHAR(50),
    TeamFontColorAway VARCHAR(50)
);

CREATE TABLE DimPlayerStg (                        
    PlayerID INT PRIMARY KEY,
    PlayerName VARCHAR(255),
    Position VARCHAR(50),
    Nationality VARCHAR(50),
    Birthdate DATE,
    TeamID INT
);

CREATE TABLE DimMatchStg (                         
    MatchID INT PRIMARY KEY,
    MatchName VARCHAR(255),
    MatchTimeUTC TIMESTAMP,
    MatchRound VARCHAR(50),
    LeagueID INT,
    HomeTeamID INT,
    AwayTeamID INT,
    SeasonName VARCHAR(50),
    MatchStatus VARCHAR(20)
);

CREATE TABLE DimDateStg (                          
    DateID INT PRIMARY KEY,
    FullDate DATE UNIQUE,
    Year INT,
    Month INT,
    Day INT,
    Weekday VARCHAR(20),
    Quarter INT
);

CREATE TABLE DimLeagueStg (                        
    LeagueID INT PRIMARY KEY,
    LeagueName VARCHAR(255),
    ParentLeagueID INT,
    ParentLeagueName VARCHAR(255),
    CountryCode VARCHAR(10),
    TournamentID INT,
    SeasonName VARCHAR(50)
);

CREATE TABLE DimEventTypeStg (                        
    EventTypeID INT PRIMARY KEY,
    EventTypeName VARCHAR(50)
);

CREATE TABLE FactPlayerMatchStg (
    MatchID INT,
    PlayerID INT,
    TeamID INT,
    MinutesPlayed INT,
    Rating FLOAT,
    GoalsScored INT,
    Assists INT,
    TotalShots INT,
    ShotsOnTarget INT,
    ExpectedGoals FLOAT,
    ExpectedAssists FLOAT,
    AccuratePasses INT,
    TotalPasses INT,
    ChancesCreated INT,
    Touches INT,
    SuccessfulDribbles INT,
    TotalDribbles INT,
    AccurateCrosses INT,
    TotalCrosses INT,
    DuelsWon INT,
    DuelsLost INT,
    WasFouled INT,
    FoulsCommitted INT,
    IsPlayerOfTheMatch BOOLEAN
);

CREATE TABLE FactMatchEventsStg (
    MatchID INT,
    EventID SERIAL PRIMARY KEY,
    TimeMinute INT,
    AddedTimeMinute INT,
    HalfPeriod INT,
    TeamID INT,
    PlayerID INT,
    AssistPlayerID INT,
    EventTypeID INT,
    XPosition FLOAT,
    YPosition FLOAT,
    ExpectedGoals FLOAT
);

CREATE TABLE FactTeamMatchStg (
    MatchID INT,
    TeamID INT,
    Possession FLOAT,
    TotalShots INT,
    ShotsOnTarget INT,
    Corners INT,
    Fouls INT,
    YellowCards INT,
    RedCards INT,
    Offsides INT,
    ExpectedGoals FLOAT,
    BigChances INT,
    BigChancesMissed INT,
    PassAccuracy FLOAT,
    TotalPasses INT,
    CompletedPasses INT
);

CREATE TABLE FactPlayerSeasonStg (
    SeasonName VARCHAR(50),
    PlayerID INT,
    TeamID INT,
    MatchesPlayed INT,
    GoalsScored INT,
    Assists INT,
    MinutesPlayed INT,
    TotalShots INT,
    ShotsOnTarget INT,
    ExpectedGoals FLOAT,
    ExpectedAssists FLOAT,
    AccuratePasses INT,
    TotalPasses INT
);

CREATE TABLE FactTeamSeasonStg (
    SeasonName VARCHAR(50),
    TeamID INT,
    MatchesPlayed INT,
    Wins INT,
    Draws INT,
    Losses INT,
    GoalsScored INT,
    GoalsConceded INT,
    Points INT
);

-- DIM and FACT Tables
CREATE TABLE IF NOT EXISTS DimTeam (
    TeamWID INT DEFAULT nextval('seq_team_id'),                          
    TeamID INT PRIMARY KEY,
    TeamName VARCHAR(255),
    TeamColorHome VARCHAR(50),
    TeamColorAway VARCHAR(50),
    TeamFontColorHome VARCHAR(50),
    TeamFontColorAway VARCHAR(50)
);

CREATE TABLE DimPlayer (
    PlayerWID INT DEFAULT nextval('seq_player_id'),
    PlayerID INT PRIMARY KEY,
    PlayerName VARCHAR(255),
    Position VARCHAR(50),
    Nationality VARCHAR(50),
    Birthdate DATE,
    TeamID INT,
    FOREIGN KEY (TeamID) REFERENCES DimTeam(TeamID)
);

CREATE TABLE DimLeague (
    LeagueWID INT DEFAULT nextval('seq_match_id'),                          
    LeagueID INT PRIMARY KEY,
    LeagueName VARCHAR(255),
    ParentLeagueID INT,
    ParentLeagueName VARCHAR(255),
    CountryCode VARCHAR(10),
    TournamentID INT,
    SeasonName VARCHAR(50)
);
                          
CREATE TABLE DimMatch (
    MatchWID INT DEFAULT nextval('seq_match_id'),
    MatchID INT PRIMARY KEY,
    MatchName VARCHAR(255),
    MatchTimeUTC TIMESTAMP,
    MatchRound VARCHAR(50),
    LeagueID INT,
    HomeTeamID INT,
    AwayTeamID INT,
    SeasonName VARCHAR(50),
    MatchStatus VARCHAR(20),
    FOREIGN KEY (LeagueID) REFERENCES DimLeague(LeagueID),
    FOREIGN KEY (HomeTeamID) REFERENCES DimTeam(TeamID),
    FOREIGN KEY (AwayTeamID) REFERENCES DimTeam(TeamID)
);

CREATE TABLE DimDate (
    DateWID INT DEFAULT nextval('seq_date_id'),
    DateID INT PRIMARY KEY,
    FullDate DATE UNIQUE,
    Year INT,
    Month INT,
    Day INT,
    Weekday VARCHAR(20),
    Quarter INT
);


CREATE TABLE DimEventType (
    EventTypeWID INT DEFAULT nextval('seq_event_type_id'),                          
    EventTypeID INT PRIMARY KEY,
    EventTypeName VARCHAR(50)
);

CREATE TABLE FactPlayerMatch (
    MatchPlayerWID INT DEFAULT nextval('seq_factplayermatch_id'),                          
    MatchID INT,
    PlayerID INT,
    TeamID INT,
    MinutesPlayed INT,
    Rating FLOAT,
    GoalsScored INT,
    Assists INT,
    TotalShots INT,
    ShotsOnTarget INT,
    ExpectedGoals FLOAT,
    ExpectedAssists FLOAT,
    AccuratePasses INT,
    TotalPasses INT,
    ChancesCreated INT,
    Touches INT,
    SuccessfulDribbles INT,
    TotalDribbles INT,
    AccurateCrosses INT,
    TotalCrosses INT,
    DuelsWon INT,
    DuelsLost INT,
    WasFouled INT,
    FoulsCommitted INT,
    IsPlayerOfTheMatch BOOLEAN,
    PRIMARY KEY (MatchID, PlayerID),
    FOREIGN KEY (MatchID) REFERENCES DimMatch(MatchID),
    FOREIGN KEY (PlayerID) REFERENCES DimPlayer(PlayerID),
    FOREIGN KEY (TeamID) REFERENCES DimTeam(TeamID)
);

CREATE TABLE FactMatchEvents (
    MatchEventsWID INT DEFAULT nextval('seq_factmatchevents_id'),                                                    
    MatchID INT,
    EventID SERIAL PRIMARY KEY,
    TimeMinute INT,
    AddedTimeMinute INT,
    HalfPeriod INT,
    TeamID INT,
    PlayerID INT,
    AssistPlayerID INT,
    EventTypeID INT,
    XPosition FLOAT,
    YPosition FLOAT,
    ExpectedGoals FLOAT,
    FOREIGN KEY (MatchID) REFERENCES DimMatch(MatchID),
    FOREIGN KEY (TeamID) REFERENCES DimTeam(TeamID),
    FOREIGN KEY (PlayerID) REFERENCES DimPlayer(PlayerID),
    FOREIGN KEY (AssistPlayerID) REFERENCES DimPlayer(PlayerID),
    FOREIGN KEY (EventTypeID) REFERENCES DimEventType(EventTypeID)
);

CREATE TABLE FactTeamMatch (
    TeamMatchWID INT DEFAULT nextval('seq_factteammatch_id'),                          
    MatchID INT,
    TeamID INT,
    Possession FLOAT,
    TotalShots INT,
    ShotsOnTarget INT,
    Corners INT,
    Fouls INT,
    YellowCards INT,
    RedCards INT,
    Offsides INT,
    ExpectedGoals FLOAT,
    BigChances INT,
    BigChancesMissed INT,
    PassAccuracy FLOAT,
    TotalPasses INT,
    CompletedPasses INT,
    PRIMARY KEY (MatchID, TeamID),
    FOREIGN KEY (MatchID) REFERENCES DimMatch(MatchID),
    FOREIGN KEY (TeamID) REFERENCES DimTeam(TeamID)
);

CREATE TABLE FactPlayerSeason (
    PlayerSeasonWID INT DEFAULT nextval('seq_factplayerseason_id'),                          
    SeasonName VARCHAR(50),
    PlayerID INT,
    TeamID INT,
    MatchesPlayed INT,
    GoalsScored INT,
    Assists INT,
    MinutesPlayed INT,
    TotalShots INT,
    ShotsOnTarget INT,
    ExpectedGoals FLOAT,
    ExpectedAssists FLOAT,
    AccuratePasses INT,
    TotalPasses INT,
    PRIMARY KEY (SeasonName, PlayerID),
    FOREIGN KEY (PlayerID) REFERENCES DimPlayer(PlayerID),
    FOREIGN KEY (TeamID) REFERENCES DimTeam(TeamID)
);

CREATE TABLE FactTeamSeason (
    PlayerSeasonWID INT DEFAULT nextval('seq_factteamseason_id'),                          
    SeasonName VARCHAR(50),
    TeamID INT,
    MatchesPlayed INT,
    Wins INT,
    Draws INT,
    Losses INT,
    GoalsScored INT,
    GoalsConceded INT,
    Points INT,
    PRIMARY KEY (SeasonName, TeamID),
    FOREIGN KEY (TeamID) REFERENCES DimTeam(TeamID)
        );
    
                        """))
        
        conn.commit()

if __name__ == "__main__":
    file_path = os.path.join(os.path.dirname(__file__), '../../.env')
    engine = postgres_connection(file_path)
    create_all_tables_sequences(engine)
    with engine.connect() as conn:
        query = conn.execute(text("""select count(*) from FactTeamSeason;"""))
        print(query)
    print("Tables and Sequences Created!")