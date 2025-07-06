from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit
import argparse
import logging
import os
import requests
import sys
from dotenv import load_dotenv
from google.cloud import storage
from sqlalchemy import create_engine, text
from datetime import datetime
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    BooleanType,
    MapType,
    FloatType,
    DoubleType,
    LongType,
    TimestampType,
)
from pyspark.sql.functions import col, explode, lit, when
from data_loading_functions import (
    dim_team_stg, dim_player_stg, dim_match_stg, dim_league_stg,
    fact_match_lineup_stg)

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

# def parse_arguments():
#     parser = argparse.ArgumentParser(description="Load Teams Json data from GCS to Postgres")
#     parser.add_argument('--gcs-path', required=True, help='GCS path with JSON files (gs://bucket/path/)')
#     parser.add_argument('--supabase-host', required=True, help='Supabase Postgres host')
#     parser.add_argument('--supabase-port', default='5432', help='Supabase Postgres port')
#     parser.add_argument('--supabase-db', required=True, help='Supabase database name')
#     parser.add_argument('--supabase-user', required=True, help='Supabase database user')
#     parser.add_argument('--supabase-password', required=True, help='Supabase database password')
#     parser.add_argument('--dim-table', default='dim_stg', help='Dimension staging table name')
#     parser.add_argument('--processing-date', default=datetime.now().strftime('%Y-%m-%d'), 
#                         help='Processing date in YYYY-MM-DD format')
#     return parser.parse_args()

def create_spark_session():
    return (SparkSession.builder
            .appName("SparkGCPtoPostgres")
            .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar,/opt/spark/jars/gcs-connector-hadoop3-latest.jar")
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/secrets/gcp_credentials.json")
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
            .getOrCreate())

def custom_schema():
        
    custom_schema = StructType([
        StructField("general", StructType([
            StructField("matchId", StringType(), True),
            StructField("matchName", StringType(), True),
            StructField("matchRound", StringType(), True),
            StructField("teamColors", StructType([
                StructField("darkMode", StructType([
                    StructField("home", StringType(), True),
                    StructField("away", StringType(), True)
                ]), True),
                StructField("lightMode", StructType([
                    StructField("home", StringType(), True),
                    StructField("away", StringType(), True)
                ]), True),
                StructField("fontDarkMode", StructType([
                    StructField("home", StringType(), True),
                    StructField("away", StringType(), True)
                ]), True),
                StructField("fontLightMode", StructType([
                    StructField("home", StringType(), True),
                    StructField("away", StringType(), True)
                ]), True)
            ]), True),
            StructField("leagueId", IntegerType(), True),
            StructField("leagueName", StringType(), True),
            StructField("leagueRoundName", StringType(), True),
            StructField("parentLeagueId", IntegerType(), True),
            StructField("countryCode", StringType(), True),
            StructField("parentLeagueName", StringType(), True),
            StructField("parentLeagueSeason", StringType(), True),
            StructField("parentLeagueTopScorerLink", StringType(), True),
            StructField("parentLeagueTournamentId", IntegerType(), True),
            StructField("homeTeam", StructType([
                StructField("name", StringType(), True),
                StructField("id", IntegerType(), True)
            ]), True),
            StructField("awayTeam", StructType([
                StructField("name", StringType(), True),
                StructField("id", IntegerType(), True)
            ]), True),
            StructField("coverageLevel", StringType(), True),
            StructField("matchTimeUTC", StringType(), True), # Consider TimestampType() if conversion is needed
            StructField("matchTimeUTCDate", StringType(), True), # Consider TimestampType() if conversion is needed
            StructField("started", BooleanType(), True),
            StructField("finished", BooleanType(), True)
        ]), True),
        StructField("header", StructType([
            StructField("teams", ArrayType(
                StructType([
                    StructField("name", StringType(), True),
                    StructField("id", IntegerType(), True),
                    StructField("score", IntegerType(), True),
                    StructField("imageUrl", StringType(), True),
                    StructField("pageUrl", StringType(), True),
                    StructField("fifaRank", IntegerType(), True) # Assuming Integer, adjust if needed (it's null in data)
                ])
            ), True),
            StructField("status", StructType([
                StructField("utcTime", StringType(), True), # Consider TimestampType()
                StructField("numberOfHomeRedCards", IntegerType(), True),
                StructField("numberOfAwayRedCards", IntegerType(), True),
                StructField("halfs", StructType([
                    StructField("firstHalfStarted", StringType(), True), # Consider TimestampType()
                    StructField("firstHalfEnded", StringType(), True), # Consider TimestampType()
                    StructField("secondHalfStarted", StringType(), True), # Consider TimestampType()
                    StructField("secondHalfEnded", StringType(), True), # Consider TimestampType()
                    StructField("firstExtraHalfStarted", StringType(), True),
                    StructField("secondExtraHalfStarted", StringType(), True),
                    StructField("gameEnded", StringType(), True) # Consider TimestampType()
                ]), True),
                StructField("finished", BooleanType(), True),
                StructField("started", BooleanType(), True),
                StructField("cancelled", BooleanType(), True),
                StructField("awarded", BooleanType(), True),
                StructField("scoreStr", StringType(), True),
                StructField("reason", StructType([
                    StructField("short", StringType(), True),
                    StructField("shortKey", StringType(), True),
                    StructField("long", StringType(), True),
                    StructField("longKey", StringType(), True)
                ]), True),
                StructField("whoLostOnPenalties", StringType(), True), # Type could vary, adjust if needed
                StructField("whoLostOnAggregated", StringType(), True)
            ]), True),
            StructField("events", StructType([
                # Using MapType for dynamic player names as keys
                StructField("homeTeamGoals", MapType(StringType(), ArrayType(
                    StructType([
                        StructField("reactKey", StringType(), True),
                        StructField("timeStr", IntegerType(), True), # String in data, converting to Int
                        StructField("type", StringType(), True),
                        StructField("time", IntegerType(), True),
                        StructField("overloadTime", StringType(), True), # Null in data, keep as String or choose specific type
                        StructField("eventId", LongType(), True), # Using LongType for potentially large IDs
                        StructField("player", StructType([
                            StructField("id", LongType(), True),
                            StructField("name", StringType(), True),
                            StructField("profileUrl", StringType(), True)
                        ]), True),
                        StructField("homeScore", IntegerType(), True),
                        StructField("awayScore", IntegerType(), True),
                        StructField("profileUrl", StringType(), True),
                        StructField("overloadTimeStr", BooleanType(), True), # Boolean in data
                        StructField("isHome", BooleanType(), True),
                        StructField("ownGoal", StringType(), True), # Null in data
                        StructField("goalDescription", StringType(), True), # Null in data
                        StructField("goalDescriptionKey", StringType(), True), # Null in data
                        StructField("suffix", StringType(), True), # Null in data
                        StructField("suffixKey", StringType(), True), # Null in data
                        StructField("isPenaltyShootoutEvent", BooleanType(), True),
                        StructField("nameStr", StringType(), True),
                        StructField("firstName", StringType(), True),
                        StructField("lastName", StringType(), True),
                        StructField("fullName", StringType(), True),
                        StructField("playerId", LongType(), True),
                        StructField("newScore", ArrayType(IntegerType()), True),
                        StructField("penShootoutScore", StringType(), True), # Null in data
                        StructField("shotmapEvent", StringType(), True), # Null in data
                        StructField("assistStr", StringType(), True), # Can be null
                        StructField("assistProfileUrl", StringType(), True), # Can be null
                        StructField("assistPlayerId", LongType(), True), # Can be null, using LongType
                        StructField("assistKey", StringType(), True), # Can be null
                        StructField("assistInput", StringType(), True) # Can be null
                    ])
                )), True),
                StructField("awayTeamGoals", MapType(StringType(), ArrayType( # Similar structure to homeTeamGoals
                    StructType([
                        StructField("reactKey", StringType(), True),
                        StructField("timeStr", IntegerType(), True), # String in data, converting to Int
                        StructField("type", StringType(), True),
                        StructField("time", IntegerType(), True),
                        StructField("overloadTime", StringType(), True), # Null in data
                        StructField("eventId", LongType(), True),
                        StructField("player", StructType([
                            StructField("id", LongType(), True),
                            StructField("name", StringType(), True),
                            StructField("profileUrl", StringType(), True)
                        ]), True),
                        StructField("homeScore", IntegerType(), True),
                        StructField("awayScore", IntegerType(), True),
                        StructField("profileUrl", StringType(), True),
                        StructField("overloadTimeStr", BooleanType(), True),
                        StructField("isHome", BooleanType(), True),
                        StructField("ownGoal", StringType(), True),
                        StructField("goalDescription", StringType(), True),
                        StructField("goalDescriptionKey", StringType(), True),
                        StructField("suffix", StringType(), True),
                        StructField("suffixKey", StringType(), True),
                        StructField("isPenaltyShootoutEvent", BooleanType(), True),
                        StructField("nameStr", StringType(), True),
                        StructField("firstName", StringType(), True),
                        StructField("lastName", StringType(), True),
                        StructField("fullName", StringType(), True),
                        StructField("playerId", LongType(), True),
                        StructField("newScore", ArrayType(IntegerType()), True),
                        StructField("penShootoutScore", StringType(), True),
                        StructField("shotmapEvent", StringType(), True),
                        StructField("assistStr", StringType(), True),
                        StructField("assistProfileUrl", StringType(), True),
                        StructField("assistPlayerId", LongType(), True),
                        StructField("assistKey", StringType(), True),
                        StructField("assistInput", StringType(), True)
                    ])
                )), True),
                StructField("homeTeamRedCards", MapType(StringType(), ArrayType(StringType())), True), # Assuming simple structure based on empty data
                StructField("awayTeamRedCards", MapType(StringType(), ArrayType(
                    StructType([
                        StructField("reactKey", StringType(), True),
                        StructField("timeStr", IntegerType(), True), # String in data, converting to Int
                        StructField("type", StringType(), True),
                        StructField("time", IntegerType(), True),
                        StructField("overloadTime", StringType(), True), # Null in data
                        StructField("eventId", LongType(), True),
                        StructField("player", StructType([
                            StructField("id", LongType(), True),
                            StructField("name", StringType(), True),
                            StructField("profileUrl", StringType(), True)
                        ]), True),
                        StructField("homeScore", IntegerType(), True),
                        StructField("awayScore", IntegerType(), True),
                        StructField("profileUrl", StringType(), True),
                        StructField("overloadTimeStr", BooleanType(), True),
                        StructField("isHome", BooleanType(), True),
                        StructField("nameStr", StringType(), True),
                        StructField("firstName", StringType(), True),
                        StructField("lastName", StringType(), True),
                        StructField("fullName", StringType(), True),
                        StructField("playerId", LongType(), True),
                        StructField("card", StringType(), True),
                        StructField("cardDescription", StringType(), True) # Null in data
                    ])
                )), True)
            ]), True)
        ]), True),
        # Additional top-level fields
        StructField("nav", ArrayType(StringType()), True),
        StructField("ongoing", BooleanType(), True),
        StructField("hasPendingVAR", BooleanType(), True),
        StructField("content", StructType([
            StructField("matchFacts", StructType([
                StructField("matchId", LongType(), True),
                StructField("highlights", StringType(), True), # Null in data
                StructField("playerOfTheMatch", StructType([]), True), # Empty object in data
                StructField("matchesInRound", ArrayType(
                    StructType([
                        StructField("id", StringType(), True),
                        StructField("utcTime", StringType(), True), # Consider TimestampType()
                        StructField("roundId", StringType(), True),
                        StructField("roundName", StringType(), True),
                        StructField("status", StructType([
                            StructField("utcTime", StringType(), True), # Consider TimestampType()
                            StructField("finished", BooleanType(), True),
                            StructField("started", BooleanType(), True),
                            StructField("cancelled", BooleanType(), True),
                            StructField("awarded", BooleanType(), True),
                            StructField("scoreStr", StringType(), True),
                            StructField("reason", StructType([
                                StructField("short", StringType(), True),
                                StructField("shortKey", StringType(), True),
                                StructField("long", StringType(), True),
                                StructField("longKey", StringType(), True)
                            ]), True)
                        ]), True),
                        StructField("homeScore", IntegerType(), True),
                        StructField("awayScore", IntegerType(), True),
                        StructField("home", StructType([
                            StructField("id", StringType(), True),
                            StructField("name", StringType(), True),
                            StructField("shortName", StringType(), True)
                        ]), True),
                        StructField("away", StructType([
                            StructField("id", StringType(), True),
                            StructField("name", StringType(), True),
                            StructField("shortName", StringType(), True)
                        ]), True),
                        StructField("league", StructType([
                            StructField("primaryLeagueId", IntegerType(), True),
                            StructField("leagueId", IntegerType(), True),
                            StructField("leagueName", StringType(), True),
                            StructField("parentLeagueId", IntegerType(), True),
                            StructField("gender", StringType(), True),
                            StructField("stageId", IntegerType(), True),
                            StructField("tournamentId", IntegerType(), True),
                            StructField("isCup", BooleanType(), True),
                            StructField("countryCode", StringType(), True)
                        ]), True)
                    ])
                ), True),
                StructField("events", StructType([ # Note: Duplicates structure from header.events
                    StructField("ongoing", BooleanType(), True),
                    StructField("events", ArrayType( # This events array contains various event types
                        StructType([ # Generic event structure - needs careful handling or flattening
                            StructField("reactKey", StringType(), True),
                            StructField("timeStr", IntegerType(), True), # Mixed types (string/int), using Int, might need cast
                            StructField("type", StringType(), True),
                            StructField("time", IntegerType(), True),
                            StructField("overloadTime", IntegerType(), True), # Mixed types (string/int), using Int
                            StructField("eventId", LongType(), True), # Can be null
                            StructField("player", StructType([
                                StructField("id", LongType(), True), # Can be null
                                StructField("name", StringType(), True), # Can be null
                                StructField("profileUrl", StringType(), True)
                            ]), True),
                            StructField("homeScore", IntegerType(), True),
                            StructField("awayScore", IntegerType(), True),
                            StructField("profileUrl", StringType(), True), # Can be null
                            StructField("overloadTimeStr", BooleanType(), True),
                            StructField("isHome", BooleanType(), True),
                            StructField("ownGoal", StringType(), True), # Can be null
                            StructField("goalDescription", StringType(), True), # Can be null
                            StructField("goalDescriptionKey", StringType(), True), # Can be null
                            StructField("suffix", StringType(), True), # Can be null
                            StructField("suffixKey", StringType(), True), # Can be null
                            StructField("isPenaltyShootoutEvent", BooleanType(), True),
                            StructField("nameStr", StringType(), True), # Can be null
                            StructField("firstName", StringType(), True), # Can be null
                            StructField("lastName", StringType(), True), # Can be null
                            StructField("fullName", StringType(), True), # Can be null
                            StructField("playerId", LongType(), True), # Can be null
                            StructField("newScore", ArrayType(IntegerType()), True), # Can be null
                            StructField("penShootoutScore", StringType(), True), # Can be null
                            StructField("shotmapEvent", StringType(), True), # Can be null
                            StructField("assistStr", StringType(), True), # Can be null
                            StructField("assistProfileUrl", StringType(), True), # Can be null
                            StructField("assistPlayerId", LongType(), True), # Can be null
                            StructField("assistKey", StringType(), True), # Can be null
                            StructField("assistInput", StringType(), True), # Can be null
                            StructField("card", StringType(), True), # Can be null
                            StructField("cardDescription", StringType(), True), # Can be null
                            StructField("minutesAddedStr", StringType(), True), # Can be null
                            StructField("minutesAddedKey", StringType(), True), # Can be null
                            StructField("minutesAddedInput", IntegerType(), True), # Can be null
                            StructField("halfStrShort", StringType(), True), # Can be null
                            StructField("halfStrKey", StringType(), True), # Can be null
                            StructField("injuredPlayerOut", BooleanType(), True), # Can be null
                            StructField("swap", ArrayType( # Can be null
                                StructType([
                                    StructField("name", StringType(), True),
                                    StructField("id", StringType(), True),
                                    StructField("profileUrl", StringType(), True)
                                ])
                            ), True)
                        ])
                    ), True),
                    StructField("eventTypes", ArrayType(StringType()), True),
                    StructField("penaltyShootoutEvents", StringType(), True) # Null in data
                ]), True),
                StructField("infoBox", StructType([
                    StructField("legInfo", StringType(), True), # Null in data
                    StructField("Match Date", StructType([ # Field name has space
                        StructField("utcTime", StringType(), True), # Consider TimestampType()
                        StructField("isDateCorrect", BooleanType(), True)
                    ]), True),
                    StructField("Tournament", StructType([
                        StructField("id", IntegerType(), True),
                        StructField("parentLeagueId", IntegerType(), True),
                        StructField("link", StringType(), True),
                        StructField("leagueName", StringType(), True),
                        StructField("roundName", StringType(), True),
                        StructField("round", StringType(), True),
                        StructField("selectedSeason", StringType(), True),
                        StructField("isCurrentSeason", BooleanType(), True)
                    ]), True),
                    StructField("Stadium", StructType([
                        StructField("name", StringType(), True),
                        StructField("country", StringType(), True)
                    ]), True),
                    StructField("Referee", StructType([
                        StructField("imgUrl", StringType(), True),
                        StructField("text", StringType(), True),
                        StructField("country", StringType(), True)
                    ]), True),
                    StructField("Attendance", IntegerType(), True)
                ]), True),
                StructField("teamForm", ArrayType( # Array of arrays
                    ArrayType(
                        StructType([
                            StructField("result", IntegerType(), True),
                            StructField("resultString", StringType(), True),
                            StructField("imageUrl", StringType(), True),
                            StructField("linkToMatch", StringType(), True),
                            StructField("date", StructType([
                                StructField("utcTime", StringType(), True) # Consider TimestampType()
                            ]), True),
                            StructField("teamPageUrl", StringType(), True),
                            StructField("tooltipText", StructType([
                                StructField("utcTime", StringType(), True), # Consider TimestampType()
                                StructField("homeTeam", StringType(), True),
                                StructField("homeTeamId", IntegerType(), True),
                                StructField("homeScore", StringType(), True),
                                StructField("awayTeam", StringType(), True),
                                StructField("awayTeamId", IntegerType(), True),
                                StructField("awayScore", StringType(), True)
                            ]), True),
                            StructField("score", StringType(), True),
                            StructField("home", StructType([
                                StructField("id", StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("isOurTeam", BooleanType(), True)
                            ]), True),
                            StructField("away", StructType([
                                StructField("id", StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("isOurTeam", BooleanType(), True)
                            ]), True)
                        ])
                    )
                ), True),
                StructField("poll", StructType([
                    StructField("renderToTop", BooleanType(), True)
                ]), True),
                StructField("topPlayers", StructType([
                    StructField("homeTopPlayers", ArrayType(StringType()), True), # Assuming string based on empty data
                    StructField("awayTopPlayers", ArrayType(StringType()), True) # Assuming string based on empty data
                ]), True),
                StructField("countryCode", StringType(), True),
                StructField("QAData", ArrayType(
                    StructType([
                        StructField("question", StringType(), True),
                        StructField("answer", StringType(), True)
                    ])
                ), True)
            ]), True),
            StructField("liveticker", StructType([
                StructField("langs", StringType(), True),
                StructField("teams", ArrayType(StringType()), True)
            ]), True),
            StructField("superlive", StructType([
                StructField("superLiveUrl", StringType(), True), # Null in data
                StructField("showSuperLive", BooleanType(), True)
            ]), True),
            StructField("buzz", StringType(), True), # Null in data
            StructField("stats", StructType([
                StructField("Periods", StructType([
                    StructField("All", StructType([
                        StructField("stats", ArrayType(
                            StructType([
                                StructField("title", StringType(), True),
                                StructField("key", StringType(), True),
                                StructField("stats", ArrayType( # Array containing stat details
                                    StructType([
                                        StructField("title", StringType(), True),
                                        StructField("key", StringType(), True),
                                        StructField("stats", ArrayType(IntegerType()), True), # Stats values
                                        StructField("type", StringType(), True),
                                        StructField("highlighted", StringType(), True)
                                    ])
                                ), True)
                            ])
                        ), True),
                        StructField("teamColors", StructType([ # Duplicates structure from general.teamColors
                            StructField("darkMode", StructType([
                                StructField("home", StringType(), True),
                                StructField("away", StringType(), True)
                            ]), True),
                            StructField("lightMode", StructType([
                                StructField("home", StringType(), True),
                                StructField("away", StringType(), True)
                            ]), True),
                            StructField("fontDarkMode", StructType([
                                StructField("home", StringType(), True),
                                StructField("away", StringType(), True)
                            ]), True),
                            StructField("fontLightMode", StructType([
                                StructField("home", StringType(), True),
                                StructField("away", StringType(), True)
                            ]), True)
                        ]), True)
                    ]), True)
                ]), True),
                StructField("playerStats", StringType(), True), # Null in data
            ]), True),
            StructField("shotmap", StructType([
                StructField("shots", ArrayType(StringType()), True), # Assuming string based on empty data
                StructField("Periods", StructType([ # Assuming structure mirrors stats.Periods
                    StructField("All", ArrayType(StringType()), True) # Assuming string based on empty data
                ]), True)
            ]), True),
            StructField("lineup", StructType([
                StructField("matchId", LongType(), True),
                StructField("lineupType", StringType(), True),
                StructField("availableFilters", ArrayType(StringType()), True),
                StructField("homeTeam", StructType([ # Detailed lineup structure
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                    StructField("formation", StringType(), True),
                    StructField("starters", ArrayType(
                        StructType([
                            StructField("id", LongType(), True),
                            StructField("name", StringType(), True),
                            StructField("positionId", IntegerType(), True),
                            StructField("usualPlayingPositionId", IntegerType(), True),
                            StructField("shirtNumber", StringType(), True), # String in data
                            StructField("isCaptain", BooleanType(), True),
                            StructField("horizontalLayout", StructType([
                                StructField("x", DoubleType(), True),
                                StructField("y", DoubleType(), True),
                                StructField("height", DoubleType(), True),
                                StructField("width", DoubleType(), True)
                            ]), True),
                            StructField("verticalLayout", StructType([
                                StructField("x", DoubleType(), True),
                                StructField("y", DoubleType(), True),
                                StructField("height", DoubleType(), True),
                                StructField("width", DoubleType(), True)
                            ]), True),
                            StructField("performance", StructType([
                                StructField("events", ArrayType(
                                    StructType([
                                        StructField("type", StringType(), True),
                                        StructField("time", IntegerType(), True)
                                    ])
                                ), True),
                                StructField("substitutionEvents", ArrayType( # Empty in example for some players
                                    StructType([
                                        StructField("time", IntegerType(), True),
                                        StructField("type", StringType(), True),
                                        StructField("reason", StringType(), True)
                                    ])
                                ), True),
                                StructField("playerOfTheMatch", BooleanType(), True)
                            ]), True),
                            StructField("firstName", StringType(), True),
                            StructField("lastName", StringType(), True)
                        ])
                    ), True),
                    StructField("coach", StructType([
                        StructField("id", LongType(), True),
                        StructField("name", StringType(), True),
                        StructField("usualPlayingPositionId", StringType(), True), # Null in data
                        StructField("primaryTeamName", StringType(), True),
                        StructField("performance", StructType([
                            StructField("events", ArrayType(StringType()), True) # Assuming string based on empty data
                        ]), True),
                        StructField("firstName", StringType(), True),
                        StructField("lastName", StringType(), True),
                        StructField("primaryTeamId", IntegerType(), True),
                        StructField("isCoach", BooleanType(), True)
                    ]), True),
                    StructField("subs", ArrayType( # Similar structure to starters but simpler
                        StructType([
                            StructField("id", LongType(), True),
                            StructField("name", StringType(), True),
                            StructField("positionId", IntegerType(), True),
                            StructField("usualPlayingPositionId", IntegerType(), True),
                            StructField("shirtNumber", StringType(), True), # String in data
                            StructField("isCaptain", BooleanType(), True),
                            StructField("performance", StructType([
                                StructField("events", ArrayType(StringType()), True), # Assuming string
                                StructField("substitutionEvents", ArrayType(
                                    StructType([
                                        StructField("time", IntegerType(), True),
                                        StructField("type", StringType(), True),
                                        StructField("reason", StringType(), True)
                                    ])
                                ), True),
                                StructField("playerOfTheMatch", BooleanType(), True)
                            ]), True),
                            StructField("firstName", StringType(), True),
                            StructField("lastName", StringType(), True)
                        ])
                    ), True),
                    StructField("unavailable", ArrayType(StringType()), True) # Assuming string based on empty data
                ]), True),
                StructField("awayTeam", StructType([ # Mirrors homeTeam structure
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                    StructField("formation", StringType(), True),
                    StructField("starters", ArrayType(
                        StructType([
                            StructField("id", LongType(), True),
                            StructField("name", StringType(), True),
                            StructField("positionId", IntegerType(), True),
                            StructField("usualPlayingPositionId", IntegerType(), True),
                            StructField("shirtNumber", StringType(), True),
                            StructField("isCaptain", BooleanType(), True),
                            StructField("horizontalLayout", StructType([
                                StructField("x", DoubleType(), True),
                                StructField("y", DoubleType(), True),
                                StructField("height", DoubleType(), True),
                                StructField("width", DoubleType(), True)
                            ]), True),
                            StructField("verticalLayout", StructType([
                                StructField("x", DoubleType(), True),
                                StructField("y", DoubleType(), True),
                                StructField("height", DoubleType(), True),
                                StructField("width", DoubleType(), True)
                            ]), True),
                            StructField("performance", StructType([
                                StructField("events", ArrayType(
                                    StructType([
                                        StructField("type", StringType(), True),
                                        StructField("time", IntegerType(), True)
                                    ])
                                ), True),
                                StructField("substitutionEvents", ArrayType(
                                    StructType([
                                        StructField("time", IntegerType(), True),
                                        StructField("type", StringType(), True),
                                        StructField("reason", StringType(), True)
                                    ])
                                ), True),
                                StructField("playerOfTheMatch", BooleanType(), True)
                            ]), True),
                            StructField("firstName", StringType(), True),
                            StructField("lastName", StringType(), True)
                        ])
                    ), True),
                    StructField("coach", StructType([
                        StructField("id", LongType(), True),
                        StructField("name", StringType(), True),
                        StructField("usualPlayingPositionId", StringType(), True),
                        StructField("primaryTeamName", StringType(), True),
                        StructField("performance", StructType([
                            StructField("events", ArrayType(StringType()), True)
                        ]), True),
                        StructField("firstName", StringType(), True),
                        StructField("lastName", StringType(), True),
                        StructField("primaryTeamId", IntegerType(), True),
                        StructField("isCoach", BooleanType(), True)
                    ]), True),
                    StructField("subs", ArrayType(
                        StructType([
                            StructField("id", LongType(), True),
                            StructField("name", StringType(), True),
                            StructField("positionId", IntegerType(), True),
                            StructField("usualPlayingPositionId", IntegerType(), True),
                            StructField("shirtNumber", StringType(), True),
                            StructField("isCaptain", BooleanType(), True),
                            StructField("performance", StructType([
                                StructField("events", ArrayType(StringType()), True),
                                StructField("substitutionEvents", ArrayType(
                                    StructType([
                                        StructField("time", IntegerType(), True),
                                        StructField("type", StringType(), True),
                                        StructField("reason", StringType(), True)
                                    ])
                                ), True),
                                StructField("playerOfTheMatch", BooleanType(), True)
                            ]), True),
                            StructField("firstName", StringType(), True),
                            StructField("lastName", StringType(), True)
                        ])
                    ), True),
                    StructField("unavailable", ArrayType(StringType()), True)
                ]), True)
            ]), True),
            StructField("playoff", BooleanType(), True),
            StructField("table", StructType([
                StructField("leagueId", StringType(), True),
                StructField("url", StringType(), True),
                StructField("teams", ArrayType(IntegerType()), True),
                StructField("tournamentNameForUrl", StringType(), True),
                StructField("parentLeagueId", IntegerType(), True),
                StructField("parentLeagueName", StringType(), True),
                StructField("isCurrentSeason", BooleanType(), True),
                StructField("parentLeagueSeason", StringType(), True),
                StructField("countryCode", StringType(), True)
            ]), True),
            StructField("h2h", StructType([
                StructField("summary", ArrayType(IntegerType()), True),
                StructField("matches", ArrayType(
                    StructType([
                        StructField("time", StructType([
                            StructField("utcTime", StringType(), True) # Consider TimestampType()
                        ]), True),
                        StructField("matchUrl", StringType(), True),
                        StructField("league", StructType([
                            StructField("name", StringType(), True),
                            StructField("id", StringType(), True),
                            StructField("pageUrl", StringType(), True)
                        ]), True),
                        StructField("home", StructType([
                            StructField("name", StringType(), True),
                            StructField("id", StringType(), True)
                        ]), True),
                        StructField("status", StructType([ # Similar to header.status
                            StructField("utcTime", StringType(), True), # Consider TimestampType()
                            StructField("started", BooleanType(), True),
                            StructField("cancelled", BooleanType(), True),
                            StructField("finished", BooleanType(), True),
                            StructField("awarded", BooleanType(), True), # Added based on other status objects
                            StructField("scoreStr", StringType(), True), # Added based on other status objects
                            StructField("reason", StructType([ # Added based on other status objects
                                StructField("short", StringType(), True),
                                StructField("shortKey", StringType(), True),
                                StructField("long", StringType(), True),
                                StructField("longKey", StringType(), True)
                            ]), True)
                        ]), True),
                        StructField("finished", BooleanType(), True), # Note: appears inside and outside status
                        StructField("away", StructType([
                            StructField("name", StringType(), True),
                            StructField("id", StringType(), True)
                        ]), True)
                    ])
                ), True)
            ]), True),
            StructField("momentum", BooleanType(), True) # Assuming boolean based on value
        ]), True),
        StructField("seo", StructType([
            StructField("path", StringType(), True),
            StructField("eventJSONLD", StructType([ # Contains nested JSON-LD schema definitions
                StructField("@context", StringType(), True),
                StructField("@type", StringType(), True),
                StructField("sport", StringType(), True),
                StructField("homeTeam", StructType([
                    StructField("@context", StringType(), True),
                    StructField("@type", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("sport", StringType(), True),
                    StructField("logo", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("location", StringType(), True), # Null in data
                    StructField("memberOf", StringType(), True) # Null in data
                ]), True),
                StructField("awayTeam", StructType([ # Mirrors homeTeam structure
                    StructField("@context", StringType(), True),
                    StructField("@type", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("sport", StringType(), True),
                    StructField("logo", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("location", StringType(), True),
                    StructField("memberOf", StringType(), True)
                ]), True),
                StructField("name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("startDate", StringType(), True), # Consider TimestampType()
                StructField("endDate", StringType(), True), # Consider TimestampType()
                StructField("eventStatus", StringType(), True),
                StructField("eventAttendanceMode", StringType(), True),
                StructField("location", StructType([
                    StructField("@type", StringType(), True),
                    StructField("url", StringType(), True)
                ]), True),
                StructField("image", ArrayType(StringType()), True),
                StructField("organizer", StructType([
                    StructField("@type", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("logo", StringType(), True)
                ]), True),
                StructField("offers", StructType([
                    StructField("@type", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("availability", StringType(), True),
                    StructField("price", StringType(), True), # String "0" in data
                    StructField("priceCurrency", StringType(), True),
                    StructField("validFrom", StringType(), True) # Consider TimestampType()
                ]), True),
                StructField("performer", ArrayType(
                    StructType([
                        StructField("@type", StringType(), True),
                        StructField("name", StringType(), True),
                        StructField("url", StringType(), True)
                    ])
                ), True)
            ]), True),
            StructField("breadcrumbJSONLD", ArrayType(
                StructType([
                    StructField("@context", StringType(), True),
                    StructField("@type", StringType(), True),
                    StructField("itemListElement", ArrayType(
                        StructType([
                            StructField("@type", StringType(), True),
                            StructField("position", IntegerType(), True),
                            StructField("name", StringType(), True),
                            StructField("item", StringType(), True)
                        ])
                    ), True)
                ])
            ), True),
            StructField("faqJSONLD", StructType([
                StructField("@context", StringType(), True),
                StructField("@type", StringType(), True),
                StructField("mainEntity", ArrayType(
                    StructType([
                        StructField("@type", StringType(), True),
                        StructField("name", StringType(), True),
                        StructField("acceptedAnswer", StructType([
                            StructField("@type", StringType(), True),
                            StructField("text", StringType(), True)
                        ]), True)
                    ])
                ), True)
            ]), True)
        ]), True)
    ])

    return custom_schema

def load_to_staging(spark_session, gcs_path, schema, postgres_args):
    '''
    Loads the data from the GCS bucket to the staging area
    '''
    raw_df = spark_session.read.option("multiline", "true").schema(schema).json(f"{gcs_path}*.json")

    # print(raw_df.limit(5).toPandas().head())
    # print(raw_df.limit(5).toPandas().columns)

    # Load Dim Stg Tables
    # dim_team_stg(postgres_args, raw_df)
    # dim_player_stg(postgres_args, raw_df)
    # dim_match_stg(postgres_args, raw_df)
    # dim_league_stg(postgres_args, raw_df)
    fact_match_lineup_stg(postgres_args, raw_df)


def main():
    # args = parse_arguments()

    spark = create_spark_session()
    env_file_path = os.path.join(os.path.dirname(__file__), '../.env')
    # env_file_path = ".env"
    sql_username, sql_password, sql_host, sql_port, sql_database = postgres_credentials(env_file_path)
    
    load_dotenv(dotenv_path=env_file_path)
    gcs_path = "gs://terraform-fotmob-terra-bucket-kg/"

    try:

        postgres_args = {
            "user": sql_username,
            "password": sql_password,
            "driver": "org.postgresql.Driver",
            "url": f"jdbc:postgresql://{sql_host}:{sql_port}/{sql_database}",

        }
        jdbc_url = postgres_args["url"]

        logger.info(f"Looking for team data in {gcs_path}...")
        print("sql_username:", sql_username)
        print("sql_password:", sql_password)
        print("sql_host:", sql_host)
        print("sql_port:", sql_port)
        print("sql_database:", sql_database)
        schema = custom_schema()
        # raw_df = spark.read.option("multiline", "true").schema(schema).json(f"{gcs_path}*.json")
        # print(raw_df.head(100))
        load_to_staging(spark, gcs_path, schema, postgres_args)




        

    
    except Exception as e:
        logger.error(f"Loading To Stage Failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
