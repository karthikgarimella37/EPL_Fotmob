from pyspark.sql.functions import col, concat, lit, lower, regexp_replace, when, explode, row_number, to_timestamp, first
from pyspark.sql import Window
from pyspark.sql.types import StructType
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def write_to_postgres(df, table_name, postgres_args):
    '''
    Writes the dataframe to the postgres table
    '''

    df.write.jdbc(
        url = postgres_args['url'],
        table = table_name,
        mode = 'append',
        properties = {
    "user": postgres_args["user"],
    "password": postgres_args["password"],
    "driver": postgres_args["driver"],
    "prepareThreshold": "0"
})


def dim_team_stg(postgres_args, raw_df):
    '''
    Loading team_dim_stg Table
    '''
    # Select and flatten general data
    general_df = raw_df.select("general.*")

    # Create home team dataframe
    home_df = general_df.select(
        col("homeTeam.id").alias("TeamID"),
        col("homeTeam.name").alias("TeamName"),
        col("teamColors.darkMode.home").alias("TeamColor"),
        col("leagueId").alias("LeagueID")
    )

    # Create away team dataframe
    away_df = general_df.select(
        col("awayTeam.id").alias("TeamID"),
        col("awayTeam.name").alias("TeamName"),
        col("teamColors.darkMode.away").alias("TeamColor"),
        col("leagueId").alias("LeagueID")
    )

    # Union home and away teams
    teams_df = home_df.union(away_df)

    # Add constructed ImageUrl and PageUrl
    teams_with_urls = teams_df.withColumn(
        "ImageUrl",
        concat(lit("https://images.fotmob.com/image_resources/logo/teamlogo/"), col("TeamID"), lit(".png"))
    ).withColumn(
        "PageUrl",
        concat(lit("https://www.fotmob.com/teams/"), col("TeamID"), lit("/overview/"), lower(regexp_replace(col("TeamName"), " ", "-")))
    )

    # Select columns for staging table
    dim_team_stg = teams_with_urls.select(
        "TeamID",
        "TeamName",
        "ImageUrl",
        "PageUrl",
        "TeamColor",
        "LeagueID"
    ).dropDuplicates(["TeamID"])

    logger.info(f"Inserting {dim_team_stg.count()} teams into team_dim_stg table.")

    write_to_postgres(dim_team_stg, 'team_dim_stg', postgres_args)

    logger.info(f"Inserted {dim_team_stg.count()} teams into team_dim_stg table.")


def dim_player_stg(postgres_args, raw_df):
    '''
    Loading player_dim_stg Table
    '''
    
    def get_player_df(team_path):
        # Explode the player list and get all fields from the struct
        player_df = raw_df.select(explode(team_path).alias("player")).select("player.*")

        # Define the columns we want to select
        columns_to_select = ["id", "name", "firstName", "lastName"]

        # Check for optional columns and add them with nulls if they don't exist.
        optional_cols_with_types = {
            "age": "int",
            "countryName": "string",
            "countryCode": "string"
        }

        for col_name, col_type in optional_cols_with_types.items():
            if col_name not in player_df.columns:
                player_df = player_df.withColumn(col_name, lit(None).cast(col_type))
            columns_to_select.append(col_name)

        return player_df.select(columns_to_select)

    home_starters = get_player_df("content.lineup.homeTeam.starters")
    home_subs = get_player_df("content.lineup.homeTeam.subs")
    away_starters = get_player_df("content.lineup.awayTeam.starters")
    away_subs = get_player_df("content.lineup.awayTeam.subs")

    # Union all players
    all_players_df = home_starters.unionByName(home_subs).unionByName(away_starters).unionByName(away_subs)

    # Select and rename columns for player_dim_stg
    player_details_df = all_players_df.select(
        col("id").alias("PlayerID"),
        col("name").alias("PlayerName"),
        col("firstName").alias("FirstName"),
        col("lastName").alias("LastName"),
        col("age").alias("Age"),
        col("countryName").alias("CountryName"),
        col("countryCode").alias("CountryCode")
    )

    

    # De-duplicate players by taking the one with the max age
    window_spec = Window.partitionBy("PlayerID").orderBy(col("Age").desc())
    deduplicated_players_df = player_details_df.withColumn("row_num", row_number().over(window_spec)) \
                                               .filter(col("row_num") == 1) \
                                               .drop("row_num")

    # Construct ProfileUrl
    player_with_url = deduplicated_players_df.withColumn(
        "ProfileUrl",
        concat(lit("https://www.fotmob.com/players/"), col("PlayerID"), lit("/"), lower(regexp_replace(col("PlayerName"), " ", "-")))
    )

    # Select final columns for the staging table
    dim_player_stg_df = player_with_url.select(
        "PlayerID",
        "PlayerName",
        "FirstName",
        "LastName",
        "Age",
        "CountryName",
        "CountryCode",
        "ProfileUrl"
    )
    
    logger.info(f"Inserting {dim_player_stg_df.count()} players into player_dim_stg table.")

    write_to_postgres(dim_player_stg_df, 'player_dim_stg', postgres_args)

    logger.info(f"Inserted {dim_player_stg_df.count()} players into player_dim_stg table.")


def dim_match_stg(postgres_args, raw_df):
    '''
    Loading match_dim_stg Table
    '''
    schema = raw_df.schema

    select_exprs = [
        col("content.matchFacts.matchId").alias("MatchID"),
        concat(col("general.homeTeam.name"), lit(" vs "), col("general.awayTeam.name")).alias("MatchName"),
        to_timestamp(col("content.matchFacts.infoBox").getField("Match Date").getField("utcTime")).alias("MatchTimeUTC"),
        col("content.matchFacts.infoBox.Tournament.round").alias("MatchRound"),
        col("general.leagueId").alias("LeagueID"),
        col("general.homeTeam.id").alias("HomeTeamID"),
        col("general.awayTeam.id").alias("AwayTeamID")
    ]

    # Optional columns check
    optional_cols = {
        "SeasonName": ("general.leagueSeason", "string"),
        "StadiumName": ("content.matchFacts.infoBox.Stadium.name", "string"),
        "Attendance": ("content.matchFacts.infoBox.Attendance", "int"),
        "RefereeName": ("content.matchFacts.infoBox.Referee.text", "string"),
        "MatchLatitude": ("content.matchFacts.infoBox.Stadium.lat", "float"),
        "MatchLongitude": ("content.matchFacts.infoBox.Stadium.lon", "float"),
        "MatchHighlightsUrl": ("content.matchFacts.highlights", "string"),
        "MatchCountryCode": ("content.matchFacts.countryCode", "string"),
        "PlayerOfTheMatchID": ("content.matchFacts.playerOfTheMatch.id", "bigint"),
        "Momentum": ("content.matchFacts.momentum", "string")
    }
    
    # Helper to check nested field existence
    def field_exists(df_schema, path):
        fields = path.split('.')
        current_schema = df_schema
        
        for field_name in fields:
            if not isinstance(current_schema, StructType) or field_name not in current_schema.names:
                return False
            current_schema = current_schema[field_name].dataType
        return True

    for alias, (path, dtype) in optional_cols.items():
        if field_exists(schema, path):
            select_exprs.append(col(path).alias(alias))
        else:
            select_exprs.append(lit(None).cast(dtype).alias(alias))

    # Handle QAData separately due to array access
    if field_exists(schema, "content.matchFacts.QAData"):
        select_exprs.append(col("content.matchFacts.QAData").getItem(0).getField("question").alias("MatchQAQuestion"))
        select_exprs.append(col("content.matchFacts.QAData").getItem(0).getField("answer").alias("MatchQAAnswer"))
    else:
        select_exprs.append(lit(None).cast("string").alias("MatchQAQuestion"))
        select_exprs.append(lit(None).cast("string").alias("MatchQAAnswer"))

    match_facts_df = raw_df.select(select_exprs).dropDuplicates(["MatchID"])

    logger.info(f"Inserting {match_facts_df.count()} matches into match_dim_stg table.")
    write_to_postgres(match_facts_df, 'match_dim_stg', postgres_args)
    logger.info(f"Inserted {match_facts_df.count()} matches into match_dim_stg table.")

def dim_league_stg(postgres_args, raw_df):
    '''
    Loading league_dim_stg Table
    '''
    league_df = raw_df.select(
        col("general.leagueId").alias("LeagueID"),
        col("general.leagueName").alias("LeagueName"),
        col("general.parentLeagueId").alias("ParentLeagueID"),
        col("general.parentLeagueName").alias("ParentLeagueName"),
        col("general.countryCode").alias("CountryCode")
    ).dropDuplicates(["LeagueID"])

    logger.info(f"Inserting {league_df.count()} leagues into league_dim_stg table.")
    write_to_postgres(league_df, 'league_dim_stg', postgres_args)
    logger.info(f"Inserted {league_df.count()} leagues into league_dim_stg table.")


def fact_match_lineup_stg(postgres_args, raw_df):
    '''
    Loading match_lineup_fact_stg Table
    '''
    def _extract_and_process_lineup(raw_df, team_str, player_str):
        is_starter = player_str == 'starters'
        path_to_players = f"content.lineup.{team_str}.{player_str}"

        # Explode and flatten player data
        lineup_df = raw_df.select(
            col("content.lineup.matchId").alias("MatchID"),
            col(f"content.lineup.{team_str}.id").alias("TeamID"),
            explode(col(path_to_players)).alias("player")
        ).select("MatchID", "TeamID", "player.*")
        
        lineup_df = lineup_df.withColumn("IsStarter", lit(is_starter))

        if "isCaptain" not in lineup_df.columns:
            lineup_df = lineup_df.withColumn("isCaptain", lit(False).cast("boolean"))
        
        # Ensure correct data types for integer columns
        if "shirtNumber" in lineup_df.columns:
            lineup_df = lineup_df.withColumn("shirtNumber", col("shirtNumber").cast("int"))
        if "positionId" in lineup_df.columns:
            lineup_df = lineup_df.withColumn("positionId", col("positionId").cast("int"))

        if "performance" in lineup_df.columns:
            perf_schema = lineup_df.select("performance.*").schema
            if "rating" not in perf_schema:
                lineup_df = lineup_df.withColumn("rating", lit(None).cast("float"))
            else:
                lineup_df = lineup_df.withColumn("rating", col("performance.rating"))
            
            if "playerOfTheMatch" not in perf_schema:
                lineup_df = lineup_df.withColumn("playerOfTheMatch", lit(None).cast("string"))
            else:
                lineup_df = lineup_df.withColumn("playerOfTheMatch", col("performance.playerOfTheMatch").cast("string"))
        else:
            lineup_df = lineup_df.withColumn("rating", lit(None).cast("float"))
            lineup_df = lineup_df.withColumn("playerOfTheMatch", lit(None).cast("string"))
            
        return lineup_df.select(
            col("MatchID"),
            col("TeamID"),
            col("id").alias("PlayerID"),
            col("IsStarter"),
            col("positionId").alias("PositionID"),
            col("shirtNumber").alias("ShirtNumber"),
            col("rating").alias("PlayerRating"),
            col("playerOfTheMatch").alias("PlayerOfTheMatch"),
            col("isCaptain").alias("IsCaptain")
        )

    home_starters = _extract_and_process_lineup(raw_df, 'homeTeam', 'starters')
    home_subs = _extract_and_process_lineup(raw_df, 'homeTeam', 'subs')
    away_starters = _extract_and_process_lineup(raw_df, 'awayTeam', 'starters')
    away_subs = _extract_and_process_lineup(raw_df, 'awayTeam', 'subs')

    match_lineup_df = home_starters.unionByName(home_subs) \
                                   .unionByName(away_starters) \
                                   .unionByName(away_subs)

    final_df = match_lineup_df.dropDuplicates(["MatchID", "TeamID", "PlayerID"])

    logger.info(f"Inserting {final_df.count()} match lineups into match_lineup_fact_stg table.")
    write_to_postgres(final_df, 'match_lineup_fact_stg', postgres_args)
    logger.info(f"Inserted {final_df.count()} match lineups into match_lineup_fact_stg table.")


def fact_player_shotmap_stg(postgres_args, raw_df):
    '''
    Loading player_shotmap_fact_stg Table
    '''
    # Check if shotmap data is available in the schema for this file.
    if not "shotMap" in raw_df.select("content.*").columns or \
       not "shots" in raw_df.select("content.shotMap.*").columns:
        logger.info("Shotmap data not found in this file's schema. Skipping.")
        return

    # Filter for rows that have shotmap data and explode the shots array.
    shotmap_df = raw_df.filter(col("content.shotMap.shots").isNotNull()).select(
        col("general.matchId").alias("MatchID"),
        explode(col("content.shotMap.shots")).alias("shot")
    )

    # If the resulting DataFrame is empty, there's nothing to load.
    if shotmap_df.rdd.isEmpty():
        logger.info("Shotmap data is null or empty for this match. Skipping.")
        return
    
    # Select and alias all the required columns from the shot data.
    final_df = shotmap_df.select(
        col("MatchID"),
        col("shot.id").alias("ShotMapID"),
        col("shot.eventType").alias("EventType"),
        col("shot.playerId").alias("PlayerID"),
        col("shot.x").alias("xPosition"),
        col("shot.y").alias("yPosition"),
        col("shot.min").alias("Minute"),
        col("shot.isBlocked").alias("IsBlocked"),
        col("shot.isOnTarget").alias("IsOnTarget"),
        col("shot.blockedX").alias("BlockedXPosition"),
        col("shot.blockedY").alias("BlockedYPosition"),
        col("shot.goalCrossedY").alias("GoalCrossedYPosition"),
        col("shot.goalCrossedZ").alias("GoalCrossedZPosition"),
        col("shot.expectedGoals").alias("ExpectedGoals"),
        col("shot.expectedGoalsOnTarget").alias("ExpectedGoalsOnTarget"),
        col("shot.shotType").alias("ShotType"),
        col("shot.situation").alias("Situation"),
        col("shot.period").alias("Period"),
        col("shot.isOwnGoal").alias("IsOwnGoal"),
        col("shot.onGoalShot.x").alias("OnGoalShotX"),
        col("shot.onGoalShot.y").alias("OnGoalShotY"),
        col("shot.isSavedOffLine").alias("IsSavedOffLine"),
        col("shot.teamColor").alias("TeamColor"),
    )
    
    # Add columns that do not exist in the source shotmap data.
    final_df = final_df.withColumn("IsPenalty", lit(None).cast("boolean")) \
                       .withColumn("NewScore", lit(None).cast("string")) \
                       .withColumn("AssistString", lit(None).cast("string")) \
                       .withColumn("AssistPlayerID", lit(None).cast("bigint"))

    # Drop duplicates based on the composite primary key.
    final_df = final_df.dropDuplicates(["MatchID", "ShotMapID", "PlayerID"])
    
    logger.info(f"Inserting {final_df.count()} shotmap events into player_shotmap_fact_stg table.")
    write_to_postgres(final_df, 'player_shotmap_fact_stg', postgres_args)
    logger.info(f"Inserted {final_df.count()} shotmap events into player_shotmap_fact_stg table.")
    
def fact_player_stats_stg(postgres_args, raw_df):
    '''
    Loading player_stats_fact_stg Table
    '''
    if "playerStats" not in raw_df.select("content.*").schema.names:
        logger.info("playerStats data not found in this file's schema. Skipping.")
        return

    # playerStats is a map, so we explode it to get rows for each player
    player_stats_df = raw_df.filter(col("content.playerStats").isNotNull()).select(
        col("general.matchId").alias("MatchID"),
        explode(col("content.playerStats")).alias("player_id_str", "player_data")
    )

    if player_stats_df.rdd.isEmpty():
        logger.info("playerStats data is null or empty. Skipping.")
        return

    # Extract base player info and explode stat groups
    base_df = player_stats_df.select(
        "MatchID",
        col("player_data.id").alias("PlayerID"),
        col("player_data.teamId").alias("TeamID"),
        col("player_data.isGoalkeeper").alias("IsGoalkeeper"),
        col("player_data.funFacts").getItem(0).getField('fallback').alias("FunFact"),
        col("player_data.shotmap").getItem(0).getField('id').alias("ShotMapID"),
        explode(col("player_data.stats")).alias("stat_group")
    )

    # From each stat group, get the map of stats
    stats_df = base_df.select(
        "MatchID", "PlayerID", "TeamID", "IsGoalkeeper", "FunFact", "ShotMapID",
        explode(col("stat_group.stats")).alias("stat_name", "stat_value_obj")
    )

    # Define stat mapping from JSON to DDL
    stat_mapping = {
        'FotMob rating': 'FotmobRating', 'Minutes played': 'MinutesPlayed', 'Goals': 'GoalsScored',
        'Assists': 'Assists', 'Total shots': 'TotalShots', 'Accurate passes': 'AccuratePasses',
        'Chances created': 'ChancesCreated', 'Expected goals (xG)': 'ExpectedGoals',
        'Expected goals on target (xGOT)': 'ExpectedGoalsOnTarget', 'Expected assists (xA)': 'ExpectedAssists',
        'xG + xA': 'xGandxA', 'Fantasy points': 'FantasyPoints', 'Fantasy bonus points': 'FantasyBonusPoints',
        'Shots on target': 'ShotsOnTarget', 'Big chances missed': 'BigChancesMissed',
        'Blocked shots': 'BlockedShots', 'Hit woodwork': 'HitWoodwork', 'Touches': 'Touches',
        'Touches in opposition box': 'TouchesinOppBox', 'Successful dribbles': 'SuccessfulDribbles',
        'Passes into final third': 'PassesintoFinalThird', 'Dispossessed': 'Dispossessed',
        'xG Non-penalty': 'xGNonPenalty', 'Tackles won': 'TacklesWon', 'Clearances': 'Clearances',
        'Headed clearance': 'HeadedClearances', 'Interceptions': 'Interceptions',
        'Defensive actions': 'DefensiveActions', 'Recoveries': 'Recoveries',
        'Dribbled past': 'DribbledPast', 'Duels won': 'DuelsWon', 'Duels lost': 'DuelsLost',
        'Ground duels won': 'GroundDuelsWon', 'Aerial duels won': 'AerialDuelsWon',
        'Was fouled': 'WasFouled', 'Fouls committed': 'FoulsCommitted'
    }
    
    # Aggregate stats for each player by taking the first non-null value for each stat type
    agg_exprs = []
    for json_name, db_name in stat_mapping.items():
        agg_exprs.append(
            first(
                when(col("stat_name") == json_name, col("stat_value_obj.stat.value")),
                ignorenulls=True
            ).alias(db_name)
        )

    grouped_df = stats_df.groupBy("MatchID", "PlayerID", "TeamID", "IsGoalkeeper", "FunFact", "ShotMapID").agg(*agg_exprs)
    
    # Ensure all columns from the DDL exist, adding nulls if they don't
    ddl_cols = [
        "PlayerID", "TeamID", "MatchID", "IsGoalkeeper", "FotmobRating", "MinutesPlayed", "GoalsScored",
        "Assists", "TotalShots", "AccuratePasses", "ChancesCreated", "ExpectedGoals", "ExpectedGoalsOnTarget",
        "ExpectedAssists", "xGandxA", "FantasyPoints", "FantasyBonusPoints", "ShotsOnTarget", "BigChancesMissed",
        "BlockedShots", "HitWoodwork", "Touches", "TouchesinOppBox", "SuccessfulDribbles",
        "PassesintoFinalThird", "Dispossessed", "xGNonPenalty", "TacklesWon", "Clearances", "HeadedClearances",
        "Interceptions", "DefensiveActions", "Recoveries", "DribbledPast", "DuelsWon", "DuelsLost",
        "GroundDuelsWon", "AerialDuelsWon", "WasFouled", "FoulsCommitted", "ShotMapID", "FunFact"
    ]

    final_df = grouped_df
    for col_name in ddl_cols:
        if col_name not in final_df.columns:
            final_df = final_df.withColumn(col_name, lit(None))
            
    final_df = final_df.select(ddl_cols).dropDuplicates(["MatchID", "TeamID", "PlayerID"])

    logger.info(f"Inserting {final_df.count()} player stats into player_stats_fact_stg table.")
    write_to_postgres(final_df, 'player_stats_fact_stg', postgres_args)
    logger.info(f"Inserted {final_df.count()} player stats into player_stats_fact_stg table.")

    
