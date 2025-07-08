import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from create_sql import postgres_credentials, postgres_connection

def merge_dim_fact(engine):
    with engine.connect() as connection:
        with connection.begin():
            connection.execute(text('''\
                MERGE INTO match_dim dim
USING match_dim_stg stg
ON dim.MatchID = stg.MatchID
WHEN MATCHED THEN
    UPDATE SET
        MatchName = stg.MatchName,
        MatchTimeUTC = stg.MatchTimeUTC,
        MatchRound = stg.MatchRound,
        LeagueID = stg.LeagueID,
        HomeTeamID = stg.HomeTeamID,
        AwayTeamID = stg.AwayTeamID,
        SeasonName = stg.SeasonName,
        StadiumName = stg.StadiumName,
        Attendance = stg.Attendance,
        RefereeName = stg.RefereeName,
        MatchLatitude = stg.MatchLatitude,
        MatchLongitude = stg.MatchLongitude,
        MatchHighlightsUrl = stg.MatchHighlightsUrl,
        MatchCountryCode = stg.MatchCountryCode,
        PlayerOfTheMatchID = stg.PlayerOfTheMatchID,
        Momentum = stg.Momentum,
        MatchQAQuestion = stg.MatchQAQuestion,
        MatchQAAnswer = stg.MatchQAAnswer,
        UpdateDate = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        MatchID,
        MatchName,
        MatchTimeUTC,
        MatchRound,
        LeagueID,
        HomeTeamID,
        AwayTeamID,
        SeasonName,
        StadiumName,
        Attendance,
        RefereeName,
        MatchLatitude,
        MatchLongitude,
        MatchHighlightsUrl,
        MatchCountryCode,
        PlayerOfTheMatchID,
        Momentum,
        MatchQAQuestion,
        MatchQAAnswer,
        InsertDate,
        UpdateDate
    ) VALUES (
        stg.MatchID,
        stg.MatchName,
        stg.MatchTimeUTC,
        stg.MatchRound,
        stg.LeagueID,
        stg.HomeTeamID,
        stg.AwayTeamID,
        stg.SeasonName,
        stg.StadiumName,
        stg.Attendance,
        stg.RefereeName,
        stg.MatchLatitude,
        stg.MatchLongitude,
        stg.MatchHighlightsUrl,
        stg.MatchCountryCode,
        stg.PlayerOfTheMatchID,
        stg.Momentum,
        stg.MatchQAQuestion,
        stg.MatchQAAnswer,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    ); 
                '''))
    print("Merged Team Dim")

if __name__ == "__main__":
    file_path = os.path.join(os.path.dirname(__file__), '../.env')
    engine = postgres_connection(file_path)
    merge_dim_fact(engine)

