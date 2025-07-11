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
        SeasonName = CASE
        WHEN EXTRACT(MONTH FROM stg.MatchTimeUTC) >= 8 THEN
            EXTRACT(YEAR FROM stg.MatchTimeUTC)::VARCHAR || '/' || (EXTRACT(YEAR FROM stg.MatchTimeUTC) + 1)::VARCHAR
        ELSE
            (EXTRACT(YEAR FROM stg.MatchTimeUTC) - 1)::VARCHAR || '/' || EXTRACT(YEAR FROM stg.MatchTimeUTC)::VARCHAR
    END,
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
        CASE
        WHEN EXTRACT(MONTH FROM stg.MatchTimeUTC) >= 8 THEN
            EXTRACT(YEAR FROM stg.MatchTimeUTC)::VARCHAR || '/' || (EXTRACT(YEAR FROM stg.MatchTimeUTC) + 1)::VARCHAR
        ELSE
            (EXTRACT(YEAR FROM stg.MatchTimeUTC) - 1)::VARCHAR || '/' || EXTRACT(YEAR FROM stg.MatchTimeUTC)::VARCHAR
    END,
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