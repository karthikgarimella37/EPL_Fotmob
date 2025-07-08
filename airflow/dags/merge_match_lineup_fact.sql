MERGE INTO match_lineup_fact fact
USING match_lineup_fact_stg stg
ON fact.MatchID = stg.MatchID AND fact.TeamID = stg.TeamID AND fact.PlayerID = stg.PlayerID
WHEN MATCHED THEN
    UPDATE SET
        IsStarter = stg.IsStarter,
        PositionID = stg.PositionID,
        ShirtNumber = stg.ShirtNumber,
        PlayerRating = stg.PlayerRating,
        PlayerOfTheMatch = stg.PlayerOfTheMatch,
        IsCaptain = stg.IsCaptain,
        UpdateDate = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        MatchID,
        TeamID,
        PlayerID,
        IsStarter,
        PositionID,
        ShirtNumber,
        PlayerRating,
        PlayerOfTheMatch,
        IsCaptain,
        InsertDate,
        UpdateDate
    ) VALUES (
        stg.MatchID,
        stg.TeamID,
        stg.PlayerID,
        stg.IsStarter,
        stg.PositionID,
        stg.ShirtNumber,
        stg.PlayerRating,
        stg.PlayerOfTheMatch,
        stg.IsCaptain,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    ); 