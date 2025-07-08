MERGE INTO team_dim dim
USING team_dim_stg stg
ON dim.TeamID = stg.TeamID
WHEN MATCHED THEN
    UPDATE SET
        TeamName = stg.TeamName,
        ImageUrl = stg.ImageUrl,
        PageUrl = stg.PageUrl,
        TeamColor = stg.TeamColor,
        LeagueID = stg.LeagueID,
        UpdateDate = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        TeamID,
        TeamName,
        ImageUrl,
        PageUrl,
        TeamColor,
        LeagueID,
        InsertDate,
        UpdateDate
    ) VALUES (
        stg.TeamID,
        stg.TeamName,
        stg.ImageUrl,
        stg.PageUrl,
        stg.TeamColor,
        stg.LeagueID,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    ); 