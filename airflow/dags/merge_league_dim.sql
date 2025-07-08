MERGE INTO league_dim dim
USING league_dim_stg stg
ON dim.LeagueID = stg.LeagueID
WHEN MATCHED THEN
    UPDATE SET
        LeagueName = stg.LeagueName,
        ParentLeagueID = stg.ParentLeagueID,
        ParentLeagueName = stg.ParentLeagueName,
        CountryCode = stg.CountryCode,
        UpdateDate = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        LeagueID,
        LeagueName,
        ParentLeagueID,
        ParentLeagueName,
        CountryCode,
        InsertDate,
        UpdateDate
    ) VALUES (
        stg.LeagueID,
        stg.LeagueName,
        stg.ParentLeagueID,
        stg.ParentLeagueName,
        stg.CountryCode,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    ); 