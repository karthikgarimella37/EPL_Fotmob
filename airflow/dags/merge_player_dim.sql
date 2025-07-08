MERGE INTO player_dim dim
USING player_dim_stg stg
ON dim.PlayerID = stg.PlayerID
WHEN MATCHED THEN
    UPDATE SET
        PlayerName = stg.PlayerName,
        FirstName = stg.FirstName,
        LastName = stg.LastName,
        Age = stg.Age,
        CountryName = stg.CountryName,
        CountryCode = stg.CountryCode,
        ProfileUrl = stg.ProfileUrl,
        UpdateDate = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        PlayerID,
        PlayerName,
        FirstName,
        LastName,
        Age,
        CountryName,
        CountryCode,
        ProfileUrl,
        InsertDate,
        UpdateDate
    ) VALUES (
        stg.PlayerID,
        stg.PlayerName,
        stg.FirstName,
        stg.LastName,
        stg.Age,
        stg.CountryName,
        stg.CountryCode,
        stg.ProfileUrl,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    ); 