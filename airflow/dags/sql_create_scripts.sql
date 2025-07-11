
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
    MatchCountryCode VARCHAR(10),
    PlayerOfTheMatchID BIGINT,
    Momentum TEXT,
    MatchQAQuestion TEXT,
    MatchQAAnswer TEXT
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
    MatchQAQuestion VARCHAR(1024),
    MatchQAAnswer VARCHAR(1024),
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

-- View for Shotmap data for Streamlit App
CREATE OR REPLACE VIEW vw_player_shotmap AS
SELECT
    shotmap.ShotMapID,
    shotmap.MatchID,
    match.MatchTimeUTC,
    match.MatchName,
    match.MatchRound,
    match.LeagueID,
    match.HomeTeamID AS MatchHomeTeam,
    match.AwayTeamID AS MatchAwayTeam,
    match.SeasonName,

    -- Player Info
    shotmap.PlayerID,
    'https://images.fotmob.com/image_resources/playerimages/' || shotmap.PlayerID || '.png' AS PlayerImageUrl,
    player.PlayerName,
    player.FirstName,
    player.LastName,

    -- Player's Team Info
    lineup.TeamID AS PlayerTeamID,
    player_team.TeamName AS PlayerTeamName,

    -- Opponent Team Info
    CASE
        WHEN lineup.TeamID = match.HomeTeamID THEN match.AwayTeamID
        ELSE match.HomeTeamID
    END AS OpponentTeamID,
    CASE
        WHEN lineup.TeamID = match.HomeTeamID THEN away_team.TeamName
        ELSE home_team.TeamName
    END AS OpponentTeamName,
    
    home_team.TeamName as HomeTeamName,
    home_team.imageurl AS MatchHomeImageUrl,
    home_team.pageurl AS MatchHomePageUrl,
    away_team.TeamName as AwayTeamName,
    away_team.imageurl AS MatchAwayImageUrl,
    away_team.pageurl AS MatchAwayPageUrl,
    match.HomeTeamID,
    match.AwayTeamID,

    -- Shot Info
    shotmap.EventType,
    shotmap.xPosition,
    shotmap.yPosition,
    shotmap.Minute,
    shotmap.ExpectedGoals,
    shotmap.ShotType,
    shotmap.Situation,
    shotmap.Period,
    shotmap.IsOnTarget,
    (LOWER(shotmap.EventType) = 'goal' AND shotmap.IsOwnGoal = FALSE) AS IsGoal,
    shotmap.IsOwnGoal

FROM
    player_shotmap_fact AS shotmap
JOIN player_dim AS player ON shotmap.PlayerID = player.PlayerID
JOIN match_lineup_fact AS lineup ON shotmap.PlayerID = lineup.PlayerID AND shotmap.MatchID = lineup.MatchID
JOIN team_dim AS player_team ON lineup.TeamID = player_team.TeamID
JOIN match_dim AS match ON shotmap.MatchID = match.MatchID
JOIN team_dim AS home_team ON match.HomeTeamID = home_team.TeamID
JOIN team_dim AS away_team ON match.AwayTeamID = away_team.TeamID;

-- View for comprehensive match facts
CREATE OR REPLACE VIEW vw_match_facts AS

-- CTE for aggregated player stats per team per match
WITH player_stats_agg AS (
    SELECT
        MatchID,
        TeamID,
        SUM(GoalsScored) AS TotalGoals,
        SUM(Assists) AS TotalAssists,
        SUM(TotalShots) AS TotalShots,
        SUM(ShotsOnTarget) AS TotalShotsOnTarget,
        SUM(AccuratePasses) AS TotalAccuratePasses,
        SUM(ChancesCreated) AS TotalChancesCreated,
        SUM(ExpectedGoals) AS TotalExpectedGoals,
        SUM(ExpectedGoalsOnTarget) AS TotalExpectedGoalsOnTarget,
        SUM(ExpectedAssists) AS TotalExpectedAssists,
        SUM(Touches) AS TotalTouches,
        SUM(TouchesinOppBox) AS TotalTouchesInOppBox,
        SUM(SuccessfulDribbles) AS TotalSuccessfulDribbles,
        SUM(PassesintoFinalThird) AS TotalPassesIntoFinalThird,
        SUM(TacklesWon) AS TotalTacklesWon,
        SUM(Clearances) AS TotalClearances,
        SUM(Interceptions) AS TotalInterceptions,
        SUM(Recoveries) AS TotalRecoveries,
        SUM(DuelsWon) AS TotalDuelsWon,
        SUM(DuelsLost) AS TotalDuelsLost,
        SUM(FoulsCommitted) AS TotalFoulsCommitted,
        SUM(WasFouled) AS TotalWasFouled,
        AVG(FotmobRating) AS AvgTeamRating
    FROM player_stats_fact
    GROUP BY MatchID, TeamID
),

-- CTE for lineups in JSON format
match_lineups AS (
    SELECT
        mlf.MatchID,
        mlf.TeamID,
        json_agg(
            json_build_object(
                'PlayerID', mlf.PlayerID,
                'PlayerName', p.PlayerName,
                'PlayerImageUrl', 'https://images.fotmob.com/image_resources/playerimages/' || mlf.PlayerID || '.png',
                'IsStarter', mlf.IsStarter,
                'IsCaptain', mlf.IsCaptain,
                'PositionID', mlf.PositionID,
                'ShirtNumber', mlf.ShirtNumber,
                'PlayerRating', mlf.PlayerRating
            ) ORDER BY mlf.IsStarter DESC, mlf.PositionID ASC
        ) AS Lineup
    FROM match_lineup_fact mlf
    JOIN player_dim p ON mlf.PlayerID = p.PlayerID
    GROUP BY mlf.MatchID, mlf.TeamID
)

SELECT
    m.MatchID,
    m.MatchName,
    m.MatchTimeUTC,
    m.LeagueID,
    l.LeagueName,
    m.SeasonName,
    m.MatchRound,
    m.StadiumName,
    m.Attendance,
    m.RefereeName,
    m.Momentum,
    m.MatchQAQuestion,
    m.MatchQAAnswer,
    
    -- Home Team Info
    m.HomeTeamID,
    ht.TeamName AS HomeTeamName,
    ht.ImageUrl AS HomeTeamImageUrl,
    
    -- Away Team Info
    m.AwayTeamID,
    at.TeamName AS AwayTeamName,
    at.ImageUrl AS AwayTeamImageUrl,

    -- Home Team Stats
    COALESCE(h_stats.TotalGoals, 0) AS HomeGoals,
    COALESCE(a_stats.TotalGoals, 0) AS AwayGoals,
    COALESCE(h_stats.TotalShots, 0) AS HomeShots,
    COALESCE(h_stats.TotalShotsOnTarget, 0) AS HomeShotsOnTarget,
    COALESCE(h_stats.TotalAccuratePasses, 0) AS HomeAccuratePasses,
    COALESCE(h_stats.TotalChancesCreated, 0) AS HomeChancesCreated,
    COALESCE(h_stats.TotalExpectedGoals, 0) AS HomeExpectedGoals,
    COALESCE(h_stats.TotalTouches, 0) AS HomeTouches,
    COALESCE(h_stats.TotalTacklesWon, 0) AS HomeTacklesWon,
    COALESCE(h_stats.AvgTeamRating, 0) AS HomeAvgTeamRating,

    -- Away Team Stats
    COALESCE(a_stats.TotalShots, 0) AS AwayShots,
    COALESCE(a_stats.TotalShotsOnTarget, 0) AS AwayShotsOnTarget,
    COALESCE(a_stats.TotalAccuratePasses, 0) AS AwayAccuratePasses,
    COALESCE(a_stats.TotalChancesCreated, 0) AS AwayChancesCreated,
    COALESCE(a_stats.TotalExpectedGoals, 0) AS AwayExpectedGoals,
    COALESCE(a_stats.TotalTouches, 0) AS AwayTouches,
    COALESCE(a_stats.TotalTacklesWon, 0) AS AwayTacklesWon,
    COALESCE(a_stats.AvgTeamRating, 0) AS AwayAvgTeamRating,
    
    -- Possession
    COALESCE((COALESCE(h_stats.TotalTouches, 0) * 100.0 / NULLIF(COALESCE(h_stats.TotalTouches, 0) + COALESCE(a_stats.TotalTouches, 0), 0)), 0) AS HomePossession,
    COALESCE((COALESCE(a_stats.TotalTouches, 0) * 100.0 / NULLIF(COALESCE(h_stats.TotalTouches, 0) + COALESCE(a_stats.TotalTouches, 0), 0)), 0) AS AwayPossession,

    -- Lineups
    h_lineup.Lineup AS HomeLineup,
    a_lineup.Lineup AS AwayLineup

FROM match_dim m
LEFT JOIN league_dim l ON m.LeagueID = l.LeagueID
LEFT JOIN team_dim ht ON m.HomeTeamID = ht.TeamID
LEFT JOIN team_dim at ON m.AwayTeamID = at.TeamID
LEFT JOIN player_stats_agg h_stats ON m.MatchID = h_stats.MatchID AND m.HomeTeamID = h_stats.TeamID
LEFT JOIN player_stats_agg a_stats ON m.MatchID = a_stats.MatchID AND m.AwayTeamID = a_stats.TeamID
LEFT JOIN match_lineups h_lineup ON m.MatchID = h_lineup.MatchID AND m.HomeTeamID = h_lineup.TeamID
LEFT JOIN match_lineups a_lineup ON m.MatchID = a_lineup.MatchID AND m.AwayTeamID = a_lineup.TeamID;

-- View for aggregated player analytics
CREATE OR REPLACE VIEW vw_player_analytics AS
SELECT
    p.PlayerID,
    p.PlayerName,
    p.FirstName,
    p.LastName,
    p.Age,
    p.CountryName,
    'https://images.fotmob.com/image_resources/playerimages/' || p.PlayerID || '.png' AS PlayerImageUrl,
    
    t.TeamID,
    t.TeamName,
    t.ImageUrl AS TeamImageUrl,
    
    -- Aggregated Stats
    COUNT(ps.MatchID) AS MatchesPlayed,
    COALESCE(SUM(ps.MinutesPlayed), 0) AS TotalMinutesPlayed,
    COALESCE(SUM(ps.GoalsScored), 0) AS TotalGoals,
    COALESCE(SUM(ps.Assists), 0) AS TotalAssists,
    COALESCE(SUM(ps.TotalShots), 0) AS TotalShots,
    COALESCE(SUM(ps.ShotsOnTarget), 0) AS TotalShotsOnTarget,
    COALESCE(SUM(ps.AccuratePasses), 0) AS TotalAccuratePasses,
    COALESCE(SUM(ps.ChancesCreated), 0) AS TotalChancesCreated,
    COALESCE(SUM(ps.SuccessfulDribbles), 0) AS TotalSuccessfulDribbles,
    COALESCE(SUM(ps.TacklesWon), 0) AS TotalTacklesWon,
    COALESCE(SUM(ps.Interceptions), 0) AS TotalInterceptions,
    COALESCE(SUM(ps.Clearances), 0) AS TotalClearances,
    COALESCE(SUM(ps.Recoveries), 0) AS TotalRecoveries,
    COALESCE(SUM(ps.DuelsWon), 0) AS TotalDuelsWon,
    COALESCE(SUM(ps.WasFouled), 0) AS TotalWasFouled,
    COALESCE(SUM(ps.FoulsCommitted), 0) AS TotalFoulsCommitted,
    COALESCE(AVG(ps.FotmobRating), 0) AS AvgRating,

    -- Advanced Metrics
    COALESCE(SUM(ps.ExpectedGoals), 0) AS TotalExpectedGoals,
    COALESCE(SUM(ps.ExpectedGoalsOnTarget), 0) AS TotalExpectedGoalsOnTarget,
    COALESCE(SUM(ps.ExpectedAssists), 0) AS TotalExpectedAssists,
    COALESCE(SUM(ps.xGandxA), 0) AS TotalxGandxA,
    COALESCE(SUM(ps.FantasyPoints), 0) AS TotalFantasyPoints,

    -- Per 90 Stats
    COALESCE(SUM(ps.GoalsScored) * 90.0 / NULLIF(SUM(ps.MinutesPlayed), 0), 0) AS GoalsPer90,
    COALESCE(SUM(ps.Assists) * 90.0 / NULLIF(SUM(ps.MinutesPlayed), 0), 0) AS AssistsPer90,
    COALESCE(SUM(ps.TotalShots) * 90.0 / NULLIF(SUM(ps.MinutesPlayed), 0), 0) AS ShotsPer90,
    COALESCE(SUM(ps.Touches) * 90.0 / NULLIF(SUM(ps.MinutesPlayed), 0), 0) AS TouchesPer90,
    COALESCE(SUM(ps.ChancesCreated) * 90.0 / NULLIF(SUM(ps.MinutesPlayed), 0), 0) AS ChancesCreatedPer90,
    COALESCE(SUM(ps.TacklesWon) * 90.0 / NULLIF(SUM(ps.MinutesPlayed), 0), 0) AS TacklesWonPer90,
    COALESCE(SUM(ps.ExpectedGoals) * 90.0 / NULLIF(SUM(ps.MinutesPlayed), 0), 0) AS ExpectedGoalsPer90,
    COALESCE(SUM(ps.ExpectedAssists) * 90.0 / NULLIF(SUM(ps.MinutesPlayed), 0), 0) AS ExpectedAssistsPer90


FROM player_stats_fact ps
JOIN player_dim p ON ps.PlayerID = p.PlayerID
JOIN team_dim t ON ps.TeamID = t.TeamID
GROUP BY
    p.PlayerID,
    p.PlayerName,
    p.FirstName,
    p.LastName,
    p.Age,
    p.CountryName,
    t.TeamID,
    t.TeamName,
    t.ImageUrl;

-- View for seasonal player analytics
CREATE OR REPLACE VIEW vw_player_seasonal_analytics AS
SELECT
    p.PlayerID,
    p.PlayerName,
    p.FirstName,
    p.LastName,
    p.Age,
    p.CountryName,
    'https://images.fotmob.com/image_resources/playerimages/' || p.PlayerID || '.png' AS PlayerImageUrl,
    
    t.TeamID,
    t.TeamName,
    t.ImageUrl AS TeamImageUrl,

    m.SeasonName,
    
    -- Aggregated Stats
    COUNT(ps.MatchID) AS MatchesPlayed,
    COALESCE(SUM(ps.MinutesPlayed), 0) AS TotalMinutesPlayed,
    COALESCE(SUM(ps.GoalsScored), 0) AS TotalGoals,
    COALESCE(SUM(ps.Assists), 0) AS TotalAssists,
    COALESCE(SUM(ps.TotalShots), 0) AS TotalShots,
    COALESCE(SUM(ps.ShotsOnTarget), 0) AS TotalShotsOnTarget,
    COALESCE(SUM(ps.AccuratePasses), 0) AS TotalAccuratePasses,
    COALESCE(SUM(ps.ChancesCreated), 0) AS TotalChancesCreated,
    COALESCE(SUM(ps.SuccessfulDribbles), 0) AS TotalSuccessfulDribbles,
    COALESCE(SUM(ps.TacklesWon), 0) AS TotalTacklesWon,
    COALESCE(SUM(ps.Interceptions), 0) AS TotalInterceptions,
    COALESCE(SUM(ps.Clearances), 0) AS TotalClearances,
    COALESCE(SUM(ps.Recoveries), 0) AS TotalRecoveries,
    COALESCE(SUM(ps.DuelsWon), 0) AS TotalDuelsWon,
    COALESCE(SUM(ps.WasFouled), 0) AS TotalWasFouled,
    COALESCE(SUM(ps.FoulsCommitted), 0) AS TotalFoulsCommitted,
    COALESCE(AVG(ps.FotmobRating), 0) AS AvgRating,

    -- Advanced Metrics
    COALESCE(SUM(ps.ExpectedGoals), 0) AS TotalExpectedGoals,
    COALESCE(SUM(ps.ExpectedGoalsOnTarget), 0) AS TotalExpectedGoalsOnTarget,
    COALESCE(SUM(ps.ExpectedAssists), 0) AS TotalExpectedAssists,
    COALESCE(SUM(ps.xGandxA), 0) AS TotalxGandxA,
    COALESCE(SUM(ps.FantasyPoints), 0) AS TotalFantasyPoints,
    
    -- Per 90 Stats
    COALESCE(SUM(ps.GoalsScored) * 90.0 / NULLIF(SUM(ps.MinutesPlayed), 0), 0) AS GoalsPer90,
    COALESCE(SUM(ps.Assists) * 90.0 / NULLIF(SUM(ps.MinutesPlayed), 0), 0) AS AssistsPer90,
    COALESCE(SUM(ps.TotalShots) * 90.0 / NULLIF(SUM(ps.MinutesPlayed), 0), 0) AS ShotsPer90,
    COALESCE(SUM(ps.Touches) * 90.0 / NULLIF(SUM(ps.MinutesPlayed), 0), 0) AS TouchesPer90,
    COALESCE(SUM(ps.ChancesCreated) * 90.0 / NULLIF(SUM(ps.MinutesPlayed), 0), 0) AS ChancesCreatedPer90,
    COALESCE(SUM(ps.TacklesWon) * 90.0 / NULLIF(SUM(ps.MinutesPlayed), 0), 0) AS TacklesWonPer90,
    COALESCE(SUM(ps.ExpectedGoals) * 90.0 / NULLIF(SUM(ps.MinutesPlayed), 0), 0) AS ExpectedGoalsPer90,
    COALESCE(SUM(ps.ExpectedAssists) * 90.0 / NULLIF(SUM(ps.MinutesPlayed), 0), 0) AS ExpectedAssistsPer90

FROM player_stats_fact ps
JOIN player_dim p ON ps.PlayerID = p.PlayerID
JOIN team_dim t ON ps.TeamID = t.TeamID
JOIN match_dim m ON ps.MatchID = m.MatchID
GROUP BY
    p.PlayerID,
    p.PlayerName,
    p.FirstName,
    p.LastName,
    p.Age,
    p.CountryName,
    t.TeamID,
    t.TeamName,
    t.ImageUrl,
    m.SeasonName;

-- View for detailed player-match analytics with date dimension
CREATE OR REPLACE VIEW vw_player_match_analytics AS
SELECT
    -- Match Info
    m.MatchID,
    m.MatchName,
    m.MatchTimeUTC,
    m.LeagueID,
    m.SeasonName,
    m.MatchRound,

    -- Player Info
    p.PlayerID,
    p.PlayerName,
    p.FirstName,
    p.LastName,
    'https://images.fotmob.com/image_resources/playerimages/' || p.PlayerID || '.png' AS PlayerImageUrl,

    -- Team Info
    t.TeamID,
    t.TeamName,
    t.ImageUrl AS TeamImageUrl,

    -- Player Stats from Fact Table
    ps.IsGoalkeeper,
    ps.FotmobRating,
    ps.MinutesPlayed,
    ps.GoalsScored,
    ps.Assists,
    ps.TotalShots,
    ps.AccuratePasses,
    ps.ChancesCreated,
    ps.ExpectedGoals,
    ps.ExpectedGoalsOnTarget,
    ps.ExpectedAssists,
    ps.xGandxA,
    ps.FantasyPoints,
    ps.FantasyBonusPoints,
    ps.ShotsOnTarget,
    ps.BigChancesMissed,
    ps.BlockedShots,
    ps.HitWoodwork,
    ps.Touches,
    ps.TouchesinOppBox,
    ps.SuccessfulDribbles,
    ps.PassesintoFinalThird,
    ps.Dispossessed,
    ps.xGNonPenalty,
    ps.TacklesWon,
    ps.Clearances,
    ps.HeadedClearances,
    ps.Interceptions,
    ps.DefensiveActions,
    ps.Recoveries,
    ps.DribbledPast,
    ps.DuelsWon,
    ps.DuelsLost,
    ps.GroundDuelsWon,
    ps.AerialDuelsWon,
    ps.WasFouled,
    ps.FoulsCommitted,
    ps.ShotMapID,
    ps.FunFact,

    -- Date Dimension
    dd.DateID,
    dd.FullDate,
    dd.Year,
    dd.Month,
    dd.Day,
    dd.DayName,
    dd.Weekday,
    dd.Quarter,
    dd.MonthName,
    dd.LeapYear,
    dd.DayOfYear,
    dd.DayOfWeek,
    dd.Week

FROM player_stats_fact ps
JOIN player_dim p ON ps.PlayerID = p.PlayerID
JOIN team_dim t ON ps.TeamID = t.TeamID
JOIN match_dim m ON ps.MatchID = m.MatchID
LEFT JOIN date_dim dd ON m.MatchTimeUTC::DATE = dd.FullDate;





