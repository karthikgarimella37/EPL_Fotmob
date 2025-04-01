# sql connections
# create db if not exists
#create tables if not exists
# try to incorporate terraform
# #create sequences for dim and fact tables



'''
CREATE DATABASE IF NOT EXISTS EPL_DB;



# DIM and FACT STG Tables
CREATE TABLE IF NOT EXISTS DimTeam (
    TeamID INT PRIMARY KEY,
    TeamName VARCHAR(255),
    TeamColorHome VARCHAR(50),
    TeamColorAway VARCHAR(50),
    TeamFontColorHome VARCHAR(50),
    TeamFontColorAway VARCHAR(50)
);

CREATE TABLE DimPlayer (
    PlayerID INT PRIMARY KEY,
    PlayerName VARCHAR(255),
    Position VARCHAR(50),
    Nationality VARCHAR(50),
    Birthdate DATE,
    TeamID INT,
    FOREIGN KEY (TeamID) REFERENCES DimTeam(TeamID)
);

CREATE TABLE DimMatch (
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
    DateID INT PRIMARY KEY,
    FullDate DATE UNIQUE,
    Year INT,
    Month INT,
    Day INT,
    Weekday VARCHAR(20),
    Quarter INT
);

CREATE TABLE DimLeague (
    LeagueID INT PRIMARY KEY,
    LeagueName VARCHAR(255),
    ParentLeagueID INT,
    ParentLeagueName VARCHAR(255),
    CountryCode VARCHAR(10),
    TournamentID INT,
    SeasonName VARCHAR(50)
);

CREATE TABLE DimEventType (
    EventTypeID INT PRIMARY KEY,
    EventTypeName VARCHAR(50)
);

CREATE TABLE FactPlayerMatch (
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
'''

'''
# DIM and FACT Tables
CREATE TABLE IF NOT EXISTS DimTeam (
    TeamID INT PRIMARY KEY,
    TeamName VARCHAR(255),
    TeamColorHome VARCHAR(50),
    TeamColorAway VARCHAR(50),
    TeamFontColorHome VARCHAR(50),
    TeamFontColorAway VARCHAR(50)
);

CREATE TABLE DimPlayer (
    PlayerID INT PRIMARY KEY,
    PlayerName VARCHAR(255),
    Position VARCHAR(50),
    Nationality VARCHAR(50),
    Birthdate DATE,
    TeamID INT,
    FOREIGN KEY (TeamID) REFERENCES DimTeam(TeamID)
);

CREATE TABLE DimMatch (
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
    DateID INT PRIMARY KEY,
    FullDate DATE UNIQUE,
    Year INT,
    Month INT,
    Day INT,
    Weekday VARCHAR(20),
    Quarter INT
);

CREATE TABLE DimLeague (
    LeagueID INT PRIMARY KEY,
    LeagueName VARCHAR(255),
    ParentLeagueID INT,
    ParentLeagueName VARCHAR(255),
    CountryCode VARCHAR(10),
    TournamentID INT,
    SeasonName VARCHAR(50)
);

CREATE TABLE DimEventType (
    EventTypeID INT PRIMARY KEY,
    EventTypeName VARCHAR(50)
);

CREATE TABLE FactPlayerMatch (
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

# Create Sequences for all DIM and FACT Tables
'''