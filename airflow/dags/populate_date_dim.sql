-- This script populates the date_dim table with a complete set of dates
-- from January 1, 2010, to December 31, 2050.

TRUNCATE TABLE date_dim RESTART IDENTITY;

INSERT INTO date_dim (
    DateID,
    FullDate,
    Year,
    Month,
    Day,
    DayName,
    Weekday,
    Quarter,
    MonthName,
    LeapYear,
    DayOfYear,
    DayOfWeek,
    Week,
    InsertDate,
    UpdateDate
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT AS DateID,
    d::DATE AS FullDate,
    EXTRACT(YEAR FROM d) AS Year,
    EXTRACT(MONTH FROM d) AS Month,
    EXTRACT(DAY FROM d) AS Day,
    TO_CHAR(d, 'FMDay') AS DayName,
    CASE
        WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS Weekday,
    EXTRACT(QUARTER FROM d) AS Quarter,
    TO_CHAR(d, 'FMMonth') AS MonthName,
    (EXTRACT(YEAR FROM d) % 4 = 0 AND (EXTRACT(YEAR FROM d) % 100 != 0 OR EXTRACT(YEAR FROM d) % 400 = 0)) AS LeapYear,
    EXTRACT(DOY FROM d) AS DayOfYear,
    EXTRACT(ISODOW FROM d) AS DayOfWeek, -- Monday=1, Sunday=7
    EXTRACT(WEEK FROM d) AS Week,
    CURRENT_TIMESTAMP AS InsertDate,
    CURRENT_TIMESTAMP AS UpdateDate
FROM
    generate_series('2010-01-01'::DATE, '2050-12-31'::DATE, '1 day'::interval) d
ON CONFLICT (DateID) DO NOTHING; 