# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Football Dimensions Tables
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC | Description     | Details                                                         |
# MAGIC |-----------------|-----------------------------------------------------------------|
# MAGIC | Author          | Dmitry Anoshin                                                 |
# MAGIC | Source          | API: https://api.football-data.org/v4/competitions/            |
# MAGIC | Database        | `interview_data_de`                                              |
# MAGIC | Schema          | `dmitryanoshin_gmailcom`                                         |
# MAGIC | Facts    | `fact_team_participation_timeseries`                                              |
# MAGIC
# MAGIC ## History
# MAGIC
# MAGIC | Date       | Developed by    | Changes              |
# MAGIC |------------|-----------------|----------------------|
# MAGIC | 2024-12-14 | Dmitry Anoshin  | Initial Pipeline     |
# MAGIC
# MAGIC ## Other Details
# MAGIC
# MAGIC This notebook doesn't explicitly use any `import` statement. Packages and functions are loaded via external notebooks.
# MAGIC

# COMMAND ----------

# MAGIC %run ../__includes/utils

# COMMAND ----------

# MAGIC %run ./__includes/dim_facts_ddl

# COMMAND ----------

# Create a text widget for the API key
dbutils.widgets.text("assignment_database", "", "Enter Assignment Database name")

# Retrieve the API key from the widget
assignment_database = dbutils.widgets.get("assignment_database")

# Create a text widget for the API key
dbutils.widgets.text("assignment_schema", "", "Enter Assignment Schema name")

# Retrieve the API key from the widget
assignment_schema = dbutils.widgets.get("assignment_schema")

# COMMAND ----------

logger = logging.getLogger("Fact tables pipeline")

spark.sql(f"TRUNCATE TABLE {assignment_database}.{assignment_schema}.fact_team_participation_timeseries")

logger.info(f"TRUNCATE TABLE {assignment_database}.{assignment_schema}.fact_team_participation_timeseries successfully")

spark.sql(f"""
INSERT INTO {assignment_database}.{assignment_schema}.fact_team_participation_timeseries         
WITH team_participation_cte AS (
    -- Base data for team participation
    SELECT 
        rct.competition_id,
        rc.competition_name,
        rct.season_id,
        rct.current_matchday AS matchday,
        rct.team_id,
        rt.team_name,
        rct.coach_name,
        rct.coach_nationality,
        rct.venue,
        rc.season_start_date,
        rc.season_end_date
    FROM {assignment_database}.{assignment_schema}.raw_competitions_teams rct
    JOIN {assignment_database}.{assignment_schema}.raw_competitions rc
        ON rct.competition_id = rc.competition_id
    JOIN {assignment_database}.{assignment_schema}.raw_teams rt
        ON rct.team_id = rt.team_id
),
team_metrics_cte AS (
    -- Aggregations at the team level across matchdays
    SELECT
        competition_id,
        season_id,
        team_id,
        COUNT(DISTINCT matchday) AS total_matchdays,
        COUNT(team_id) AS total_participations,
        COUNT(DISTINCT venue) AS unique_venues,
        MAX(season_end_date) AS last_season_date
    FROM team_participation_cte
    GROUP BY competition_id, season_id, team_id
),
season_metrics_cte AS (
    -- Aggregations at the season level
    SELECT
        competition_id,
        season_id,
        COUNT(DISTINCT team_id) AS total_teams,
        MAX(season_start_date) AS first_match_date,
        MAX(season_end_date) AS last_match_date
    FROM team_participation_cte
    GROUP BY competition_id, season_id
)
-- Final Fact Table
SELECT 
    tp.competition_id,
    tp.competition_name,
    tp.season_id,
    tp.matchday,
    tp.team_id,
    tp.team_name,
    tp.coach_name,
    tp.coach_nationality,
    tp.venue,
    tm.total_matchdays,
    tm.total_participations,
    tm.unique_venues,
    sm.total_teams,
    sm.first_match_date,
    sm.last_match_date
FROM team_participation_cte tp
LEFT JOIN team_metrics_cte tm
    ON tp.competition_id = tm.competition_id 
    AND tp.season_id = tm.season_id 
    AND tp.team_id = tm.team_id
LEFT JOIN season_metrics_cte sm
    ON tp.competition_id = sm.competition_id 
    AND tp.season_id = sm.season_id;
""")

logger.info(f"Inserted data into {assignment_database}.{assignment_schema}.fact_team_participation_timeseries successfully")


# COMMAND ----------

result = spark.sql(f"""
WITH matchday_gaps AS (
    SELECT 
        competition_id,
        season_id,
        team_id,
        matchday,
        LAG(matchday) OVER (PARTITION BY competition_id, season_id, team_id ORDER BY matchday) AS previous_matchday
    FROM {assignment_database}.{assignment_schema}.fact_team_participation_timeseries
)
SELECT *
FROM matchday_gaps
WHERE matchday - previous_matchday > 1
""")

# Assert that the result is empty
if result.count() > 0:
    raise AssertionError("Matchday gaps found in fact_team_participation_timeseries. Investigate missing matchdays.")
else:
    print("Assertion passed: No matchday gaps in fact_team_participation_timeseries.")

# COMMAND ----------

# Run the Spark SQL query to check for bad data
result = spark.sql(f"""
SELECT *
FROM {assignment_database}.{assignment_schema}.fact_team_participation_timeseries
WHERE 
    competition_id IS NULL OR
    season_id IS NULL OR
    matchday IS NULL OR
    team_id IS NULL OR
    team_name IS NULL OR
    total_matchdays < 0 OR
    total_participations < 0 OR
    unique_venues < 0 OR
    total_teams < 0 OR
    first_match_date > last_match_date
""")

# Assert that no rows violate the data quality rules
if result.count() > 0:
    print("Invalid data detected:")
    result.show()  # Show the invalid rows for debugging
    raise AssertionError("Invalid or bad data found in fact_team_participation_timeseries. Check NULL values, negative values, or date inconsistencies.")
else:
    print("Assertion passed: No NULL or invalid data in fact_team_participation_timeseries.")