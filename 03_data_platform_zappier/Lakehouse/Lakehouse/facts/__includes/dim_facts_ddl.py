# Databricks notebook source
# Create a text widget for the API key
dbutils.widgets.text("assignment_database", "", "Enter Assignment Database name")

# Retrieve the API key from the widget
assignment_database = dbutils.widgets.get("assignment_database")

# Create a text widget for the API key
dbutils.widgets.text("assignment_schema", "", "Enter Assignment Schema name")

# Retrieve the API key from the widget
assignment_schema = dbutils.widgets.get("assignment_schema")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {assignment_database}.{assignment_schema}.dim_teams (
    team_id INT COMMENT 'Unique identifier for the team',
    team_name STRING COMMENT 'Full name of the team',
    team_short_name STRING COMMENT 'Short name of the team',
    team_tla STRING COMMENT 'Three-letter acronym for the team',
    team_crest STRING COMMENT 'URL to the crest image of the team',
    team_address STRING COMMENT 'Address of the team',
    team_website STRING COMMENT 'Official website of the team',
    team_founded INT COMMENT 'Year the team was founded',
    team_club_colors STRING COMMENT 'Club colors of the team',
    team_venue STRING COMMENT 'Home venue of the team'
)
USING DELTA
COMMENT 'Dimension table containing information about teams.';
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {assignment_database}.{assignment_schema}.dim_competitions (
    competition_id INT COMMENT 'Unique identifier for the competition',
    competition_name STRING COMMENT 'Official name of the competition',
    competition_code STRING COMMENT 'Abbreviated code for the competition',
    competition_type STRING COMMENT 'Type of competition (e.g., LEAGUE, CUP)',
    area_id INT COMMENT 'Unique identifier for the area (e.g., country) where the competition is held',
    area_name STRING COMMENT 'Name of the area (e.g., country) where the competition is held',
    area_code STRING COMMENT 'Code representing the area',
    area_flag STRING COMMENT 'URL to the flag image of the area'
)
USING DELTA
PARTITIONED BY (competition_type)
COMMENT 'Dimension table containing competition details.';
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {assignment_database}.{assignment_schema}.fact_team_participation_timeseries (
    competition_id INT COMMENT 'Unique identifier for the competition',
    competition_name STRING COMMENT 'Name of the competition (e.g., Premier League)',
    season_id INT COMMENT 'Unique identifier for the season',
    matchday INT COMMENT 'The matchday number in the season',
    team_id INT COMMENT 'Unique identifier for the team',
    team_name STRING COMMENT 'Full name of the team',
    coach_name STRING COMMENT 'Name of the team coach for the specific matchday',
    coach_nationality STRING COMMENT 'Nationality of the team coach for the specific matchday',
    venue STRING COMMENT 'Venue where the team plays its home matches',
    total_matchdays INT COMMENT 'Total number of distinct matchdays the team participated in during the season',
    total_participations INT COMMENT 'Total count of participations by the team in the season',
    unique_venues INT COMMENT 'Number of unique venues used by the team during the season',
    total_teams INT COMMENT 'Total number of teams participating in the competition during the season',
    first_match_date DATE COMMENT 'Start date of the season (earliest match date)',
    last_match_date DATE COMMENT 'End date of the season (latest match date)'
)
USING DELTA
PARTITIONED BY (competition_id, season_id)
COMMENT 'Fact table capturing team participation data at a matchday level, with aggregated metrics at team and season levels.';
""")

# COMMAND ----------

