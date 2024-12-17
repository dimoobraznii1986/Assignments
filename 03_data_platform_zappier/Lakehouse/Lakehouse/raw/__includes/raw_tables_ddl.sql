-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC This notbook creates table for RAW tables if they aren't exist.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Create a text widget for the API key
-- MAGIC dbutils.widgets.text("assignment_database", "", "Enter Assignment Database name")
-- MAGIC
-- MAGIC # Retrieve the API key from the widget
-- MAGIC assignment_database = dbutils.widgets.get("assignment_database")
-- MAGIC
-- MAGIC # Create a text widget for the API key
-- MAGIC dbutils.widgets.text("assignment_schema", "", "Enter Assignment Schema name")
-- MAGIC
-- MAGIC # Retrieve the API key from the widget
-- MAGIC assignment_schema = dbutils.widgets.get("assignment_schema")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC raw_competitions_query = f"""
-- MAGIC CREATE TABLE IF NOT EXISTS {assignment_database}.{assignment_schema}.raw_competitions (
-- MAGIC     competition_id INT COMMENT 'Unique identifier for the competition',
-- MAGIC     area_id INT COMMENT 'Unique identifier for the area (e.g., country) where the competition is held',
-- MAGIC     area_name STRING COMMENT 'Name of the area (e.g., country) where the competition is held',
-- MAGIC     area_code STRING COMMENT 'Code representing the area',
-- MAGIC     area_flag STRING COMMENT 'URL to the flag image of the area',
-- MAGIC     competition_name STRING COMMENT 'Official name of the competition',
-- MAGIC     competition_code STRING COMMENT 'Abbreviated code for the competition',
-- MAGIC     competition_type STRING COMMENT 'Type of competition (e.g., LEAGUE, CUP)',
-- MAGIC     competition_emblem STRING COMMENT 'URL to the emblem image of the competition',
-- MAGIC     competition_plan STRING COMMENT 'Plan or tier of the competition',
-- MAGIC     season_id INT COMMENT 'Unique identifier for the current season of the competition',
-- MAGIC     season_start_date DATE COMMENT 'Start date of the current season',
-- MAGIC     season_end_date DATE COMMENT 'End date of the current season',
-- MAGIC     season_current_matchday INT COMMENT 'Current matchday number in the season',
-- MAGIC     winner_id INT COMMENT 'Unique identifier for the winning team of the season, if available',
-- MAGIC     winner_name STRING COMMENT 'Name of the winning team of the season, if available',
-- MAGIC     winner_short_name STRING COMMENT 'Short name of the winning team',
-- MAGIC     winner_tla STRING COMMENT 'Three-letter acronym of the winning team',
-- MAGIC     winner_crest STRING COMMENT 'URL to the crest image of the winning team',
-- MAGIC     winner_address STRING COMMENT 'Address of the winning team',
-- MAGIC     winner_website STRING COMMENT 'Official website of the winning team',
-- MAGIC     winner_founded INT COMMENT 'Year the winning team was founded',
-- MAGIC     winner_club_colors STRING COMMENT 'Club colors of the winning team',
-- MAGIC     winner_venue STRING COMMENT 'Home venue of the winning team',
-- MAGIC     winner_last_updated TIMESTAMP COMMENT 'Timestamp of the last update for the winning team information',
-- MAGIC     number_of_available_seasons INT COMMENT 'Total number of seasons available for the competition',
-- MAGIC     last_updated TIMESTAMP COMMENT 'Timestamp of the last update for the competition information',
-- MAGIC     etl_timestamp_utc TIMESTAMP NOT NULL COMMENT 'Timestamp in UTC when the data was extracted'
-- MAGIC )
-- MAGIC USING DELTA
-- MAGIC PARTITIONED BY (season_start_date)
-- MAGIC COMMENT 'This table contains raw competition data extracted from the Football Data API.';
-- MAGIC """
-- MAGIC
-- MAGIC spark.sql(raw_competitions_query)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC raw_teams_query = f"""
-- MAGIC CREATE TABLE IF NOT EXISTS {assignment_database}.{assignment_schema}.raw_teams (
-- MAGIC     team_id INT COMMENT 'Unique identifier for the team',
-- MAGIC     team_name STRING COMMENT 'Official name of the team',
-- MAGIC     team_short_name STRING COMMENT 'Short name of the team',
-- MAGIC     team_tla STRING COMMENT 'Three-letter acronym of the team',
-- MAGIC     team_crest STRING COMMENT 'URL to the crest image of the team',
-- MAGIC     team_address STRING COMMENT 'Official address of the team',
-- MAGIC     team_website STRING COMMENT 'Official website of the team',
-- MAGIC     team_founded INT COMMENT 'Year the team was founded',
-- MAGIC     team_club_colors STRING COMMENT 'Club colors of the team',
-- MAGIC     team_venue STRING COMMENT 'Home venue of the team',
-- MAGIC     team_last_updated TIMESTAMP COMMENT 'Timestamp of the last update for the team',
-- MAGIC     etl_timestamp_utc TIMESTAMP NOT NULL COMMENT 'Timestamp in UTC when the data was extracted'
-- MAGIC )
-- MAGIC USING DELTA
-- MAGIC COMMENT 'This table contains raw team data extracted from the Football Data API.';
-- MAGIC """
-- MAGIC
-- MAGIC spark.sql(raw_teams_query)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC raw_competitions_teams_query = f"""
-- MAGIC CREATE TABLE IF NOT EXISTS {assignment_database}.{assignment_schema}.raw_competitions_teams (
-- MAGIC     season_id INT COMMENT 'Unique identifier for the season',
-- MAGIC     team_id INT COMMENT 'Unique identifier for the team',
-- MAGIC     team_name STRING COMMENT 'Full name of the team',
-- MAGIC     short_name STRING COMMENT 'Short name of the team',
-- MAGIC     team_tla STRING COMMENT 'Three-letter abbreviation for the team',
-- MAGIC     crest STRING COMMENT 'URL for the team crest image',
-- MAGIC     address STRING COMMENT 'Address of the team',
-- MAGIC     website STRING COMMENT 'Official website of the team',
-- MAGIC     founded INT COMMENT 'Year the team was founded',
-- MAGIC     club_colors STRING COMMENT 'Official colors of the team',
-- MAGIC     venue STRING COMMENT 'Home venue of the team',
-- MAGIC     coach_name STRING COMMENT 'Full name of the coach',
-- MAGIC     coach_nationality STRING COMMENT 'Nationality of the coach',
-- MAGIC     last_updated TIMESTAMP COMMENT 'Timestamp when the record was last updated',
-- MAGIC     competition_id INT COMMENT 'Unique identifier for the competition',
-- MAGIC     competition_name STRING COMMENT 'Name of the competition',
-- MAGIC     competition_code STRING COMMENT 'Code for the competition',
-- MAGIC     competition_type STRING COMMENT 'Type of competition (e.g., LEAGUE, CUP)',
-- MAGIC     competition_emblem STRING COMMENT 'URL for the competition emblem image',
-- MAGIC     coach_id INT COMMENT 'Unique identifier for the coach',
-- MAGIC     coach_first_name STRING COMMENT 'First name of the coach',
-- MAGIC     coach_last_name STRING COMMENT 'Last name of the coach',
-- MAGIC     coach_dob STRING COMMENT 'Date of birth of the coach',
-- MAGIC     contract_start STRING COMMENT 'Start date of the coach’s contract',
-- MAGIC     contract_until STRING COMMENT 'End date of the coach’s contract',
-- MAGIC     season_start_date TIMESTAMP COMMENT 'Start date of the season',
-- MAGIC     season_end_date TIMESTAMP COMMENT 'End date of the season',
-- MAGIC     current_matchday INT COMMENT 'Current matchday in the season',
-- MAGIC     season_winner STRING COMMENT 'Winner of the season, if applicable',
-- MAGIC     etl_timestamp_utc TIMESTAMP COMMENT 'Timestamp of the ETL process in UTC'
-- MAGIC )
-- MAGIC USING DELTA
-- MAGIC PARTITIONED BY (competition_id)
-- MAGIC COMMENT 'Delta table storing teams, coaches, and competition details by season'
-- MAGIC """
-- MAGIC
-- MAGIC spark.sql(raw_competitions_teams_query)
-- MAGIC

-- COMMAND ----------

-- Opitonal we can delete everyhting
-- DROP TABLE interview_data_de.dmitryanoshin_gmailcom.raw_competitions;
-- DROP TABLE interview_data_de.dmitryanoshin_gmailcom.raw_teams;
-- DROP TABLE interview_data_de.dmitryanoshin_gmailcom.raw_competitions_teams;

-- COMMAND ----------
