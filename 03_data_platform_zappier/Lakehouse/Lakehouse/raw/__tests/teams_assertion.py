# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Assertion Mock Data Data Frame

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Row

# Mock data
from pyspark.sql import Row
from pyspark.sql import functions as F

# Mock data
mock_teams_data = [
    Row(
        team_id=1,
        team_name="1. FC Köln",
        team_short_name="1. FC Köln",
        team_tla="KOE",
        team_crest="https://crests.football-data.org/1.png",
        team_address="Franz-Kremer-Allee 1 Köln 50937",
        team_website="http://www.fc-koeln.de",
        team_founded=1948,
        team_club_colors="Red / White",
        team_venue="RheinEnergieSTADION",
        team_last_updated='2022-02-25T16:49:46.000+00:00',
        etl_timestamp_utc='2024-12-15T02:51:10.551+00:00'
    ),
    Row(
        team_id=2,
        team_name="TSG 1899 Hoffenheim",
        team_short_name="Hoffenheim",
        team_tla="TSG",
        team_crest="https://crests.football-data.org/2.png",
        team_address="Horrenberger Straße 58 Zuzenhausen 74939",
        team_website="http://www.achtzehn99.de",
        team_founded=1921,
        team_club_colors="Blue / White",
        team_venue="PreZero Arena",
        team_last_updated="2022-02-25T16:49:58.000+00:00",
        etl_timestamp_utc="2024-12-15T02:51:10.551+00:00"
    )
]

# Create a DataFrame
mock_teams_df = spark.createDataFrame(mock_teams_data)

# COMMAND ----------

