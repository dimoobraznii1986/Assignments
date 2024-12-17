# Databricks notebook source
from pyspark.sql.types import (
    StructType, StructField, 
    IntegerType, StringType, 
    TimestampType, DateType
)

from pyspark.sql import Row
from datetime import datetime

competitions_teams_schema = StructType([
    StructField("season_id", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True),
    StructField("short_name", StringType(), True),
    StructField("team_tla", StringType(), True),
    StructField("crest", StringType(), True),
    StructField("address", StringType(), True),
    StructField("website", StringType(), True),
    StructField("founded", IntegerType(), True),
    StructField("club_colors", StringType(), True),
    StructField("venue", StringType(), True),
    StructField("coach_name", StringType(), True),
    StructField("coach_nationality", StringType(), True),
    StructField("last_updated", TimestampType(), True),
    StructField("competition_id", IntegerType(), True),
    StructField("competition_name", StringType(), True),
    StructField("competition_code", StringType(), True),
    StructField("competition_type", StringType(), True),
    StructField("competition_emblem", StringType(), True),
    StructField("coach_id", IntegerType(), True),
    StructField("coach_first_name", StringType(), True),
    StructField("coach_last_name", StringType(), True),
    StructField("coach_dob", DateType(), True),
    StructField("contract_start", StringType(), True),
    StructField("contract_until", StringType(), True),
    StructField("season_start_date", TimestampType(), True),
    StructField("season_end_date", TimestampType(), True),
    StructField("current_matchday", IntegerType(), True),
    StructField("season_winner", StringType(), True),
    StructField("etl_timestamp_utc", TimestampType(), True)
])
# Mock data
mock_competitions_teams_data = [
    Row(
        season_id=2301,
        team_id=59,
        team_name="Blackburn Rovers FC",
        short_name="Blackburn",
        team_tla="BLA",
        crest="https://crests.football-data.org/59.png",
        address="Ewood Park Blackburn BB2 4JF",
        website="http://www.rovers.co.uk",
        founded=1874,
        club_colors="Blue / White",
        venue="Ewood Park",
        coach_name="John Eustace",
        coach_nationality="England",
        last_updated=datetime.fromisoformat("2022-02-19T08:50:27+00:00"),
        competition_id=2016,
        competition_name="Championship",
        competition_code="ELC",
        competition_type="LEAGUE",
        competition_emblem="https://crests.football-data.org/ELC.png",
        coach_id=47211,
        coach_first_name="John",
        coach_last_name="Eustace",
        coach_dob=datetime.fromisoformat("1979-11-03").date(),
        contract_start="2024-02",
        contract_until="2026-06",
        season_start_date=datetime.fromisoformat("2024-08-09T00:00:00+00:00"),
        season_end_date=datetime.fromisoformat("2025-05-03T00:00:00+00:00"),
        current_matchday=21,
        season_winner=None,
        etl_timestamp_utc=datetime.fromisoformat("2024-12-15T20:08:58.007+00:00")
    ),
    Row(
        season_id=2301,
        team_id=68,
        team_name="Norwich City FC",
        short_name="Norwich",
        team_tla="NOR",
        crest="https://crests.football-data.org/68.png",
        address="Carrow Road Norwich NR1 1JE",
        website="http://www.canaries.co.uk",
        founded=1902,
        club_colors="Yellow / Green",
        venue="Carrow Road",
        coach_name="Johannes Thorup",
        coach_nationality="Denmark",
        last_updated=datetime.fromisoformat("2022-03-23T08:28:03+00:00"),
        competition_id=2016,
        competition_name="Championship",
        competition_code="ELC",
        competition_type="LEAGUE",
        competition_emblem="https://crests.football-data.org/ELC.png",
        coach_id=205535,
        coach_first_name="Johannes",
        coach_last_name="Thorup",
        coach_dob=datetime.fromisoformat("1989-02-19").date(),
        contract_start="2024-07",
        contract_until="2027-06",
        season_start_date=datetime.fromisoformat("2024-08-09T00:00:00+00:00"),
        season_end_date=datetime.fromisoformat("2025-05-03T00:00:00+00:00"),
        current_matchday=21,
        season_winner=None,
        etl_timestamp_utc=datetime.fromisoformat("2024-12-15T20:08:58.007+00:00")
    )
]

# Create DataFrame with schema
mock_competitions_teams_df = spark.createDataFrame(mock_competitions_teams_data,schema=competitions_teams_schema)

# COMMAND ----------

