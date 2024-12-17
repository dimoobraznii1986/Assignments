# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Assertion Mock Data Data Frame

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Row

# Mock data
mock_data = [
    Row(
        competition_id=2018,
        area_id=2077,
        area_name="Europe",
        area_code="EUR",
        area_flag="https://crests.football-data.org/EUR.svg",
        competition_name="European Championship",
        competition_code="EC",
        competition_type="CUP",
        competition_emblem="https://crests.football-data.org/ec.png",
        competition_plan="TIER_ONE",
        season_id=1537,
        season_start_date="2024-06-14",
        season_end_date="2024-07-14",
        season_current_matchday=7,
        winner_id=760,
        winner_name="Spain",
        winner_short_name="Spain",
        winner_tla="ESP",
        winner_crest="https://crests.football-data.org/760.svg",
        winner_address="Ramón y Cajal, s/n Las Rozas 28230",
        winner_website="http://www.rfef.es",
        winner_founded=1909,
        winner_club_colors="Red / Blue / Yellow",
        winner_venue="Estadio Alfredo Di Stéfano",
        winner_last_updated="2021-05-26T09:46:48.000+00:00",
        number_of_available_seasons=17,
        last_updated="2024-09-13T17:04:30.000+00:00",
        etl_timestamp_utc="2024-12-14T21:45:09.195+00:00"
    ),
    Row(
        competition_id=2000,
        area_id=2267,
        area_name="World",
        area_code="INT",
        area_flag=None,
        competition_name="FIFA World Cup",
        competition_code="WC",
        competition_type="CUP",
        competition_emblem="https://crests.football-data.org/qatar.png",
        competition_plan="TIER_ONE",
        season_id=1382,
        season_start_date="2022-11-20",
        season_end_date="2022-12-18",
        season_current_matchday=8,
        winner_id=762,
        winner_name="Argentina",
        winner_short_name="Argentina",
        winner_tla="ARG",
        winner_crest="https://crests.football-data.org/762.png",
        winner_address="Viamonte 1366/76 Buenos Aires, Buenos Aires 1053",
        winner_website="http://www.afa.org.ar",
        winner_founded=1893,
        winner_club_colors="Sky Blue / White / Black",
        winner_venue=None,
        winner_last_updated="2022-05-17T21:09:25.000+00:00",
        number_of_available_seasons=22,
        last_updated="2024-09-13T17:04:20.000+00:00",
        etl_timestamp_utc="2024-12-14T21:45:09.195+00:00"
    )
]

expected_df = spark.createDataFrame(mock_data)

# COMMAND ----------

