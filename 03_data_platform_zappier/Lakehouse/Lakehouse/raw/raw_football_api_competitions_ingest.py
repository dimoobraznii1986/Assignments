# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Raw Layer 
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC | Description     | Details                                                         |
# MAGIC |-----------------|-----------------------------------------------------------------|
# MAGIC | Author          | Dmitry Anoshin                                                 |
# MAGIC | Source          | API: https://api.football-data.org/v4/competitions/            |
# MAGIC | Database        | `interview_data_de`                                              |
# MAGIC | Schema          | `dmitryanoshin_gmailcom`                                         |
# MAGIC | Output Table    | `raw_competitions_teams`                                               |
# MAGIC
# MAGIC ## History
# MAGIC
# MAGIC | Date       | Developed by    | Changes              |
# MAGIC |------------|-----------------|----------------------|
# MAGIC | 2024-12-14 | Dmitry Anoshin  | Initial Pipeline     |
# MAGIC
# MAGIC ## Other Details
# MAGIC
# MAGIC This notebook is using API toke as is in development purpose. In production environment we will leverage [Databricks Scope](https://docs.databricks.com/en/security/secrets/index.html).
# MAGIC
# MAGIC This notebook doesn't explicitly use any `import` statement. Packages and functions are loaded via external notebooks.

# COMMAND ----------

# MAGIC %run ../__includes/utils

# COMMAND ----------

# MAGIC %run ./__includes/raw_tables_ddl

# COMMAND ----------

# MAGIC %run ./__includes/raw_football_schemas

# COMMAND ----------

# MAGIC %run ./__tests/competition_assertion

# COMMAND ----------

# Create a text widget for the API key
# TODO: add scope integration 
dbutils.widgets.text("api_key", "", "Enter API Key")

# Retrieve the API key from the widget
api_key = dbutils.widgets.get("api_key")

# Create a text widget for the API key
dbutils.widgets.text("assignment_database", "", "Enter Assignment Database name")

# Retrieve the API key from the widget
assignment_database = dbutils.widgets.get("assignment_database")

# Create a text widget for the API key
dbutils.widgets.text("assignment_schema", "", "Enter Assignment Schema name")

# Retrieve the API key from the widget
assignment_schema = dbutils.widgets.get("assignment_schema")

# COMMAND ----------

parameters = {
    "url": "https://api.football-data.org/v4/competitions/",
    "retries": 3,
    "backoff_factor": 1,
    "target_table": f"{assignment_database}.{assignment_schema}.raw_competitions"
}

# COMMAND ----------

logger = logging.getLogger("football_pipeline_competitions")

# Track start time
pipeline_start_time = time.time()
logger.info("Pipeline started.")

try:
    # Fetch raw data
    logger.info("Fetching data from the Football Data API.")
    raw_data = get_football_data_json(
        url=parameters["url"],
        api_key=api_key,
        retries=parameters["retries"],
        backoff_factor=parameters["backoff_factor"]
    )
    logger.info("Data fetched successfully.")

    # Extract competitions data
    logger.info("Extracting competitions data from the raw API response.")
    competitions_data = raw_data.get("competitions", [])
    logger.info(f"Extracted {len(competitions_data)} competitions.")

    # Create DataFrame
    logger.info("Creating DataFrame from competitions data.")
    df = spark.createDataFrame(competitions_data, schema=api_competitions_schema)
    logger.info("DataFrame created successfully.")

    # Flatten the DataFrame
    logger.info("Flattening the DataFrame.")
    df_flattened = df.select(
        F.col("id").alias("competition_id"),
        F.col("area.id").alias("area_id"),
        F.col("area.name").alias("area_name"),
        F.col("area.code").alias("area_code"),
        F.col("area.flag").alias("area_flag"),
        F.col("name").alias("competition_name"),
        F.col("code").alias("competition_code"),
        F.col("type").alias("competition_type"),
        F.col("emblem").alias("competition_emblem"),
        F.col("plan").alias("competition_plan"),
        F.col("currentSeason.id").alias("season_id"),
        F.to_date(F.col("currentSeason.startDate")).alias("season_start_date"),
        F.to_date(F.col("currentSeason.endDate")).alias("season_end_date"),
        F.col("currentSeason.currentMatchday").alias("season_current_matchday"),
        F.col("currentSeason.winner.id").alias("winner_id"),
        F.col("currentSeason.winner.name").alias("winner_name"),
        F.col("currentSeason.winner.shortName").alias("winner_short_name"),
        F.col("currentSeason.winner.tla").alias("winner_tla"),
        F.col("currentSeason.winner.crest").alias("winner_crest"),
        F.col("currentSeason.winner.address").alias("winner_address"),
        F.col("currentSeason.winner.website").alias("winner_website"),
        F.col("currentSeason.winner.founded").alias("winner_founded"),
        F.col("currentSeason.winner.clubColors").alias("winner_club_colors"),
        F.col("currentSeason.winner.venue").alias("winner_venue"),
        F.to_timestamp(F.col("currentSeason.winner.lastUpdated")).alias("winner_last_updated"),
        F.col("numberOfAvailableSeasons").alias("number_of_available_seasons"),
        F.to_timestamp(F.col("lastUpdated")).alias("last_updated")
    )
    df_with_timestamp = df_flattened.withColumn("etl_timestamp_utc", F.current_timestamp())
    logger.info("Flattened DataFrame created successfully.")

    # Write DataFrame to Delta Table
    target_table = parameters["target_table"]
    logger.info(f"Writing data to Delta table: {target_table}")
    df_with_timestamp.write.format("delta").mode("overwrite").saveAsTable(target_table)
    logger.info(f"Data written successfully to {target_table}.")

    # Optimize the Delta Table
    logger.info(f"Optimizing Delta table: {target_table}")
    spark.sql(f"OPTIMIZE {target_table}")
    logger.info(f"Delta table {target_table} optimized successfully.")

except Exception as e:
    logger.error(f"Pipeline failed: {e}")
    raise

# Track end time
pipeline_end_time = time.time()

# Calculate execution duration
execution_duration = pipeline_end_time - pipeline_start_time
logger.info(f"Pipeline completed. Total execution time: {execution_duration:.2f} seconds.")

# COMMAND ----------

# Running test
query = f"""
SELECT *
FROM {target_table}
WHERE
    (competition_id = 2018 AND competition_name = 'European Championship' AND area_id = 2077 AND season_start_date = '2024-06-14')
    OR
    (competition_id = 2000 AND competition_name = 'FIFA World Cup' AND area_id = 2267 AND season_start_date = '2022-11-20');
"""
processed_df = spark.sql(query)
# Assertions for data consistency
# Check row count matches
try:
    assert processed_df.count() == expected_df.count(), "Row count does not match expected result."
    print("Assertions passed successfully.")
except AssertionError as e:
    print(f"Assertion failed: {e}")
    raise  # Re-raise the exception to propagate it

# Get column names for both DataFrames
processed_columns = processed_df.columns
mock_columns = expected_df.columns

# Compare column names
try:
    assert processed_columns == mock_columns, (
        f"Column names do not match!\nProcessed columns: {processed_columns}\nMock columns: {mock_columns}"
    )
    print("Assertions passed successfully.")
except AssertionError as e:
    print(f"Assertion failed: {e}")
    raise  # Re-raise the exception to propagate it
