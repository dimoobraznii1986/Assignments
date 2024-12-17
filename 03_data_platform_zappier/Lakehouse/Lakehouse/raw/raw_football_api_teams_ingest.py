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
# MAGIC | Source          | API: https://api.football-data.org/v4/teams/            |
# MAGIC | Database        | `interview_data_de`                                              |
# MAGIC | Schema          | `dmitryanoshin_gmailcom`                                         |
# MAGIC | Output Table    | `raw_teams`                                               |
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

# MAGIC %run ./__tests/teams_assertion

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
    "url": "https://api.football-data.org/v4/teams/",
    "retries": 3,
    "backoff_factor": 1,
    "target_table": f"{assignment_database}.{assignment_schema}.raw_teams"
}

# COMMAND ----------

logger = logging.getLogger("football_pipeline_teams")

# Track start time
pipeline_start_time = time.time()
logger.info("Teams pipeline started.")

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
    teams_data = raw_data.get("teams", [])
    logger.info(f"Extracted {len(teams_data)} teams.")

    # Create DataFrame
    logger.info("Creating DataFrame from teams data.")
    df = spark.createDataFrame(teams_data, schema=api_teams_schema)
    logger.info("DataFrame created successfully.")

    # Flatten the DataFrame
    logger.info("Flattening the DataFrame.")
    df_flattened_teams = df.select(
        F.col("id").alias("team_id"),
        F.col("name").alias("team_name"),
        F.col("shortName").alias("team_short_name"),
        F.col("tla").alias("team_tla"),
        F.col("crest").alias("team_crest"),
        F.col("address").alias("team_address"),
        F.col("website").alias("team_website"),
        F.col("founded").alias("team_founded"),
        F.col("clubColors").alias("team_club_colors"),
        F.col("venue").alias("team_venue"),
        F.to_timestamp(F.col("lastUpdated")).alias("team_last_updated")
    ).withColumn("etl_timestamp_utc", F.current_timestamp())

    # Log success
    logger.info("Flattened Teams DataFrame created successfully.")

    # Write DataFrame to Delta Table
    target_table = parameters["target_table"]
    logger.info(f"Writing data to Delta table: {target_table}")
    df_flattened_teams.write.format("delta").mode("overwrite").saveAsTable(target_table)
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
    team_id in (1,2)
"""
processed_df = spark.sql(query)
# Assertions for data consistency
# Check row count matches
try:
    assert processed_df.count() == mock_teams_df.count(), "Row count does not match expected result."
    print("Assertions passed successfully.")
except AssertionError as e:
    print(f"Assertion failed: {e}")
    raise  

# Get column names for both DataFrames
processed_columns = processed_df.columns
mock_columns = mock_teams_df.columns

# Compare column names
try:
    assert processed_columns == mock_columns, (
        f"Column names do not match!\nProcessed columns: {processed_columns}\nMock columns: {mock_columns}"
    )
    print("Assertions passed successfully.")
except AssertionError as e:
    print(f"Assertion failed: {e}")
    raise  


# COMMAND ----------

