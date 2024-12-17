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
# MAGIC | Source          | API: https://api.football-data.org/v4/competitions/{id}/teams            |
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

# MAGIC %run ./__tests/competitions_teams_assertion

# COMMAND ----------

# MAGIC %run ./__includes/seed_competition_list

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
    "target_table": f"{assignment_database}.{assignment_schema}.raw_competitions_teams",
}

# COMMAND ----------

# Get the list of unique competition IDs
competition_ids = get_competitions_list(api_key)

logger.info(f"List of competitions: {competition_ids}")

# COMMAND ----------

logger = logging.getLogger("football_pipeline")
import requests

# Track start time
pipeline_start_time = time.time()
logger.info("Pipeline started.")

for competition in competition_ids:
    # Wait for 5 seconds
    time.sleep(5)
    try:
        # Fetch raw data
        logger.info("Fetching data from the Competitions and Teams for {competition}")
        raw_data = get_football_data_json(
            url=parameters["url"],
            api_key=api_key,
            retries=parameters["retries"],
            backoff_factor=parameters["backoff_factor"],
            competition_id=competition
        )
        logger.info("Data fetched successfully.")
        

        # Convert the dictionary into a DataFrame
        logger.info("Creating DataFrame from teams data.")
        df = spark.createDataFrame([raw_data], schema=api_competitions_teams_schema)
        logger.info("DataFrame created successfully.")


        # Flatten Teams Data
        teams_df = df.select(F.explode(F.col("teams")).alias("team"))


        # Flatten the nested `team` structure
        flattened_teams_df = teams_df.select(
            F.col("team.id").alias("team_id"),
            F.col("team.name").alias("team_name"),
            F.col("team.shortName").alias("short_name"),
            F.col("team.tla").alias("team_tla"),
            F.col("team.crest").alias("crest"),
            F.col("team.address").alias("address"),
            F.col("team.website").alias("website"),
            F.col("team.founded").alias("founded"),
            F.col("team.clubColors").alias("club_colors"),
            F.col("team.venue").alias("venue"),
            F.col("team.runningCompetitions").alias("running_competitions"),
            F.col("team.lastUpdated").alias("last_updated")
        )

        # Explode `running_competitions` array and flatten the structure
        running_competitions_df = flattened_teams_df.select(
            F.col("team_id"),
            F.explode(F.col("running_competitions")).alias("competition")
        ).select(
            F.col("team_id"),
            F.col("competition.id").alias("competition_id"),
            F.col("competition.name").alias("competition_name"),
            F.col("competition.code").alias("competition_code"),
            F.col("competition.type").alias("competition_type"),
            F.col("competition.emblem").alias("competition_emblem")
        )

        # Flatten the nested `coach` field
        coach_df = teams_df.select(
            F.col("team.id").alias("team_id"),
            F.col("team.coach.id").alias("coach_id"),
            F.col("team.coach.name").alias("coach_name"),
            F.col("team.coach.firstName").alias("coach_first_name"),
            F.col("team.coach.lastName").alias("coach_last_name"),
            F.col("team.coach.nationality").alias("coach_nationality"),
            F.col("team.coach.dateOfBirth").alias("coach_dob"),
            F.col("team.coach.contract.start").alias("contract_start"),
            F.col("team.coach.contract.until").alias("contract_until")
        )

        # Flatten the `season` field
        season_df = df.select(
            F.col("season.id").alias("season_id"),
            F.col("season.startDate").alias("season_start_date"),
            F.col("season.endDate").alias("season_end_date"),
            F.col("season.currentMatchday").alias("current_matchday"),
            F.col("season.winner").alias("season_winner")
        )

        # Add Season ID to Teams Data
        teams_with_season_df = flattened_teams_df.withColumn(
            "season_id", F.lit(raw_data["season"]["id"])
        )

        # Join DataFrames
        final_df = teams_with_season_df \
            .join(running_competitions_df, on="team_id", how="left") \
            .join(coach_df, on="team_id", how="left") \
            .join(season_df, on="season_id", how="left")
        
        # Adding timestamp
        final_df = final_df.withColumn("etl_timestamp_utc", F.current_timestamp())

        # Convert date and timestamp columns from STRING to TIMESTAMP
        final_df = final_df.withColumn("season_start_date", F.to_timestamp(F.col("season_start_date"), "yyyy-MM-dd")) \
                        .withColumn("season_end_date", F.to_timestamp(F.col("season_end_date"), "yyyy-MM-dd")) \
                        .withColumn("last_updated", F.to_timestamp(F.col("last_updated")))
       
        # Remove the running_competitions array column
        final_df = final_df.drop("running_competitions")
        final_df = final_df.filter(F.col("competition_id") == competition)

        # Write DataFrame to Delta Table
        target_table = parameters["target_table"]
        logger.info(f"Writing data to Delta table: {target_table}")
        # Write to Delta table, overwriting only the specified partition
        final_df.write.format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"competition_id = {competition}") \
            .saveAsTable(target_table)
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
WHERE (season_id = 2301 AND team_id = 59)
   OR (season_id = 2301 AND team_id = 68);
"""
processed_df = spark.sql(query)
# Assertions for data consistency
# Check row count matches
try:
    assert processed_df.count() == mock_competitions_teams_df.count(), "Row count does not match expected result."
    print("Assertions passed successfully.")
except AssertionError as e:
    print(f"Assertion failed: {e}")
    raise  # Re-raise the exception to propagate it

# Get column names for both DataFrames
processed_columns = processed_df.columns
mock_columns = mock_competitions_teams_df.columns

# Compare column names
try:
    assert processed_columns == mock_columns, (
        f"Column names do not match!\nProcessed columns: {processed_columns}\nMock columns: {mock_columns}"
    )
    print("Assertions passed successfully.")
except AssertionError as e:
    print(f"Assertion failed: {e}")
    raise  # Re-raise the exception to propagate it


# COMMAND ----------

