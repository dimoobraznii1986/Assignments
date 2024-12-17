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
# MAGIC | Dimensions    | `dim_teams`, `dim_competitions`                                               |
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

logger = logging.getLogger("football_pipeline_dimensions tables")

spark.sql(f"""
TRUNCATE TABLE {assignment_database}.{assignment_schema}.dim_teams
""")

logger.info(f"TRUNCATE TABLE {assignment_database}.{assignment_schema}.dim_teams successfully")

spark.sql(f"""
INSERT INTO {assignment_database}.{assignment_schema}.dim_teams
SELECT DISTINCT
    team_id,
    team_name,
    team_short_name,
    team_tla,
    team_crest,
    team_address,
    team_website,
    team_founded,
    team_club_colors,
    team_venue
FROM {assignment_database}.{assignment_schema}.raw_teams;
""")

logger.info(f"Inserted data into {assignment_database}.{assignment_schema}.dim_teams successfully")

# COMMAND ----------

spark.sql(f"""
TRUNCATE TABLE {assignment_database}.{assignment_schema}.dim_competitions
""")

logger.info(f"TRUNCATE TABLE {assignment_database}.{assignment_schema}.dim_competitions successfully")

spark.sql(f"""
INSERT INTO {assignment_database}.{assignment_schema}.dim_competitions
SELECT DISTINCT
    competition_id,
    competition_name,
    competition_code,
    competition_type,
    area_id,
    area_name,
    area_code,
    area_flag
FROM {assignment_database}.{assignment_schema}.raw_competitions;
""")

logger.info(f"Inserted data into {assignment_database}.{assignment_schema}.dim_competitions successfully")

# COMMAND ----------

