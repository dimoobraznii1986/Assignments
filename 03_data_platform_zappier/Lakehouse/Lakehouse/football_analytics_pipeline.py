# Databricks notebook source
# MAGIC %md
# MAGIC # Primary notebook for scheduding

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC | Description     | Details                                                         |
# MAGIC |-----------------|-----------------------------------------------------------------|
# MAGIC | Author          | Dmitry Anoshin                                                 |
# MAGIC | Source          | API: https://api.football-data.org/         |
# MAGIC | Database        | `interview_data_de`                                              |
# MAGIC | Schema          | `dmitryanoshin_gmailcom`                                         |
# MAGIC
# MAGIC ## Required parameters
# MAGIC
# MAGIC - `api_key`
# MAGIC - `assignment_database`
# MAGIC - `assignment_schema`
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

# MAGIC %md
# MAGIC ## Load utilities

# COMMAND ----------

# MAGIC %run ./__includes/utils

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Running Raw data pipelines

# COMMAND ----------



# COMMAND ----------

# MAGIC %run ./raw/raw_football_api_competitions_ingest

# COMMAND ----------

# MAGIC %run ./raw/raw_football_api_teams_ingest

# COMMAND ----------

# MAGIC %run ./raw/raw_football_api_competitions_teams_ingest

# COMMAND ----------

# MAGIC %md
# MAGIC ## Running Facts and Dimensions Tabls

# COMMAND ----------

# MAGIC %run ./facts/dimensions

# COMMAND ----------

# MAGIC %run ./facts/facts

# COMMAND ----------

