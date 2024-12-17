# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Raw Table Schemas

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# competitions API schema
api_competitions_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("area", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("code", StringType(), True),
        StructField("flag", StringType(), True)
    ]), True),
    StructField("name", StringType(), True),
    StructField("code", StringType(), True),
    StructField("type", StringType(), True),
    StructField("emblem", StringType(), True),
    StructField("plan", StringType(), True),
    StructField("currentSeason", StructType([
        StructField("id", IntegerType(), True),
        StructField("startDate", StringType(), True),
        StructField("endDate", StringType(), True),
        StructField("currentMatchday", IntegerType(), True),
        StructField("winner", StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("shortName", StringType(), True),
            StructField("tla", StringType(), True),
            StructField("crest", StringType(), True),
            StructField("address", StringType(), True),
            StructField("website", StringType(), True),
            StructField("founded", IntegerType(), True),
            StructField("clubColors", StringType(), True),
            StructField("venue", StringType(), True),
            StructField("lastUpdated", StringType(), True)
        ]), True)
    ]), True),
    StructField("numberOfAvailableSeasons", IntegerType(), True),
    StructField("lastUpdated", StringType(), True)
])

# COMMAND ----------

# teams API schema
api_teams_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("shortName", StringType(), True),
    StructField("tla", StringType(), True),
    StructField("crest", StringType(), True),
    StructField("address", StringType(), True),
    StructField("website", StringType(), True),
    StructField("founded", IntegerType(), True),
    StructField("clubColors", StringType(), True),
    StructField("venue", StringType(), True),
    StructField("lastUpdated", StringType(), True)
])

# COMMAND ----------

# competition teams schema
api_competitions_teams_schema = StructType([
    StructField("count", IntegerType(), True),
    StructField("filters", StructType([
        StructField("season", StringType(), True)
    ]), True),
    StructField("competition", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("code", StringType(), True),
        StructField("type", StringType(), True),
        StructField("emblem", StringType(), True)
    ]), True),
    StructField("season", StructType([
        StructField("id", IntegerType(), True),
        StructField("startDate", StringType(), True),
        StructField("endDate", StringType(), True),
        StructField("currentMatchday", IntegerType(), True),
        StructField("winner", StringType(), True)
    ]), True),
    StructField("teams", ArrayType(StructType([
        StructField("area", StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("code", StringType(), True),
            StructField("flag", StringType(), True)
        ]), True),
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("shortName", StringType(), True),
        StructField("tla", StringType(), True),
        StructField("crest", StringType(), True),
        StructField("address", StringType(), True),
        StructField("website", StringType(), True),
        StructField("founded", IntegerType(), True),
        StructField("clubColors", StringType(), True),
        StructField("venue", StringType(), True),
        StructField("runningCompetitions", ArrayType(StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("code", StringType(), True),
            StructField("type", StringType(), True),
            StructField("emblem", StringType(), True)
        ])), True),
        StructField("coach", StructType([
            StructField("id", IntegerType(), True),
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("name", StringType(), True),
            StructField("dateOfBirth", StringType(), True),
            StructField("nationality", StringType(), True),
            StructField("contract", StructType([
                StructField("start", StringType(), True),
                StructField("until", StringType(), True)
            ]), True)
        ]), True),
        StructField("squad", ArrayType(StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("position", StringType(), True),
            StructField("dateOfBirth", StringType(), True),
            StructField("nationality", StringType(), True)
        ])), True),
        StructField("staff", ArrayType(StringType()), True),
        StructField("lastUpdated", StringType(), True)
    ])), True)
])

# COMMAND ----------

