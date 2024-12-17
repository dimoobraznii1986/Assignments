# Databricks notebook source
# MAGIC %md
# MAGIC Unique list of competitions

# COMMAND ----------

import requests

# COMMAND ----------

# Function to fetch competitions from the API
def get_competitions_list(api_key):
    url = "https://api.football-data.org/v4/competitions/"
    headers = {"X-Auth-Token": api_key}
    
    try:
        # Call the API
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            competitions_data = response.json()
            competitions = competitions_data.get("competitions", [])
            competition_ids = {comp["id"] for comp in competitions}
            return competition_ids
        else:
            raise Exception(f"Failed to fetch competitions. Status code: {response.status_code}, message: {response.text}")
    
    except Exception as e:
        print(f"Error occurred while fetching competitions: {e}")
        raise