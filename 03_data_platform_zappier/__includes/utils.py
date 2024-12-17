# Databricks notebook source
# MAGIC %md
# MAGIC This notebook imports packages and enable Logger.

# COMMAND ----------

# import all packages
import requests
import json
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

# enable logging
import logging

# Configure the logger
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",  # Simple log format
    level=logging.INFO  # Log level
)

# COMMAND ----------

# retry logic for API
def create_session(retries=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504)):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,  # Wait time multiplier: {backoff_factor} * (2 ** retry_number)
        status_forcelist=status_forcelist,  # List of status codes to retry
        allowed_methods=["GET"],  # Retry only on GET requests
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# COMMAND ----------

def get_football_data_json(url, api_key, retries=3, backoff_factor=1, competition_id=None):
    session = create_session(retries=retries, backoff_factor=backoff_factor)
    headers = {"X-Auth-Token": api_key}

    # Adjust URL if competition_id is provided
    if competition_id:
        url = f"{url.rstrip('/')}/{competition_id}/teams"

    for attempt in range(retries):
        logger.info(f"Attempt {attempt + 1}: Sending request to URL: {url} with headers: {headers}")
        response = session.get(url, headers=headers)
        logger.info(f"Response: {response.status_code}")

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:  # Rate limit
            retry_after = int(response.headers.get("Retry-After", 5))
            logger.warning(f"Rate limited. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
        else:
            logger.error(f"Attempt {attempt + 1} failed with status {response.status_code}. Retrying...")
            time.sleep(backoff_factor * (2 ** attempt))
    
    raise Exception(f"Failed after {retries} retries. Last response code: {response.status_code}")

# COMMAND ----------

