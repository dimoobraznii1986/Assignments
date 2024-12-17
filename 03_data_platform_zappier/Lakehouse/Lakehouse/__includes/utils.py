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
    """
    Creates a requests session with retry logic for robust API communication.

    Parameters:
        retries (int): Total number of retry attempts for failed requests. Defaults to 3.
        backoff_factor (int): Exponential backoff factor for retry intervals. The wait time between retries
                              is calculated as {backoff_factor} * (2 ** retry_number). Defaults to 1.
        status_forcelist (tuple): HTTP status codes that should trigger a retry. Defaults to:
                                  (429, 500, 502, 503, 504).

    Returns:
        requests.Session: A configured session with retry logic applied.

    Notes:
        - Retries are applied only to GET requests, as specified by `allowed_methods`.
        - Mounts an `HTTPAdapter` to handle retries for both HTTP and HTTPS connections.
        - Useful for handling transient API issues like rate limits or server errors.

    Example:
        >>> session = create_session(retries=5, backoff_factor=2)
        >>> response = session.get("https://api.football-data.org/v4/teams/")
        >>> response.json()
    """
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
    """
    Fetch football data from a specified API endpoint, with retry logic for robustness.

    Parameters:
        url (str): The base URL of the API endpoint.
        api_key (str): The API key for authentication.
        retries (int): Number of retry attempts in case of failure. Defaults to 3.
        backoff_factor (int): Exponential backoff factor for retry intervals. Defaults to 1.
        competition_id (str, optional): The ID of the competition to fetch team data for.
                                         If provided, it appends `/competition_id/teams` to the URL.

    Returns:
        dict: The JSON response from the API if the request is successful.

    Raises:
        Exception: If all retry attempts fail, raises an exception with the last response code.

    Notes:
        - Handles rate-limiting by respecting the "Retry-After" header if present.
        - Implements exponential backoff for retries to avoid overwhelming the server.

    Example:
        >>> get_football_data_json("https://api.football-data.org/v4/competitions", 
                                   api_key="your_api_key", competition_id="2021")
    """
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

