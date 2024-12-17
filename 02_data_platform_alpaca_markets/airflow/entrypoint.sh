#!/bin/bash

# Wait for Airflow to be up (optional, can be adjusted based on your setup)
sleep 10

# Initialize Airflow database
airflow db init

# Create user
airflow users create \
    --username $AIRFLOW_USERNAME \
    --password $AIRFLOW_PASSWORD \
    --firstname $AIRFLOW_FIRSTNAME \
    --lastname $AIRFLOW_LASTNAME \
    --role $AIRFLOW_ROLE \
    --email $AIRFLOW_EMAIL

# Start Airflow (webserver, scheduler, or standalone, depending on your setup)
exec airflow standalone
