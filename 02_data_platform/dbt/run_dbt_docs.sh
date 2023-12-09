#!/bin/bash
dbt run --select fct_pdt_count --profiles-dir . --project-dir dbt_trades
dbt docs generate --project-dir dbt_trades
dbt docs serve --project-dir dbt_trades --port 8080