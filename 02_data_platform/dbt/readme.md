# DBT project for trades

```
docker ps
docker exec -it <container id> bash
dbt run --select fct_pdt_count --profiles-dir . --project-dir dbt_trades
```

or we can run command remote:
```
docker exec -i 02_data_platform-dbt-1 dbt run --select fct_pdt_count --profiles-dir . --project-dir dbt_trades
```