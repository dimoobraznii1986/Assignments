# DBT project for trades

```
docker ps
docker exec -it <container id> bash
dbt run --select fct_pdt_count --profiles-dir . --project-dir dbt_trades
```