# Football Teams and Competition Analytics

## Summary

This document details the home assignment project for Football data.

## API Scope

The scope of data:

- Teams `/v4/teams/`
- Competitions `/v4/competitions/`
- Teams and Competitions `/v4/competitions/{id}/teams`

## Approach

I will expand on the details below. In this section, I want to share the general approach. I used a similar approach to the one I implemented at Microsoft Xbox when working on Delta Lake and performing the migration of a legacy MPP data warehouse.

This is my project structure:

```bash
├── README.md
├── __includes 
│   └── utils.py # common functions and libraries for this project
├── facts # aka Gold layer
│   ├── __includes
│   │   └── dim_facts_ddl.py
│   ├── dimensions.py
│   └── facts.py
├── football_analytics_pipeline.py
└── raw # aka Bronze Layer
    ├── __includes
    │   ├── raw_football_schemas.py # Struct Schemas for API
    │   ├── raw_tables_ddl.sql # Table DDLs
    │   └── seed_competition_list.py # Data seed for list of competitions
    ├── __tests # assertions for notebooks
    │   ├── competition_assertion.py 
    │   ├── competitions_teams_assertion.py
    │   └── teams_assertion.py
    ├── raw_football_api_competitions_ingest.py
    ├── raw_football_api_competitions_teams_ingest.py
    └── raw_football_api_teams_ingest.py
```

My approach is to create reusable elements, ensure quality, and enable parameterization. This allows for reusing elements effectively.  
I am using layers similar to [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture) but with my own terms such as `raw`, `stage`, and `facts`. I skipped the middle layer for our use cases.

In the folder `raw`, I am extracting data from the API and writing it into the database.

> Note: I focused mostly on the Databricks development framework rather than the actual data and business analysis of the data I am getting from APIs.

## How to run

You can run the master notebook `/Workspace/Users/dmitry.anoshin@gmail.com/Lakehouse/football_analytics_pipeline` that will trigger everything. 

You have to provide:
- API Key (I am using Widget)
- Target Schema and Database (I am using Widget)

The notebooks will create:
- staging tables:
  - `raw_competitions`
  - `raw_teams`
  - `raw_competitions_teams`
- facts and dim tables:
  - `dim_teams`
  - `dim_competitions`
  - `fact_team_participation_timeseries`

It will also run assertions for staging and fact tables.

I've [scheduled](https://dbc-a41d8d9a-943c.cloud.databricks.com/jobs/699004285441833?o=2830205401490575) the pipeline to run daily.

## Help of AI

I use AI tools daily in my work, primarily ChatGPT and Claude, which are usually sufficient.

Use cases for the scenario:
- Generate Struct Schema and DDL for tables
- Generate comments for columns and tables
- Verify grammar in texts
- Generate docstrings for functions
- Troubleshoot PySpark
- Generate SQL for fact tables (when requirements, context, or time to define business logic are limited)

## Technology Choice

### Storage

Databricks is connected to an external S3 bucket, which is the default choice for the organization. Databricks is hosted on AWS.

### Data Ingestion

For the current use case, we are using Python code to directly hit API endpoints and write the data into Delta Tables.  
An alternative could be streaming solutions with Autoloader (Spark Structured Streaming) if we have Kafka or Kinesis endpoints, or batch ingestion tools such as Airbyte, Meltano, or Fivetran.

### Orchestration

For this assignment, I am using Databricks Jobs, which serve as a simple cron scheduler. It can run on a schedule and execute a dedicated notebook with nested notebooks. However, this approach lacks support for complex dependencies.

An alternative could be Delta Live Tables or an external orchestration tool such as Airflow, Dagster, or Prefect.

### Data Transformation

For raw data, we are using PySpark; for the facts layer, we use SparkSQL.  
Alternatively, we could use dbt Core to store complex business logic and documentation in a centralized location.

### Business Intelligence

Using SQL Warehouse, we can connect our fact tables with popular BI tools or use SQL Workspace to query data based on Unity Catalog.

## Solution Architecture

The current architecture consists of two layers:
- `raw` (aka bronze), where we control data ingestion and apply minimal transformations.
- `facts` (aka gold), where we apply business transformations.

In `raw`, I've used a reusable function `get_football_data_json` that pulls data based on `URL` and `API_KEY`.

## API Retry

We are calling the API endpoint, and to avoid rate limits (429 errors), I've implemented two options:
- By default, I've added a function `create_session` that is responsible for retries.
- For the iteration pipeline, I've included a 5-second wait between iterations.

### Things to Improve in Future Iterations

There are several things to improve in future iterations:
1. Integration scope for storing secrets and API keys.
2. Adding Repos for better integration.
3. Option to use Databricks CLI for local IDE development.
4. Adding more tests (assertions).
5. Adding DataObservability platform for alerts.

## Engineering Excellence

In this section, I want to highlight additional topics related to the production operation of Databricks.

>Note: This is based on my previous experience with Databricks.

### Performance Optimization

I've implemented table partitions and used the `OPTIMIZE` command for all tables to improve performance. Although we are currently using a Serverless Cluster, it is still essential to monitor the performance of pipelines regularly.

Currently, our ETL processes reload all data with every run. An ideal improvement would be to adopt an incremental approach to ingest only new or updated data, reducing processing time and cost. Additionally, we could explore switching to a dedicated cluster for better control and compare its cost with the serverless option.

In the event of job failures, we can review the Spark UI and Spark Metrics to identify bottlenecks and optimize our pipelines. We can also leverage caching (`CACHE`) for frequently used data to improve performance.

### Version Control

By using Repos, we can connect Databricks workspaces to popular version control systems like GitHub, GitLab, and others. This enables a more structured engineering approach, including personal development spaces, collaboration, and thorough code reviews, ensuring higher-quality code in production.

### Security

To secure production operations, we need to focus on several key areas:
- **Network Security**: Use private subnets for the Databricks Workspace and Clusters to isolate workloads.
- **Secrets Management**: Utilize AWS Secrets Manager with Databricks Scopes for secure integration with sensitive credentials.
- **Role-Based Access Control (RBAC)**: Implement fine-grained access control to restrict data and system access.
- **External Authentication**: Integrate with third-party authentication providers like Okta for robust identity and access management.

### CI/CD

Databricks development revolves around code, so adopting software engineering practices is critical. This includes using tools like `pre-commit` for code validation, running linters, writing unit tests, and automating deployments through CI/CD pipelines. These practices help maintain code quality and streamline the development lifecycle.

### Infrastructure as Code (IaC)

Managing infrastructure with tools like Terraform ensures consistency, transparency, and repeatability. By defining Databricks infrastructure as code, teams can track changes, collaborate effectively, and maintain high-quality environments for production and development.

### Cost Tracking

Databricks can become expensive if not managed carefully. It’s crucial to monitor weekly credit consumption to identify inefficiencies. Cost-cutting strategies could include:
- Reviewing the necessity of serverless clusters versus dedicated clusters.
- Optimizing pipeline execution to avoid redundant computations.
- Leveraging spot instances or auto-scaling to manage cluster costs dynamically.

### Additional Recommendations

- Regularly review pipeline runtimes and resource utilization to identify inefficiencies.
- Automate alerts for cost thresholds or unexpected increases in credit usage.
- Document all best practices and share them across the team to promote consistency and operational excellence.