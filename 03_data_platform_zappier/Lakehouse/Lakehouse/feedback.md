# Feedback

## What Went Well

### Technical Strengths
- Your understanding of **Databricks** and **PySpark** stood out, along with thoughtful implementation of:
  - Logging mechanisms  
  - Execution time tracking  
  - Optimization commands  

### Documentation
- You provided clear, descriptive documentation, including:
  - Field comments  
  - Structured **README** files  

### Production Readiness Considerations
- Concepts like **retries for API handling** and **ETL timestamps** for data lineage were good additions.


## Areas to Improve

### Data Engineering Fundamentals
- Raw data wasn’t preserved in its **original form** and was flattened unnecessarily during ingestion.  
- **API handling** could have been more consistent, particularly with reusing functions like `get_football_data_json`.  
- There were redundant code executions, such as running **DDL scripts multiple times**.

### Architecture and Code Structure
- The use of `%run` for imports led to **modularity issues** and duplicated code execution.  
- Mixing **ingestion** and **transformation logic** made the workflow harder to follow and maintain.

### Data Modeling
- Your **fact table design** mixed multiple grains (`team`, `season`, and `match day`), causing confusion in understanding the data structure.  
- Some unnecessary **transformations** complicated the data ingestion process and impacted clarity.

### Production Readiness
- **Error handling** was incomplete, and **data quality checks** were limited.  
- Aggregations appeared **incorrect**, and there were **inconsistencies** in competition-team relationships.

---

## Summary

You brought some strong **technical knowledge** and **documentation skills** to the table. However, there were key areas—particularly in **data modeling**, **modularity**, and **fundamentals**—where we felt the submission fell short.  

I hope this feedback provides clarity and helps as you continue to build on your skills.