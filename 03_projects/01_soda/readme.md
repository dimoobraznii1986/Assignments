


## Create table 

```sql
CREATE TABLE raw_dmitry.superstore.orders(
   row_id        INTEGER  NOT NULL PRIMARY KEY 
  ,order_id      VARCHAR(14) NOT NULL
  ,order_date    DATE  NOT NULL
  ,ship_date     DATE  NOT NULL
  ,ship_mode     VARCHAR(14) NOT NULL
  ,customer_id   VARCHAR(8) NOT NULL
  ,customer_name VARCHAR(22) NOT NULL
  ,segment       VARCHAR(11) NOT NULL
  ,country       VARCHAR(13) NOT NULL
  ,city          VARCHAR(17) NOT NULL
  ,state         VARCHAR(20) NOT NULL
  ,postal_code   VARCHAR(50)
  ,region        VARCHAR(7) NOT NULL
  ,product_id    VARCHAR(15) NOT NULL
  ,category      VARCHAR(15) NOT NULL
  ,subcategory   VARCHAR(11) NOT NULL
  ,product_name  VARCHAR(127) NOT NULL
  ,sales         NUMERIC(9,4) NOT NULL
  ,quantity      INTEGER  NOT NULL
  ,discount      NUMERIC(4,2) NOT NULL
  ,profit        NUMERIC(21,16) NOT NULL
);
```

## Create stage in Snowflake 

```sql
CREATE STAGE file_stage;
```
'@"RAW_DMITRY"."SUPERSTORE"."FILE_STAGE"/supestore_returns.csv'
'@"RAW_DMITRY"."SUPERSTORE"."FILE_STAGE"/supestore_orders.csv'

## Create Staging

```sql
copy into orders
from @file_stage/supestore_orders.csv
file_format = (
    type = 'CSV'
    field_delimiter = '|'
    skip_header = 1
    );
```

```sql
copy into returned_orders
from @file_stage/supestore_returns.csv
file_format = (
    type = 'CSV'
    field_delimiter = '|'
    skip_header = 1
    );
```

```sql
CREATE TABLE returned_orders(
   returned   Boolean NOT NULL
  ,order_id   VARCHAR(14) NOT NULL
);
```

## Create virtual env

```bash
python -m venv soda_venv
```

```bash
pip install soda-core-snowflake
```

```bash
pip freeze > requirements.txt
```

## Prepare a "configuration.yml" file

In the project directory, create the `configuration.yml` file. It will store connection details for Snowflake.

> Use this instruction - https://docs.soda.io/soda/connect-snowflake.html -  as a reference
to copy+paste the connection syntax into your file, then adjust the values to correspond with your data source’s details.

Create and use system variables for *username*, *password*, *account* attributes for security.

```yaml
data_source snowflake:
  type: snowflake
  username: ${SNOWFLAKE_USER}
  password: ${SNOWFLAKE_PASSWORD}
  account: ${SNOWFLAKE_ACCOUNT}
  database:  RAW_DMITRY
  schema:    SUPERSTORE
  warehouse: WH_SUPERSTORE
  connection_timeout: 240
  session_params:
    QUERY_TAG: soda-queries
    QUOTED_IDENTIFIERS_IGNORE_CASE: false
```

Run the following command in prompt to test the configured connection to Snowflake:

```bash
soda test-connection -d snowflake -c ./configuration.yml -V
```

In case of successful configuration, you should see the *Connection 'snowflake' is valid* message in output.


## Write checks in a checks.yml file

Let’s create checks YAML file for our superstore dataset.
Since we are going to test the *Orders* table in Snowflake, I created a separate folder called *checks* and the *orders.yml* in it.

> Your checks define a passing state, what you expect to see in your dataset. Do not define a failed state

> Useful links:
> * SodaCL tutorial - https://docs.soda.io/soda/quick-start-sodacl.html
> * SodaCL metrics and checks - https://docs.soda.io/soda-cl/metrics-and-checks.html 

`orders.yml`

```yaml
# Checks for ORDERS table in Snowflake

checks for ORDERS:
# Built-in metrics:
  - row_count > 10000
  - missing_count(row_id) = 0
  - duplicate_count(row_id) = 0
  - missing_count(order_id) = 0
  - missing_count(order_date) = 0

# Custom metric
  - count_order_date_after_ship_date = 0:
      count_order_date_after_ship_date query: |
        SELECT COUNT(*) 
        FROM ORDERS 
        WHERE order_date > ship_date

# Schema validation
  - schema:
      fail:
        when required column missing:
          - ORDER_ID
          - ROW_ID
        when wrong column type:
          ORDER_ID: TEXT
```

`returned_orders.yml`

```yaml
checks for RETURNED_ORDERS:
# Built-in metrics:
  - row_count > 0

# Referential integrity
  - values in (order_id) must exist in orders (order_id)
```


## Run a scan

To run a scan of the data in your data source, execute the following command:

```yaml
soda scan -d snowflake  -c ./configuration.yml ./checks/
```

The output:

```bash
The output:
[22:51:18] Soda Core 3.1.4
[22:51:19] Scan summary:
[22:51:19] 5/5 checks PASSED:
[22:51:19]     ORDERS in snowflake
[22:51:19]       row_count > 0 [PASSED]
[22:51:19]       missing_count(row_id) = 0 [PASSED]
[22:51:19]       duplicate_count(row_id) = 0 [PASSED]
[22:51:19]       missing_count(order_id) = 0 [PASSED]
[22:51:19]       missing_count(order_date) = 0 [PASSED]
[22:51:19] All is good. No failures. No warnings. No errors.
```

### Soda Core Python library + Snowflake

Use Soda Core Python library on local machine to check the data in Snowflake.

Steps 1, 2, 3 in this scenario are identical to the previous one. So, refer to the prevous section to complete them unless you did already. 

The only difference will be in step 4, we will be running a scan using Soda Core Python library instead of Soda Core CLI.

We will be using the following `soda_scan.py` script to run a scan programmatically

```python
from soda.scan import Scan

scan = Scan()
scan.set_data_source_name("snowflake")

# Add configuration YAML files
scan.add_configuration_yaml_file(file_path="./soda/configuration.yml")

# Add variables
scan.add_variables({"date": "2024-01-28"})

# Add check YAML files 
scan.add_sodacl_yaml_file("./soda/checks/orders.yml")
scan.add_sodacl_yaml_file("./soda/checks/returned_orders.yml")

# Execute the scan
#scan.execute()
exit_code = scan.execute()
print(exit_code)

# Set logs to verbose mode, equivalent to CLI -V option
scan.set_verbose(True)

# Print results of scan
print(scan.get_logs_text())

# Set scan definition name, equivalent to CLI -s option;
#scan.set_scan_definition_name("YOUR_SCHEDULE_NAME")

# Inspect the scan result
#print(scan.get_scan_results()) #returns result as json

# Inspect the scan logs
#scan.get_logs_text() # returns result as text log as you see in CLI

# Typical log inspection # Important feature - used to build automated pipelines
#print(scan.assert_no_error_logs())
#print(scan.assert_no_checks_fail())

# Advanced methods to inspect scan execution logs
#print(scan.has_error_logs())
#print(scan.get_error_logs_text())

# Advanced methods to review check results details
#print(scan.get_checks_fail())
#print(scan.has_check_fails())
#print(scan.get_checks_fail_text())
#print(scan.assert_no_checks_warn_or_fail())
#print(scan.get_checks_warn_or_fail())
#scan.has_checks_warn_or_fail()
#scan.get_checks_warn_or_fail_text()
#print(scan.get_all_checks_text())
```

Run the following command to execute a scan using the python script:

`python .\python\soda_pandas_scan.py`

