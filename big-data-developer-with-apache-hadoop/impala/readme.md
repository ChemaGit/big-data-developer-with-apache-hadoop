## Executing Interactive Queries With Impala

### Impala supports a subset of SQL-92
````text
- Syntax is nearly identical to HiveQL
````

### Impala and Hive are similar in many ways
````
- Tables created in Hive are visible in Impala(and vice versa)
````

### Unlike Hive, Impala does not execute queries via MapReduce

### Hive is better for large, long-running batch processes
````
- Impala is best for interactive/ad-hoc queries
````

### Hive answers queries by running MapReduce jobs
````
- Takes advantage of Hadoop's fault tolerance
- If a node fails during the query, MapReduce runs the task elsewhere
````

### Impala does not use MapReduce
````
- If a node fails during a query, the query will fail
- Workaround: just re-run the query
````
````shell script
### Execute the impal-shell command to start the shell
impala-shell
impala-shell -i myserver.example.com:21000
impala-shell -f myquery.sql
impala-shell -q 'SELECT COUNT(*) FROM accounts' --delimited --output_delimiter='\t' -o results.txt
impala-shell -q 'SELECT * FROM accounts LIMIT 100' --delimited --output_delimiter=',' -o results.txt
impala-shell -f myquery.sql --delimited --output_delimiter='\t' -o results.txt
### Use quit command to exit the Impala shell
quit;
````

### Impala may also support statements that some versions of Hive do not
````sqlite-sql
-- Such the ability to insert individual rows
INSERT INTO departments VALUES (9, 'Engineering')
````

### As with Hive, no guarantee regarding which 10 results are returned
````sqlite-sql
-- Use ORDER BY for top-N queries
-- The field(s) you ORDER BY must be selected in the query
SELECT acct_num, city FROM accounts ORDER BY acct_num DESC LIMIT 10;
````

### Impala supports many of the same built-in functions as Hive
````sqlite-sql
SELECT CONCAT_WS(', ', last_name, first_name) AS full_name FROM accounts WHERE acct_num = 871573;
````

### In the version of Impala on the VM, INVALIDATE METADATA reloads all metadata
````sqlite-sql
INVALIDATE METADATA;
INVALIDATE METADATA products;
````

### Tables created in Impala are immediately available in Hive
````sqlite-sql
CREATE EXTERNAL TABLE prospects
  (id INT,
  name STRING,
  email STRING,
  last_contact TIMESTAMP)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LOCATION '/dept/sales/prospects';
````

### Impala does not support Hive SerDes
### Workaround: use a CTAS in Hive to create a new table for Impala
````sqlite-sql
CREATE TABLE delimited_logdata
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '\t'
 AS
 SELECT * FROM logdata;
````

### Parquet is an ideal format when queries on the table will typically access only a subset of its columns
````sqlite-sql
CREATE TABLE survey_results
    (contact_id INT,
    interviewer_id INT,
    when_interviewed TIMESTAMP,
    annual_income INT,
    occupation STRING,
    postal_code STRING,
    signal_strength_rating TINYINT,
    product_quality_rating TINYINT,
    support_staff_rating TINYINT,
    overall_value_rating TINYINT,
    would_recommend BOOLEAN)
STORED AS PARQUET;
````

### Queries examples
````sqlite-sql
SELECT * FROM accounts WHERE acct_num = 42;
SELECT COUNT(*) AS statecount FROM accounts WHERE state = "OR";

SELECT accounts.state, COUNT(*) AS statecount FROM accounts
JOIN deviceactivations ON (accounts.acct_num = deviceactivations.acct_num)
GROUP BY accounts.state;
````