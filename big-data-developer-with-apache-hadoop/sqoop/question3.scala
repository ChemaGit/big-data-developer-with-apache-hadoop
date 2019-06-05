/**
 * Problem Scenario 19 : You have been given following mysql database details as well as
 * other info.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Now accomplish following activities.
 * 1. Import departments table from mysql to hdfs as textfile in departments_text directory. 
 * 2. Import departments table from mysql to hdfs as sequncefile in departments_sequence directory.
 * 3. Import departments table from mysql to hdfs as avro file in departments_avro directory.
 * 4. Import departments table from mysql to hdfs as parquet file in departments_parquet directory.
 */
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba --password cloudera \
--table departments \
--as-textfile \
--delete-target-dir \
--target-dir departments_text 

hdfs dfs -ls departments_text
hdfs dfs -cat departments_text/part-m-00000 | head -n 20

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba --password cloudera \
--table departments \
--as-sequencefile \
--delete-target-dir \
--target-dir departments_sequence 

hdfs dfs -ls departments_sequence
hdfs dfs -cat departments_sequence/part-m-00000 | head -n 20

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba --password cloudera \
--table departments \
--as-avrodatafile \
--delete-target-dir \
--target-dir departments_avro 

hdfs dfs -ls departments_avro
hdfs dfs -cat departments_avro/part-m-00000.avro | head -n 20

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba --password cloudera \
--table departments \
--as-parquetfile \
--delete-target-dir \
--target-dir departments_parquet 

hdfs dfs -ls departments_parquet
hdfs dfs -cat departments_parquet/44d4678f-e252-4502-9d5d-7f42f7b31f27.parquet | head -n 20