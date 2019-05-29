/** Question 58
  * Problem Scenario 5 : You have been given following mysql database details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Please accomplish following activities.
  * 1. List all the tables using sqoop command from retail_db
  * 2. Write simple sqoop eval command to check whether you have permission to read database tables or not.
  * 3. Import all the tables as avro files in /user/hive/warehouse/retail_cca174.db
  * 4. Import departments table as a text file in /user/cloudera/departments.
  */
sqoop list-tables \
  --connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera

sqoop eval \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --query "select * from customer_new"

sqoop import-all-tables \
  --connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --as-avrodatafile \
  --warehouse-dir /user/hive/warehouse/retail_cca174.db \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--autoreset-to-one-mapper

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table departments \
  --delete-target-dir \
  --target-dir /user/cloudera/departments \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

$ hdfs dfs -ls /user/hive/warehouse/retail_cca174.db
$ hdfs dfs -ls /user/hive/warehouse/retail_cca174.db/products
$ avro-tools tojson hdfs://quickstart.cloudera/user/hive/warehouse/retail_cca174.db/products/part-m-00000.avro
$ hdfs dfs -ls /user/cloudera/departments