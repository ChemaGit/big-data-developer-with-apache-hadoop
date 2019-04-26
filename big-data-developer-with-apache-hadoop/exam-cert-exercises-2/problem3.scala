/*
Question 3: Correct
Instructions:
Connect to mySQL database using sqoop, import all products into a metastore table named product_new inside default database.
Data Description:
A mysql instance is running on the gateway node.In that instance you will find products table
> Installation : on the cluser node gateway
> Database name: retail_db
> Table name: Products
> Username: root
> Password: cloudera

Output Requirement:
product_new table does not exist in metastore.
Save output in parquet format fields separated by a colon.
*/
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table products \
  --fields-terminated-by ':' \
  --as-parquetfile \
  --hive-import \
--hive-database default \
--create-hive-table \
  --hive-table product_new \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

// Explanation
// Use localhost or quickstart while running on CDH, "-m 1" is used because product table does not have any primary key.You can use either "split-by" or "-m 1" if table does not have any primary key.

$ hive
  hive> use default;
hive> show tables;
hive> describe formatted product_new;
hive> select * from product_new limit 10;

$ hdfs dfs -ls /user/hive/warehouse/product_new
$ parquet-tools meta hdfs://quickstart.cloudera/user/hive/warehouse/product_new/2c21269c-e325-40c9-92a9-4461f9a30c22.parquet
$ parquet-tools head hdfs://quickstart.cloudera/user/hive/warehouse/product_new/2c21269c-e325-40c9-92a9-4461f9a30c22.parquet