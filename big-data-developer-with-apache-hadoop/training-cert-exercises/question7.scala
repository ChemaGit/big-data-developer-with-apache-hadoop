/** Question 7
  * Problem Scenario 1:
  * You have been given MySQL DB with following details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * table=retail_db.categories
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Please accomplish following activities.
  * 1. Connect MySQL DB and check the content of the tables.
  * 2. Copy "retaildb.categories" table to hdfs, without specifying directory name.
  * 3. Copy "retaildb.categories" table to hdfs, in a directory name "categories_target".
  * 4. Copy "retaildb.categories" table to hdfs, in a warehouse directory name "categories_warehouse".
  */
$ mysql -u root -p
mysql> select * from categories limit 10;

sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
  --username root \
  --password cloudera \
  --table categories \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hdfs dfs -ls /user/cloudera
$ hdfs dfs -cat /user/cloudera/categories/part*

sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
  --username root \
  --password cloudera \
  --table categories \
  --delete-target-dir \
--target-dir categories_target \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hdfs dfs -cat /user/cloudera/categories_target/part*

sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
  --username root \
  --password cloudera \
  --table categories \
  --warehouse-dir categories_warehouse \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hdfs dfs -ls /user/cloudera/categories_warehouse
$ hdfs dfs -cat /user/cloudera/categories_warehouse/categories/part*