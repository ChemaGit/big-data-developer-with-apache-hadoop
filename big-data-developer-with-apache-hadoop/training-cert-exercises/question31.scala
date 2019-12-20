/** Question 31
  * Problem Scenario 4: You have been given MySQL DB with following details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * table=retail_db.categories
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Please accomplish following activities.
  * Import Single table categories (Subset data) to hive managed table , where category_id between 1 and 22
  */
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table categories \
  --where "category_id between 1 and 22" \
  --hive-import \
--hive-database hadoopexam \
--create-hive-table \
  --hive-table categories_subset \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hive
  hive> use hadoopexam;
hive> show tables;
hive> select * from categories_subset;
hive> exit;