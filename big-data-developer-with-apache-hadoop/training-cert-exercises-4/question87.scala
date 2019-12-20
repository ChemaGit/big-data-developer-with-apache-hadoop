/** Question 87
  * Problem Scenario 20 : You have been given MySQL DB with following details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * table=retail_db.categories
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Please accomplish following activities.
  * 1. Write a Sqoop Job which will import "retaildb.categories" table to hdfs, in a directory name "/user/cloudera/question87/categories_targetJob".
  */
sqoop job \
--create categories_job \
  -- import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table categories \
  --target-dir /user/cloudera/question87/categories_targetJob \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ sqoop job --exec categories_job

$ hdfs dfs -cat /user/cloudera/question87/categories_targetJob/part*