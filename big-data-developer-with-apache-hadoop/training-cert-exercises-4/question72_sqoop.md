# Question 72
````text
   Problem Scenario 3: You have been given MySQL DB with following details.
   user=retail_dba
   password=cloudera
   database=retail_db
   table=retail_db.categories
   jdbc URL = jdbc:mysql://quickstart:3306/retail_db
   Please accomplish following activities.
   1. Import data from categories table, where category_id=22 (Data should be stored in categories subset)
   2. Import data from categories table, where category_id>22 (Data should be stored in categories_subset_2)
   3. Import data from categories table, where category_id between 1 and 22 (Data should be stored in categories_subset_3)
   4. While importing catagories data change the delimiter to '|' (Data should be stored in categories_subset_4)
   5. Importing data from catagories table and restrict the import to category_name,category id columns only with delimiter as '|' (In categories_subset_5 directory)
   6. Add null values in the table using below SQL statement ALTER TABLE categories modify category_department_id int(11); INSERT INTO categories values (NULL,'TESTING');
   7. Importing data from catagories table (In categories_subset_6 directory) using '|' delimiter and category_id between 1 and 61 and encode null values for both string and non string columns.
   8. Import entire schema retail_db in a directory categories_subset_all_tables
````   

````properties
// 1. Import data from categories table, where category_id=22 (Data should be stored in categories_subset)
sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
  --username root \
  --password cloudera \
  --table categories \
  --where "category_id = 22" \
  --delete-target-dir \
--target-dir /user/cloudera/question72/categories_subset \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hdfs dfs -cat /user/cloudera/question72/categories_subset/part*

// 2. Import data from categories table, where category_id>22 (Data should be stored in categories_subset_2)
sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
  --username root \
  --password cloudera \
  --table categories \
  --where "category_id > 22" \
  --delete-target-dir \
--target-dir /user/cloudera/question72/categories_subset_2 \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hdfs dfs -cat /user/cloudera/question72/categories_subset_2/part*

// 3. Import data from categories table, where category_id between 1 and 22 (Data should be stored in categories_subset_3)
sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
  --username root \
  --password cloudera \
  --table categories \
  --where "category_id between 1 and 22" \
  --delete-target-dir \
--target-dir /user/cloudera/question72/categories_subset_3 \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hdfs dfs -cat /user/cloudera/question72/categories_subset_3/part*

// 4. While importing catagories data change the delimiter to '|' (Data should be stored in categories_subset_4)
sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
  --username root \
  --password cloudera \
  --table categories \
  --fields-terminated-by '|' \
  --delete-target-dir \
--target-dir /user/cloudera/question72/categories_subset_4 \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hdfs dfs -cat /user/cloudera/question72/categories_subset_4/part*

// 5. Importing data from catagories table and restrict the import to category_name,category id columns only with delimiter as '|' (In categories_subset_5 directory)
sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
  --username root \
  --password cloudera \
  --table categories \
  --columns "category_id, category_name" \
  --fields-terminated-by '|' \
  --delete-target-dir \
--target-dir /user/cloudera/question72/categories_subset_5 \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hdfs dfs -cat /user/cloudera/question72/categories_subset_5/part*

// 6. Add null values in the table using below SQL statement ALTER TABLE categories modify category_department_id int(11); INSERT INTO categories values (60, NULL, 'TESTING');
$ mysql -u root -p
mysql> use retail_db;
mysql> ALTER TABLE categories modify category_department_id int(11);
mysql> INSERT INTO categories values (60, NULL, 'TESTING');
mysql> exit;

// 7. Importing data from catagories table (In categories_subset_6 directory) using '|' delimiter and category_id between 1 and 61 and encode null values for both string and non string columns.
sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
  --username root \
  --password cloudera \
  --table categories \
  --where "category_id between 1 and 61" \
  --fields-terminated-by '|' \
  --null-string "NaN" \
  --null-non-string -999 \
  --delete-target-dir \
  --target-dir /user/cloudera/question72/categories_subset_6 \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hdfs dfs -cat /user/cloudera/question72/categories_subset_6/part*

// 8. Import entire schema retail_db in a directory categories_subset_all_tables
sqoop import-all-tables \
  --connect jdbc:mysql://quickstart.cloudera/retail_db \
  --username root \
  --password cloudera \
  --warehouse-dir /user/cloudera/question72/categories_subset_all_tables \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--autoreset-to-one-mapper

$ hdfs dfs -ls /user/cloudera/question72/categories_subset_all_tables
````