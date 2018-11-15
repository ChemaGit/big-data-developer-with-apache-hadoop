/** Question 72
 * Problem Scenario 3: You have been given MySQL DB with following details.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * table=retail_db.categories
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Please accomplish following activities.
 * 1. Import data from categories table, where category=22 (Data should be stored in categories subset)
 * 2. Import data from categories table, where category>22 (Data should be stored in categories_subset_2)
 * 3. Import data from categories table, where category between 1 and 22 (Data should be stored in categories_subset_3)
 * 4. While importing catagories data change the delimiter to '|' (Data should be stored in categories_subset_S)
 * 5. Importing data from catagories table and restrict the import to category_name,category id columns only with delimiter as '|'
 * 6. Add null values in the table using below SQL statement ALTER TABLE categories modify category_department_id int(11); INSERT INTO categories values (eO.NULL.'TESTING');
 * 7. Importing data from catagories table (In categories_subset_17 directory) using '|' delimiter and categoryjd between 1 and 61 and encode null values for both string and non string columns.
 * 8. Import entire schema retail_db in a directory categories_subset_all_tables
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution: 
//Step 1: Import Single table (Subset data) 
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table categories \
--warehouse-dir categories_subset \ 
--where "category_id = 22" \ 
--m 1 

//Step 2 : Check the output partition 
$ hdfs dfs -cat categories_subset/categories/part-m-00000 

//Step 3 : Change the selection criteria (Subset data) 
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \ 
--password cloudera \ 
--table categories \ 
--warehouse-dir categories_subset_2 \ 
--where "category_id > 22" \ 
--m 1 

//Step 4 : Check the output partition 
$ hdfs dfs -cat categories_subset_2/categories/part-m-00000 

//Step 5 : Use between clause (Subset data) 
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \ 
--password cloudera \ 
--table categories \ 
--warehouse-dir categories_subset_3 \ 
--where "category_id between 1 and 22" 
--m 1 

//Step 6 : Check the output partition 
$ hdfs dfs -cat categories_subset_3/categories/part-m-00000 

//Step 7 : Changing the delimiter during import. 
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \ 
--password cloudera \ 
--table categories \ 
--warehouse-dir categories_subset_6 \
--where "/categoryjd / between 1 and 22" \ 
--fields-terminated-by '|' \
--m 1 

//Step 8 : Check the.output partition 
$ hdfs dfs -cat categories_subset_6/categories/part-m-00000 

//Step 9 : Selecting subset columns 
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table categories \
--warehouse-dir categories_subset_col \ 
--where "category_id between 1 and 22" \ 
--fields-terminated-by '|' \
--columns "category_name,category_id" \ 
--m 1 

//Step 10 : Check the output partition 
$ hdfs dfs -cat categories_subset_col/categories/part-m-00000 

//Step 11 : Inserting record with null values (Using mysql) 
ALTER TABLE categories modify category_department_id int(11); 
ALTER TABLE categories modify category_name varchar(45); 
INSERT INTO categories values (NULL,"TESTING"); 
select * from categories; 

//Step 12 : Encode non string null column 
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table categories \
--warehouse-dir categortes_subset_17 \
--where "category_id between 1 and 61" \ 
--fields-terminated-by '|' \
--null-string 'N' \
--null-non-string 'N' \ 
--m 1 

//Step 13 : View the content 
$ hdfs dfs -cat categories_subset_17/categories/part-m-00000 

//Step 14 : Import all the tables from a schema (This step will take little time) 
sqoop import-all-tables \
--connect jdbc:mysql://quickstart:3306/retail_db \ 
--username retail_dba \
--password cloudera \
--warehouse-dir categories_subset_all_tables \
--autoreset-to-one-mapper

//Step 15 : View the contents 
$ hdfs dfs -ls categories_subset_all_tables 

//Step 16 : Cleanup or back to originals. 
delete from categories where category_id in (59,60); 
ALTER TABLE categories modify category_department_id int(11) NOT NULL; 
ALTER TABLE categories modify category_name varchar(45) NOT NULL; 
desc categories;