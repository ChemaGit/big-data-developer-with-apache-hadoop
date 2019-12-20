/**
Pre-Work: Please perform these steps before solving the problem
1. Login to MySQL using below commands on a fresh terminal window
    mysql -u retail_dba -p
    Password = cloudera
2. Create a replica product table and name it products_replica
    create table products_replica as select * from products
3. Add primary key to the newly created table
    alter table products_replica add primary key (product_id);
4. Add two more columns
    alter table products_replica add column (product_grade int, product_sentiment varchar(100))
5. Run below two update statements to modify the data
    update products_replica set product_grade = 1  where product_price > 500;
    update products_replica set product_sentiment  = 'WEAK'  where product_price between 300 and  500;

Problem 5: Above steps are important so please complete them successfully before attempting to solve the problem
1. Using sqoop, import products_replica table from MYSQL into hdfs such that fields are separated by a '|' and lines are separated by '\n'. 
   Null values are represented as -1 for numbers and "NOT-AVAILABLE" for strings. 
   Only records with product id greater than or equal to 1 and less than or equal to 1000 should be imported and use 3 mappers for importing. 
   The destination file should be stored as a text file to directory  /user/cloudera/problem5/products-text. 
2. Using sqoop, import products_replica table from MYSQL into hdfs such that fields are separated by a '*' and lines are separated by '\n'. 
   Null values are represented as -1000 for numbers and "NA" for strings. Only records with product id less than or equal to 1111 should be imported and use 2 mappers for importing. 
   The destination file should be stored as a text file to directory  /user/cloudera/problem5/products-text-part1. 
3. Using sqoop, import products_replica table from MYSQL into hdfs such that fields are separated by a '*' and lines are separated by '\n'. 
   Null values are represented as -1000 for numbers and "NA" for strings. Only records with product id greater than 1111 should be imported and use 5 mappers for importing. 
   The destination file should be stored as a text file to directory  /user/cloudera/problem5/products-text-part2.
4. Using sqoop merge data available in /user/cloudera/problem5/products-text-part1 and /user/cloudera/problem5/products-text-part2 to produce a new set of files in /user/cloudera/problem5/products-text-both-parts
5. Using sqoop do the following. Read the entire steps before you create the sqoop job.
	-create a sqoop job Import Products_replica table as text file to directory /user/cloudera/problem5/products-incremental. Import all the records.
	-insert three more records to Products_replica from mysql
	-run the sqoop job again so that only newly added records can be pulled from mysql
	-insert 2 more records to Products_replica from mysql
	-run the sqoop job again so that only newly added records can be pulled from mysql
	-Validate to make sure the records have not be duplicated in HDFS
6. Using sqoop do the following. Read the entire steps before you create the sqoop job.
	-create a hive table in database named problem5 using below command 
	-create table products_hive  (product_id int, product_category_id int, product_name string, product_description string, product_price float, product_imaage string,product_grade int,  product_sentiment string);
	-create a sqoop job Import Products_replica table as hive table to database named problem5. name the table as products_hive. 
	-insert three more records to Products_replica from mysql
	-run the sqoop job again so that only newly added records can be pulled from mysql
	-insert 2 more records to Products_replica from mysql
	-run the sqoop job again so that only newly added records can be pulled from mysql
	-Validate to make sure the records have not been duplicated in Hive table
7. Using sqoop do the following. .
	-insert 2 more records into products_hive table using hive. 
	-create table in mysql using below command   
	-create table products_external  (product_id int(11) primary Key, product_grade int(11), product_category_id int(11), product_name varchar(100), product_description varchar(100), 
		product_price float, product_impage varchar(500), product_sentiment varchar(100));
	-export data from products_hive (hive) table to (mysql) products_external table. 
	-insert 2 more records to Products_hive table from hive
	-export data from products_hive table to products_external table. 
	-Validate to make sure the records have not be duplicated in mysql table

	*/

/*
Pre-Work: Please perform these steps before solving the problem
1. Login to MySQL using below commands on a fresh terminal window
    mysql -u retail_dba -p
    Password = cloudera
2. Create a replica product table and name it products_replica
    create table products_replica as select * from products
3. Add primary key to the newly created table
    alter table products_replica add primary key (product_id);
4. Add two more columns
    alter table products_replica add column (product_grade int, product_sentiment varchar(100))
5. Run below two update statements to modify the data
    update products_replica set product_grade = 1  where product_price > 500;
    update products_replica set product_sentiment  = 'WEAK'  where product_price between 300 and  500;
*/
$ mysql -u root -p
mysql> use hadoopexam;
mysql> create table products_replica as select * from retail_db.products;
mysql> alter table products_replica add primary key (product_id);
mysql> alter table products_replica add column (product_grade int, product_sentiment varchar(100));
mysql> update products_replica set product_grade = 1  where product_price > 500;
mysql> update products_replica set product_sentiment  = 'WEAK'  where product_price between 300 and  500;

// 1. Using sqoop, import products_replica table from MYSQL into hdfs such that fields are separated by a '|' and lines are separated by '\n'.
//   Null values are represented as -1 for numbers and "NOT-AVAILABLE" for strings.
//   Only records with product id greater than or equal to 1 and less than or equal to 1000 should be imported and use 3 mappers for importing.
//   The destination file should be stored as a text file to directory  /user/cloudera/problem5/products-text.
sqoop import \
--connect jdbc:mysql://quickstart:3306/hadoopexam \
	--username root \
	--password cloudera \
	--table products_replica \
	--where "product_id between 1 and 1000" \
	--as-textfile \
	--fields-terminated-by '|' \
	--lines-terminated-by '\n' \
	--null-string "NOT-AVAILABLE" \
	--null-non-string -1 \
	--delete-target-dir \
	--target-dir /user/cloudera/problem5/products-text \
	--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 3

$ hdfs dfs -ls /user/cloudera/problem5/products-text
$ hdfs dfs -cat /user/cloudera/problem5/products-text/part* | head -n 20

// 2. Using sqoop, import products_replica table from MYSQL into hdfs such that fields are separated by a '*' and lines are separated by '\n'.
//   Null values are represented as -1000 for numbers and "NA" for strings. Only records with product id less than or equal to 1111 should be imported and use 2 mappers for importing.
//   The destination file should be stored as a text file to directory  /user/cloudera/problem5/products-text-part1.
sqoop import \
--connect jdbc:mysql://quickstart:3306/hadoopexam \
	--username root \
	--password cloudera \
	--table products_replica \
	--where "product_id <= 1111" \
	--as-textfile \
	--fields-terminated-by '*' \
	--lines-terminated-by '\n' \
	--null-string "NA" \
	--null-non-string -1000 \
	--delete-target-dir \
	--target-dir /user/cloudera/problem5/products-text-part1 \
	--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 2

$ hdfs dfs -ls /user/cloudera/problem5/products-text-part1
$ hdfs dfs -cat /user/cloudera/problem5/products-text-part1/part* | head -n 20

// 3. Using sqoop, import products_replica table from MYSQL into hdfs such that fields are separated by a '*' and lines are separated by '\n'.
//   Null values are represented as -1000 for numbers and "NA" for strings. Only records with product id greater than 1111 should be imported and use 5 mappers for importing.
//   The destination file should be stored as a text file to directory  /user/cloudera/problem5/products-text-part2.
sqoop import \
--connect jdbc:mysql://quickstart:3306/hadoopexam \
	--username root \
	--password cloudera \
	--table products_replica \
	--where "product_id > 1111" \
	--as-textfile \
	--fields-terminated-by '*' \
	--lines-terminated-by '\n' \
	--null-string "NA" \
	--null-non-string -1000 \
	--delete-target-dir \
	--target-dir /user/cloudera/problem5/products-text-part2 \
	--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 5

$ hdfs dfs -ls /user/cloudera/problem5/products-text-part2
$ hdfs dfs -cat /user/cloudera/problem5/products-text-part2/part* | head -n 20

// 4. Using sqoop merge data available in /user/cloudera/problem5/products-text-part1 and /user/cloudera/problem5/products-text-part2 to produce a new set of files in /user/cloudera/problem5/products-text-both-parts
sqoop merge \
--jar-file /home/cloudera/bindir/products_replica.jar \
	--class-name products_replica \
--merge-key "product_id" \
	--new-data /user/cloudera/problem5/products-text-part2 \
	--onto /user/cloudera/problem5/products-text-part1 \
	--target-dir /user/cloudera/problem5/products-text-both-parts

$ hdfs dfs -ls /user/cloudera/problem5/products-text-both-parts
$ hdfs dfs -cat /user/cloudera/problem5/products-text-both-parts/part-r-00000 | head -n 20
$ hdfs dfs -cat /user/cloudera/problem5/products-text-both-parts/part-r-00000 | tail -n 20

// 5. Using sqoop do the following. Read the entire steps before you create the sqoop job.
//	-create a sqoop job Import Products_replica table as text file to directory /user/cloudera/problem5/products-incremental. Import all the records.
//	-insert three more records to Products_replica from mysql
//	-run the sqoop job again so that only newly added records can be pulled from mysql
//	-insert 2 more records to Products_replica from mysql
//	-run the sqoop job again so that only newly added records can be pulled from mysql
//	-Validate to make sure the records have not be duplicated in HDFS
sqoop job --create products_replica \
	-- import \
--connect jdbc:mysql://quickstart:3306/hadoopexam \
	--username root \
	--password cloudera \
	--table products_replica \
	--as-textfile \
	--incremental append \
	--check-column "product_id" \
	--target-dir /user/cloudera/problem5/products-incremental \
	--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ sqoop job --exec products_replica
	$ hdfs dfs -ls /user/cloudera/problem5/products-incremental
$ hdfs dfs -cat /user/cloudera/problem5/products-incremental/part* | tail -n 20

mysql> insert into products_replica values (1346,2,"Dell Computer","A very good computer",300.00,"not avaialble",3,"STRONG");
mysql> insert into products_replica values (1347,5,"Headphones Cool","Noisy headphones",356.00,"not avaialble",3,"STRONG");
mysql> insert into products_replica values (1348,10,"Smart TV","Plus Size",1350.00,"not avaialble",3,"STRONG");

$ sqoop job --exec products_replica
	$ hdfs dfs -ls /user/cloudera/problem5/products-incremental
$ hdfs dfs -cat /user/cloudera/problem5/products-incremental/part* | tail -n 20

mysql> insert into products_replica values (1349,4,"HP Computer","A regular computer",150.00,"not avaialble",1,"WEAK");
mysql> insert into products_replica values (1350,6,"Microphone Cool","Good Microphone",25.00,"not avaialble",3,"STRONG");

$ sqoop job --exec products_replica
	$ hdfs dfs -ls /user/cloudera/problem5/products-incremental
$ hdfs dfs -cat /user/cloudera/problem5/products-incremental/part* | tail -n 20

// 6. Using sqoop do the following. Read the entire steps before you create the sqoop job.
//	-create a hive table in database named problem5 using below command
//	-create table products_hive  (product_id int, product_category_id int, product_name string, product_description string, product_price float, product_imaage string,product_grade int,  product_sentiment string);
//	-create a sqoop job Import Products_replica table as hive table to database named problem5. name the table as products_hive.
//	-insert three more records to Products_replica from mysql
//	-run the sqoop job again so that only newly added records can be pulled from mysql
//	-insert 2 more records to Products_replica from mysql
//	-run the sqoop job again so that only newly added records can be pulled from mysql
//	-Validate to make sure the records have not been duplicated in Hive table

$ hive
	hive> create database problem5;
hive> use problem5;
hive> create table products_hive  (product_id int, product_category_id int, product_name string, product_description string, product_price float, product_imaage string,product_grade int,  product_sentiment string);

sqoop job --create products_replica_hive \
	-- import \
--connect jdbc:mysql://quickstart:3306/hadoopexam \
	--username root \
	--password cloudera \
	--table products_replica \
	--as-textfile \
	--hive-import \
--hive-database problem5 \
--hive-table products_hive \
--incremental append \
	--check-column "product_id" \
	--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ sqoop job --exec products_replica_hive
	hive> select * from products_hive;

$ mysql -u root -p
mysql> use hadoopexam;
mysql> insert into products_replica values (1351,2,"Dell Computer","A very good computer",300.00,"not avaialble",3,"STRONG");
mysql> insert into products_replica values (1352,5,"Headphones Cool","Noisy headphones",356.00,"not avaialble",3,"STRONG");
mysql> insert into products_replica values (1353,10,"Smart TV","Plus Size",1350.00,"not avaialble",3,"STRONG");

$ sqoop job --exec products_replica_hive
	hive> select * from products_hive;
hive> select count(*) from products_hive;

mysql> insert into products_replica values (1354,5,"Headphones Cool","Noisy headphones",356.00,"not avaialble",3,"STRONG");
mysql> insert into products_replica values (1355,10,"Smart TV","Plus Size",1350.00,"not avaialble",3,"STRONG");

$ sqoop job --exec products_replica_hive
	hive> select * from products_hive;
hive> select count(*) from products_hive;

// 7. Using sqoop do the following. .
//	-insert 2 more records into products_hive table using hive.
//	-create table in mysql using below command
//	-create table products_external  (product_id int(11) primary Key, product_grade int(11), product_category_id int(11), product_name varchar(100), product_description varchar(100),
//		product_price float, product_impage varchar(500), product_sentiment varchar(100));
//	-export data from products_hive (hive) table to (mysql) products_external table.
//	-insert 2 more records to Products_hive table from hive
//	-export data from products_hive table to products_external table.
//	-Validate to make sure the records have not be duplicated in mysql table
$ hive
	hive> use problem5;
hive> insert into table products_hive values (1356,5,"Headphones Cool","Noisy headphones",356.00,"not avaialble",3,"STRONG");
hive> insert into table products_hive values (1357,5,"Headphones Cool","Noisy headphones",356.00,"not avaialble",3,"STRONG");
hive> select * from products_hive;

$ mysql -u root -p
mysql> use hadoopexam;
mysql> create table products_external(product_id int(11) primary Key, product_grade int(11), product_category_id int(11), product_name varchar(100), product_description varchar(100),product_price float, product_impage varchar(500), product_sentiment varchar(100));

sqoop export \
--connect jdbc:mysql://quickstart:3306/hadoopexam \
	--username root \
	--password cloudera \
	--table products_external \
	--update-key "product_id" \
	--update-mode allowinsert \
--columns "product_id,product_category_id,product_name,product_description,product_price,product_impage,product_grade,product_sentiment" \
	--export-dir /user/hive/warehouse/problem5.db/products_hive \
	--input-fields-terminated-by '\001' \
	--input-lines-terminated-by '\n' \
	--input-null-string "IMPAGE" \
	--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

mysql> select * from products_external;
mysql> select count(*) from products_external;