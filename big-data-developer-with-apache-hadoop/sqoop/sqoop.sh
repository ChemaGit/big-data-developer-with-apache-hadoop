#to see a list of available tools run
sqoop help
#list all tables in the loudacre database in MySQL
sqoop list-tables \
--connect jdbc:mysql://localhost/loudacre \
--username training
--password training
#perform a database query using the eval tool
sqoop eval \
--query "SELECT * FROM device LIMIT 20" \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training

sqoop eval \
--connect jdbc:mysql://localhost/loudacre \
--username training -P \
-e "INSERT INTO test VALUES(194765,129762, 'Pinto', '17/07/1969', 'Happy Man')"

#List database schemas present on a server.
sqoop list-databases \
--connect jdbc:mysql://localhost/loudacre \
--username training -P

#imports an entire database
#The option --autoreset-to-one-mapper is typically used with the import-all-tables tool to automatically handle tables without a primary key in a schema.
sqoop import-all-tables \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--autoreset-to-one-mapper
#we can exclude some tables
sqoop import-all-tables \
--connect jdbc:mysql://localhost/loudacre \
--username training -P \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--warehouse-dir /loudacre/almost-all-tables \
--exclude-tables "test,webpage,device,accountdevice,accounts" \
--autoreset-to-one-mapper

#use the --warehouse-dir option to specify a different base directory
sqoop import-all-tables \
--connect jdbc:mysql://localhost/loudacre \
--username training
--password training
--warehouse-dir /loudacre

#import a single table
sqoop import --table accounts \
--connnect jdbc:mysql://localhost/loudacre \
--username training
--password training
--warehouse-dir /loudacre
#import only specified columns from a single table
sqoop import --table accounts \
--connect jdbc:mysql://localhost/loudacre \
--username training
--password training
--columns "id, first_name,last_name,state"
--warehouse-dir /loudacre

sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--table accounts \
--columns "acct_num, acct_create_dt, first_name, last_name" \
--target-dir /loudacre/accounts \
--num-mappers 1

sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--table accounts \
--columns "acct_num,acct_close_dt, first_name,city,state,zipcode" \
--fields-terminated-by "-" \
--null-string "none" \
--null-non-string "0" \
--delete-target-dir \
--target-dir /loudacre/accounts \
--num-mappers 1
#import only matching rows from a single table
sqoop import --table accounts \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--where "state='CA'"
--warehouse-dir /loudacre

sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--table accounts \
--columns "acct_num, acct_close_dt, first_name, city, state, zipcode" \
--where "acct_num >= 5 and acct_num <= 30" \
--fields-terminated-by "@" \
--null-string "none" \
--null-non-string "0" \
--delete-target-dir \
--target-dir /loudacre/accounts \
--num-mappers 1

#import from a query
sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--query "select * from accounts a, accountdevice b where a.acct_num = b.account_id and \$CONDITIONS" \
-m 1 \
--target-dir /loudacre/accounts/join \
--delete-target-dir

sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--query "select * from accounts where acct_num between 1 and 22 and \$CONDITIONS" \
-m 1 \
--target-dir /loudacre/accounts/problem \
--delete-target-dir

#import from MYSQL to a HIVE table
sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--table accountdevice \
--where "account_id between 1 and 22" \
-m 1 \
--hive-import

sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training -P \
--table webpage \
--hive-import \
--hive-table hiveweb

#we can specify an altenate location
sqoop import-all-tables \
--connect jdbc:mysql://localhost/loudacre \
--username training
--password training \
--autoreset-to-one-mapper \
--target-dir /loudacre/customer_accounts
#specifying an alternate delimiter
sqoop import --table accounts \
--connect jdbc:mysql://localhost/loudacre \
--username training
--password training
--fields-terminated-by "\t" \
--target-dir /loudacre/customer_accounts

sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--table accounts \
--fields-terminated-by "|" \
--escaped-by \\ \
--enclosed-by '\"' \
--delete-target-dir \
--target-dir /loudacre/accounts \
--num-mappers 1

#storing data in a compressed file
sqoop import --table accounts \
--connect jdbc:mysql://localhost/loudacre \
--username training
--password training
--fields-terminated by "\t"
--target-dir /loudacre/customer_accounts \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec

sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--table accounts \
--delete-target-dir \
--target-dir /loudacre/accounts \
--compression-codec org.apache.hadoop.io.compress.GzipCodec \
--num-mappers 1

sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--table accounts \
--compress \  #compression in default mode .gz
--delete-target-dir \
--target-dir /loudacre/compress \
--num-mappers 1

#sqoop supports importing data as Parquet or Avro files
sqoop import --table accounts \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--as-parquetfile \
--target-dir /loudacre/customer_accounts

sqoop import --table accounts \
--connnect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--as-avrodatafile \
--target-dir /loudacre/customer_accounts

#sqoop provides an incremental import mode which can be used to retrieve only rows newer than some previously-imported set of rows.
sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
-P \
--table accounts \
--null-string "none" \
--null-non-string "0"
--check-column acct_num \
--incremental append \
--last-value 1000 \
--as-avrodatafile \
--target-dir /loudacre/accounts \
--num-mappers 1

import \
--connect jdbc:mysql://localhost/loudacre \
--username training --password training \
--incremental append \
--table visitas \
--target-dir /visitas \
--check-column id \
--last-value 9 \
--fields-terminated-by ":" \
--outdir /tmp

sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--table accounts \
--incremental append \
--check-column acct_num \
--last-value 10 \
--target-dir /loudacre/accounts \
-m 1

#Sqoop supports export data from Hadoop to RDBMS with the export tool
sqoop export \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--export-dir /loudacre/recommender_output \
--update-mode allowinsert \
--table product_recommendations

sqoop export \
--connect jdbc:mysql://localhost/loudacre \
--username training -P \
--table test \
--update-mode allowinsert \
--export-dir /loudacre/test \
--input-null-string "null" \
--input-null-non-string "0" \
-m 1

$ sqoop export \
--connect jdbc:mysql://localhost/test \
--username training \
--password training \
--table departments_hive02 \
--update-mode allowinsert \
--export-dir /user/hive/warehouse/hadoopexam.db/departments_hive01 \
--input-fields-terminated-by '\001' \
--input-lines-terminated-by '\n' \
--input-null-string "" \
--input-null-non-string "-999" \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--validate \
--num-mappers 1

#create a hive table, only metastore data with a definition for a table based on a database table previously imported to HDFS, or one planned to be imported.
sqoop create-hive-table \
--connect jdbc:mysql://localhost/loudacre \
--username training  -P \
--table accounts \
--hive-table accounts

#The job tool allows you to create and work with saved jobs. Saved jobs remember the parameters used to specify a job, so they can be re-executed by invoking the job by its handle.
$ sqoop job \
--create myjob \
-- import \
--connect jdbc:mysql://localhost/retail_db \
--username training \
--password training \
--table categories \
--warehouse-dir /categories_target_job \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--num-mappers 1
#Step 2: list the jobs
$ sqoop job --list
#Step 3: Describe the job
$ sqoop job --show myjob
#Step 4: Execute the job
$ sqoop job --exec myjob