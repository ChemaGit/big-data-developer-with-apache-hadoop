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
--username training
--password training
#imports an entire database
#The option --autoreset-to-one-mapper is typically used with the import-all-tables tool to automatically handle tables without a primary key in a schema.
sqoop import-all-tables \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
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
--delete-target-dir \
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

#Sqoop supports export data from Hadoop to RDBMS with the export tool
sqoop export \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--export-dir /loudacre/recommender_output \
--update-mode allowinsert \
--table product_recommendations