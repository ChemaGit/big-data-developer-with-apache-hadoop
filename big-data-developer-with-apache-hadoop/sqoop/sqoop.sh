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
sqoop import-all-tables \
--connect jdbc:mysql://localhost/loudacre \
--username training
--password training
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
#import only matching rows from a single table
sqoop import --table accounts \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--where "state='CA'"
--warehouse-dir /loudacre
#we can specify an altenate location
sqoop import-all-tables \
--connect jdbc:mysql://localhost/loudacre \
--username training
--password training
--target-dir /loudacre/customer_accounts
#specifying an alternate delimiter
sqoop import --table accounts \
--connect jdbc:mysql://localhost/loudacre \
--username training
--password training
--fields-terminated-by "\t" \
--target-dir /loudacre/customer_accounts
#storing data in a compressed file
sqoop import --table accounts \
--connect jdbc:mysql://localhost/loudacre \
--username training
--password training
--fields-terminated by "\t"
--target-dir /loudacre/customer_accounts \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec
#sqoop supports importing data sa Parquet or Avro files
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
#Sqoop supports export data from Hadoop to RDBMS with the export tool
sqoop export \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--export-dir /loudacre/recommender_output \
--update-mode allowinsert \
--table product_recommendations