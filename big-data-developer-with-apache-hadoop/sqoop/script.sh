#!/bin/bash

# Using tab delimited
sqoop import \
--connect jdbc:mysql://dev.loudacre.com/loudacre \
--username training --password training \
--table accounts \
--target-dir account-tabbed \
--fields-terminated-by "\t"

# Using sequence file
sqoop import \
--connect jdbc:mysql://dev.loudacre.com/loudacre \
--username training --password training \
--table accounts \
--target-dir account-seq \
--as-sequencefile

# Using Avro
sqoop import \
--connect jdbc:mysql://dev.loudacre.com/loudacre \
--username training --password training \
--table accounts \
--target-dir account-avro \
--as-avrodatafile

# Using a custom query
sqoop import \
--connect jdbc:mysql://dev.loudacre.com/loudacre \
--username training --password training \
--table accounts \
--target-dir account-active-ca \
--where 'state = "CA" and acct_close_dt IS NULL'

#Import table from MySQL to HIVE

sqoop import \
--table accounts \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--null-non-string '\\N' \
--split-by acct_num \
--fields-terminated-by "," \
--hive-import --create-hive-table --hive-table accounts_sqoop \
--target-dir /loudacre/accounts