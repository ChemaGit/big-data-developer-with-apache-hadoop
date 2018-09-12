#Problem Scenario 4: You have been given MySQL DB with following details.
#user=training
#password=training
#database=loudacre
#table=accountdevice
#jdbc URL = jdbc:mysql://localhost/loudacre
#Please accomplish following activities.
#Import Single table accountdevice (Subset data} to hive managed table , where account_id
#between 1 and 22

sqoop import \
--connect jdbc:mysql://localhost/loudacre \
--username training \
--password training \
--table accountdevice \
--where "account_id between 1 and 22" \
-m 1 \
--hive-import