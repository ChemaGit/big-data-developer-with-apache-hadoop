# Question 3: Correct
````text
Run below sqoop command to import customer table from mysql into hdfs

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table customers \
--target-dir /user/cloudera/problem3/customer/permissions

Instructions:
Change permissions of all the files under /user/cloudera/problem3/customer/permissions such that owner has read,write and execute permissions, 
group has read and write permissions whereas others have just read and execute permissions
````

````bash
$ sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table customers \
--target-dir /user/cloudera/problem3/customer/permissions \
--delete-target-dir \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir

$ hdfs dfs -ls /user/cloudera/problem3/customer/permissions
$ hdfs dfs -chmod 765 /user/cloudera/problem3/customer/permissions/*
$ hdfs dfs -ls /user/cloudera/problem3/customer/permissions
````

