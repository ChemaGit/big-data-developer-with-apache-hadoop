# Question 4:
````text
Instructions:
Connect to mySQL database using sqoop, import all products into a metastore table named product_sample.

Data Description:
A mysql instance is running on the gateway node.In that instance you will find customers table that contains customers data.
> Installation : on the cluser node gateway
> Database name: retail_db
> Table name: Products
> Username: root
> Password: cloudera

Output Requirement:
product_sample table does not exist in metastore.
Fields should be separated format separated by a '^'
````

````bash
$ sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table products \
--as-textfile \
--fields-terminated-by "^" \
--hive-import \
--hive-database default \
--hive-table "product_sample" \
--create-hive-table \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hive 
hive> use default;
hive> show tables;
hive> describe formatted product_sample;
hive> select * from product_sample limit 10;
````

