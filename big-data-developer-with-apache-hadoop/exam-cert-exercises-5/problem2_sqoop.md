# Question 2:
````text
Instructions:
Connect to mySQL database using sqoop, import all customers whose street name contains "Plaza" . Example:
Hazy Mountain Plaza
Tawny Fox Plaza .

Data Description:
A mysql instance is running on the gateway node.In that instance you will find customers table that contains customers data.
> Installation : on the cluser node gateway
> Database name: retail_db
> Table name: Customers
> Username: root
> Password: cloudera

Output Requirement:
Place the customers files in HDFS directory "/user/cloudera/problem1/customers/textdata"
Save output in text format with fields seperated by a '*' and lines should be terminated by pipe
Load only "Customer id, Customer fname, Customer lname and Customer street name"
Sample Output
11942*Mary*Bernard*Tawny Fox Plaza|10480*Robert*Smith*Lost Horse Plaza|.................................
````

````bash
sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table customers \
--columns "customer_id,customer_fname,customer_lname,customer_street" \
--as-textfile \
--fields-terminated-by "*" \
--lines-terminated-by "|" \
--delete-target-dir \
--target-dir  /user/cloudera/problem1/customers/textdata \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

hdfs dfs -ls /user/cloudera/problem1/customers/textdata
hdfs dfs -cat /user/cloudera/problem1/customers/textdata/p* | tail -n 20
````

