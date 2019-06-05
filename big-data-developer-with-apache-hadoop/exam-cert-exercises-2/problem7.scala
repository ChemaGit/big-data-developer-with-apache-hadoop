/*
Question 7: Correct
PreRequiste:
[PreRequiste will not be there in actual exam]
Run below sqoop command to import customer table from mysql into hdfs to the destination /user/cloudera/problem2/customer/tab

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table customers \
--target-dir /user/cloudera/problem2/customer/tab \
--fields-terminated-by "\t" \
--columns "customer_id,customer_fname,customer_state"

Instructions:
Provided tab delimited file, get total numbers customers in each state whose first name starts with 'M' and save results in HDFS in json format.

Input folder
/user/cloudera/problem2/customer/tab

Output Requirement:
Result should be saved in /user/cloudera/problem2/customer_json_new.
Output should have state name followed by total number of customers in that state.
*/

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --password cloudera \
  --username root \
  --table customers \
  --delete-target-dir \
  --target-dir /user/cloudera/problem2/customer/tab \
  --fields-terminated-by "\t" \
  --columns "customer_id,customer_fname,customer_state" \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

val customers = sc.textFile("/user/cloudera/problem2/customer/tab").map(line => line.split("\t")).map(r => (r(0).toInt,r(1),r(2))).toDF("id","name","state")
customers.registerTempTable("customers")
sqlContext.sql("""SELECT state, COUNT(id) as total_customers FROM customers WHERE name LIKE("M%") GROUP BY state""").show()
val result = sqlContext.sql("""SELECT state, COUNT(id) as total_customers FROM customers WHERE name LIKE("M%") GROUP BY state""")
result.toJSON.repartition(1).saveAsTextFile("/user/cloudera/problem2/customer_json_new")

$ hdfs dfs -ls /user/cloudera/problem2/customer_json_new
$ hdfs dfs -cat /user/cloudera/problem2/customer_json_new/part-00000 | head -n 50