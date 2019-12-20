/*
Question 6: Correct
PreRequiste:
[Prerequisite section will not be there in actual exam]
Run below sqoop command to import customer table from mysql into hdfs to the destination /user/cloudera/problem6/customer/text as text file and fields seperated by tab character Only import customer_id,customer_fname,customer_city.

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table customers \
--fields-terminated-by '\t' \
--columns "customer_id,customer_fname,customer_city" \
--target-dir /user/cloudera/problem6/customer/text

Instructions:
Find all customers that lives 'Brownsville' city and save the result into HDFS.
Input folder is /user/cloudera/problem6/customer/text.

Output Requirement:
Result should be saved in /user/cloudera/problem6/customer_Brownsville Output file should be saved in Json format

[You will not be provided with any answer choice in actual exam.Below answers are just provided to guide you]
Important Information:
Please make sure you are running all your solutions on spark 1.6 since it will be default spark version provided by exam environment.
*/
sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --password cloudera \
  --username root \
  --table customers \
  --fields-terminated-by '\t' \
  --columns "customer_id,customer_fname,customer_city" \
  --delete-target-dir \
  --target-dir /user/cloudera/problem6/customer/text \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

case class Customer(id: Int,name: String,city: String)
val customers = sc.textFile("/user/cloudera/problem6/customer/text").map(line => line.split('\t')).map(r => Customer(r(0).toInt,r(1),r(2))).toDF
customers.show(10)
val custFilter = customers.filter("city = 'Brownsville'")
custFilter.show(10)
custFilter.toJSON.saveAsTextFile("/user/cloudera/problem6/customer_Brownsville")

$ hdfs dfs -cat /user/cloudera/problem6/customer_Brownsville/part-00000 | head -n 50