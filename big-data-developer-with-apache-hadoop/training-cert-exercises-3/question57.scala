/** Question 57
  * Problem Scenario 73 : You have been given data in json format as below.
  * {"first_name":"Ankit", "last_name":"Jain"}
  * {"first_name":"Amir", "last_name":"Khan"}
  * {"first_name":"Rajesh", "last_name":"Khanna"}
  * {"first_name":"Priynka", "last_name":"Chopra"}
  * {"first_name":"Kareena", "last_name":"Kapoor"}
  * {"first_name":"Lokesh", "last_name":"Yadav"}
  * Do the following activity
  * 1. create employee.json file locally.
  * 2. Load this file on hdfs
  * 3. Register this data as a temp table in Spark using Scala.
  * 4. Write select query and print this data.
  * 5. Now save back this selected data in json format on the directory question57.
  */
$ gedit /home/cloudera/files/employee.json &
  $ hdfs dfs -put /home/cloudera/files/employee.json /user/cloudera/files
$ hdfs dfs -cat /user/cloudera/files/employee.json

val emp = sqlContext.read.json("/user/cloudera/files/employee.json")
emp.registerTempTable("employee")
sqlContext.sql("""select date_format(current_date,'dd/MM/yyyy') as date, first_name, last_name, concat(first_name,",",last_name) as full_name from employee""").show()
val result = sqlContext.sql("""select date_format(current_date,'dd/MM/yyyy') as date, first_name, last_name, concat(first_name,",",last_name) as full_name from employee""")
result.toJSON.repartition(1).saveAsTextFile("/user/cloudera/question57/json")

$ hdfs dfs -ls /user/cloudera/question57/json
$ hdfs dfs -cat /user/cloudera/question57/json/part-00000