/** Question 65
  * Problem Scenario 91 : You have been given data in json format as below.
  * {"first_name":"Ankit", "last_name":"Jain"}
  * {"first_name":"Amir", "last_name":"Khan"}
  * {"first_name":"Rajesh", "last_name":"Khanna"}
  * {"first_name":"Priynka", "last_name":"Chopra"}
  * {"first_name":"Kareena", "last_name":"Kapoor"}
  * {"first_name":"Lokesh", "last_name":"Yadav"}
  * Do the following activity
  * 1. create employee.json file locally.
  * 2. Load this tile on hdfs
  * 3. Register this data as a temp table in Spark using scala.
  * 4. Write select query and print this data.
  * 5. Now save back this selected data as table in format orc. /user/cloudera/question65/orc
  * 6. Now save back this selected data as parquet file compressed in snappy codec in HDFS /user/cloudera/question65/parquet-snappy
  */
$ gedit /home/cloudera/files/employee.json &
  $ hdfs dfs -put /home/cloudera/files/employee.json /user/cloudera/files

val employee = sqlContext.read.json("/user/cloudera/files/employee.json")
employee.registerTempTable("employee")
sqlContext.sql("""select * from employee""").show()
employee.write.orc("/user/cloudera/question65/orc")
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
employee.write.parquet("/user/cloudera/question65/parquet-snappy")

$ hdfs dfs -ls /user/cloudera/question65/orc
$ hdfs dfs -text /user/cloudera/question65/orc/part-r-00000-1321d325-f0e1-4742-9d5f-5cf5daebabf0.orc

$ hdfs dfs -ls /user/cloudera/question65/parquet-snappy
$ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/question65/parquet-snappy/part-r-00000-9c6e9286-8d47-4274-9391-b26bdc66f3bb.snappy.parquet
$ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/question65/parquet-snappy/part-r-00000-9c6e9286-8d47-4274-9391-b26bdc66f3bb.snappy.parquet