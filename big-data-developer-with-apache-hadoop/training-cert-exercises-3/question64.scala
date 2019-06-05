/** Question 64
  * Problem Scenario 34 : You have given a file named spark6/user.csv.
  * Data is given below:
  * user.csv
  * id,topic,hits
  * Rahul,scala,120
  * Nikita,spark,80
  * Mithun,spark,1
  * myself,cca175,180
  * Now write a Spark code in scala which will remove the header part and create RDD of values as below, for all rows. And also if id is myself" than filter out row.
  * Map(id -> om, topic -> scala, hits -> 120)
  */
$ gedit /home/cloudera/files/user.csv &
  $ hdfs dfs -put /home/cloudera/files/user.csv /user/cloudera/files

val user = sc.textFile("/user/cloudera/files/user.csv").map(lines => lines.split(","))
val header = user.first
val userFiltered = user.filter(r => r(0) != header(0)).filter(r => r(0) != "myself")
userFiltered.map(r => r.zip(header).toMap).collect
// res2: Array[scala.collection.immutable.Map[String,String]] = Array(Map(Rahul -> id, scala -> topic, 120 -> hits), Map(Nikita -> id, spark -> topic, 80 -> hits), Map(Mithun -> id, spark -> topic, 1 -> hits))