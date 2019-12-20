/** Question 93
  * Problem Scenario 36 : You have been given a file named /home/cloudera/files/data.csv (type,name).
  * data.csv
  * 1,Lokesh
  * 2,Bhupesh
  * 2,Amit
  * 2,Ratan
  * 2,Dinesh
  * 1,Pavan
  * 1,Tejas
  * 2,Sheela
  * 1,Kumar
  * 1,Venkat
  * 1. Load this file from hdfs and save it back as (id, (all names of same type)) in results directory. However, make sure while saving it should be only one file.
  */
$ touch /home/cloudera/files/data.csv
$ gedit /home/cloudera/files/data.csv &
$ hdfs dfs -put /home/cloudera/files/data.csv /user/cloudera/files

val data = sc.textFile("/user/cloudera/files/data.csv").map(line => line.split(",")).map(r => (r(0),r(1))).groupByKey()
val format = data.map({case( (id, names)) => (id , names.toList.mkString("(",",",")"))})
format.saveAsTextFile("/user/cloudera/question93")

$ spark-shell -i probe.scala

$ hdfs dfs -cat /user/cloudera/question93/part*