/** Question 74
  * Problem Scenario 45 : You have been given 2 files , with the content as given Below
  * (technology.txt)
  * (salary.txt)
  * (technology.txt)
  * first,last,technology
  * Amit,Jain,java
  * Lokesh,kumar,unix
  * Mithun,kale,spark
  * Rajni,vekat,hadoop
  * Rahul,Yadav,scala
  * (salary.txt)
  * first,last,salary
  * Amit,Jain,100000
  * Lokesh,kumar,95000
  * Mithun,kale,150000
  * Rajni,vekat,154000
  * Rahul,Yadav,120000
  * Write a Spark program, which will join the data based on first and last name and save the joined results in following format, first Last.technology.salary
  */
$ gedit /user/cloudera/files/technology.txt
$ gedit /user/cloudera/files/salary.txt
$ hdfs dfs -put /user/cloudera/files/technology.txt /user/cloudera/files
$ hdfs dfs -put /user/cloudera/files/salary.txt /user/cloudera/files

val tech = sc.textFile("/user/cloudera/files/technology.txt").map(line => line.split(",")).map(r => ( (r(0),r(1)),r(2) ))
val  salary = sc.textFile("/user/cloudera/files/salary.txt").map(line => line.split(",")).map(r => ( (r(0),r(1)),r(2) ))
val joined = tech.join(salary).map({case( ( (f,l),(t,s)) ) => "%s,%s,%s,%s".format(f,l,t,s)})
joined.repartition(1).saveAsTextFile("/user/cloudera/question74/result")

$ hdfs dfs -cat /user/cloudera/question74/result/part*