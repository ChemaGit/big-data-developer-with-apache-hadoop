/** Question 78
  * Problem Scenario 35 : You have been given a file named spark7/EmployeeName.csv (id,name).
  * EmployeeName.csv
  * E01,Lokesh
  * E02,Bhupesh
  * E03,Amit
  * E04,Ratan
  * E05,Dinesh
  * E06,Pavan
  * E07,Tejas
  * E08,Sheela
  * E09,Kumar
  * E10,Venkat
  * 1. Load this file from hdfs and sort it by name and save it back as (id,name) in results directory. However, make sure while saving it should be able to write In a single file.
  */
$ gedit /home/cloudera/files/EmployeeName.csv
$ hdfs dfs -put /home/cloudera/files/EmployeeName.csv /user/cloudera/files

val emp = sc.textFile("/user/cloudera/files/EmployeeName.csv").map(line => line.split(",")).map(r => (r(0),r(1))).sortBy(t => t._2)
emp.map(t => "(%s,%s)".format(t._1,t._2)).repartition(1).saveAsTextFile("/user/cloudera/question78/result")

$ hdfs dfs -ls /user/cloudera/question78/result/
  $ hdfs dfs -cat /user/cloudera/question78/result/part-00000