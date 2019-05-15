/** Question 32
  * Problem Scenario 39 : You have been given two files
  * spark16/file1.txt
  * 1,9,5
  * 2,7,4
  * 3,8,3
  * spark16/file2.txt
  * 1,g,h
  * 2,i,j
  * 3,k,l
  * Load these two tiles as Spark RDD and join them to produce the below results
  * (1,((9,5),(g,h))) (2, ((7,4), (i,j))) (3, ((8,3), (k,l)))
  * And write code snippet which will sum the second columns of above joined results (5+4+3).
  */
val file1 = sc.textFile("/user/cloudera/files/file11.txt").map(line => line.split(",")).map(r => (r(0),(r(1),r(2).toInt)))
val file2 = sc.textFile("/user/cloudera/files/file22.txt").map(line => line.split(",")).map(r => (r(0),(r(1),r(2))))
val joined = file1.join(file2)
val result = joined.map({case((_,((_,v),(_,_)))) => v}).reduce( (v,c) => v + c)