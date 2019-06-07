/** Question 82
  * Problem Scenario 40 : You have been given sample data as below in a file called /home/cloudera/files/file222.txt
  * 3070811,1963,1096,,"US","CA",,1,
  * 3022811,1963,1096,,"US","CA",,1,56
  * 3033811,1963,1096,,"US","CA",,1,23
  * Below is the code snippet to process this tile.
  * val field= sc.textFile("spark15/file1.txt")
  * val mapper = field.map(x=> A)
  * mapper.map(x => x.map(x=> {B})).collect
  * Please fill in A and B so it can generate below final output
  * Array(Array(3070811,1963,109G, 0, "US", "CA", 0,1, 0),Array(3022811,1963,1096, 0, "US", "CA", 0,1, 56),Array(3033811,1963,1096, 0, "US", "CA", 0,1, 23)
  */
$ gedit /home/cloudera/files/file222.txt &
  $ hdfs dfs -put /home/cloudera/files/file222.txt /user/cloudera/files

val field= sc.textFile("/user/cloudera/files/file222.txt")
val mapper = field.map(x => x.split(","))
mapper.map(x => x.map(x => {if(x.isEmpty || x == "" || x == " ") 0 else x})).collect

// res2: Array[Array[Any]] = Array(Array(3070811, 1963, 1096, 0, "US", "CA", 0, 1, 0), Array(3022811, 1963, 1096, 0, "US", "CA", 0, 1, 56), Array(3033811, 1963, 1096, 0, "US", "CA", 0, 1, 23))