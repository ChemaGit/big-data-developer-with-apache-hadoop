val myDir = "/loudacre/json/"
val myrdd = sc.wholeTextFiles(myDir)
val myRdd2 = myrdd.map(pair => pair._2)
myRdd2.collect().foreach(println)

import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
val df = sqlContext.read.json(myRdd2)