val myDir = "/loudacre/json/"
val myrdd = sc.wholeTextFiles(myDir)
val myRdd2 = myrdd.map(pair => pair._2)
myRdd2.collect().foreach(println)

import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
val df = sqlContext.read.json(myRdd2)

/********************************************/
import scala.util.parsing.json.JSON 

val myDir = "/loudacre/json/"
val myrdd = sc.wholeTextFiles(myDir)
val myrdd2 = myrdd.map(pair => JSON.parseFull(pair._2).get.asInstanceOf[Map[String, String]])
myrdd2.take(10).foreach(rec => rec.mkString(","))
for(record <- myrdd2.take(2)) println(record.getOrElse("widget", null))