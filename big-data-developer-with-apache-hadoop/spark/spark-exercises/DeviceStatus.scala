/**
  * Determine	which	delimiter	to	use	(hint:	the	character	at	position	19	is	the
  * first	use	of	the	delimiter).
  * Filter	out	any	records	which	do	not	parse	correctly	(hint:	each record	should
  * have	exactly	14	values).
  * Extract	the	date	(first	field),	model	(second	field),	device	ID	(third	field),	and
  * latitude	and	longitude	(13th	and	14th	fields	respectively).
  * The	second	field	contains	the	device	manufacturer	and	model	name	(such	as
  * Ronin S2).	Split	this	field	by	spaces	to	separate	the	manufacturer	from	the
  * model	(for	example,	manufacturer	Ronin,	model	S2).	Keep	just	the
  * manufacturer	name.
  * Save	the	extracted	data	to	comma-delimited	text	files	in	the
  * /loudacre/devicestatus_etl	directory	on	HDFS.
  * Confirm	that	the	data	in	the	file(s)	was	saved	correctly.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DeviceStatus {

 val spark = SparkSession
   .builder()
   .appName("DeviceStatus")
   .master("local[*]")
   .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
   .config("spark.app.id", "DeviceStatus") // To silence Metrics warning
   .getOrCreate()

 val sc = spark.sparkContext

 val dir = "/loudacre/devicestatus.txt"

 def main(args: Array[String]): Unit = {

  Logger.getRootLogger.setLevel(Level.ERROR)

  try {

   val lines = sc.textFile(dir).map(line => line.split(line(19)))
   val filt = lines.filter(arr => arr.length == 14)
   val extract = filt.map(line => (line(0),line(1),line(2),line(12),line(13)))
   val mod = extract.map(l => (l._1, l._2.split(" ")(0),l._3,l._4,l._5))
   val text = mod.map(l => l._1 + "," + l._2 + "," + l._3 + "," + l._4 + "," + l._5)
   text.saveAsTextFile("/loudacre/devicestatus_etl")

   // To have the opportunity to view the web console of Spark: http://localhost:4040/
   println("Type whatever to the console to exit......")
   scala.io.StdIn.readLine()
  } finally {
   sc.stop()
   println("SparkContext stopped.")
   spark.stop()
   println("SparkSession stopped.")
  }
 }
}
