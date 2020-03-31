package spark.spark-sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/*
$ spark2-submit \
--class "DevelopAndTesting" \
--master local[4] \
target/scala-2.11/develop-and-testing_2.11-0.1.jar
*/
object DevelopAndTesting {

  /**
    * UDF function
    * @return
    */
  def distanceOf = (lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
    val earthRadius = 3963 - 13 * Math.sin(lat1)

    val dLat = Math.toRadians(lat2 - lat1)
    val dLon = Math.toRadians(lon2 - lon1)

    val a = Math.pow(Math.sin(dLat / 2), 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.pow(Math.sin(dLon / 2), 2)

    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))

    earthRadius * c
  }

  val input = "hdfs://quickstart.cloudera/public/cognitive_class/"

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().
      setAppName("DevelopAndTesting")
      .setMaster("local[4]"))

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    // register UDF function
    sqlContext.udf.register("distanceOf",distanceOf)

    Logger.getRootLogger.setLevel(Level.ERROR)

    val intput1 = sc.textFile(s"${input}trips/*")
    val header1 = intput1.first()

    val trips = intput1
      .filter(_ != header1)
      .map(_.split(","))
      .map(utils.Trip.parse(_))
      .toDF

    val input2 = sc.textFile(s"${input}stations/*")
    val header2 = input2.first()

    val stations = input2
      .filter(_ != header2)
      .map(_.split(","))
      .map(utils.Station.parse(_))
      .toDF

    println(s"nums trips: ${trips.count()}")
    println(s"nums stations: ${stations.count()}")
    trips.show(5)
    stations.show(5)

    stations.createOrReplaceTempView("stations")
    trips.createOrReplaceTempView("trips")

    val startTerminals = sqlContext
      .sql(
        """SELECT tr.id, tr.startStation, st.lat, st.lon
          | FROM trips tr
          | JOIN stations st
          | ON tr.startTerminal = st.id
        """.stripMargin)
      .cache()

    startTerminals.show(5)
    startTerminals.createOrReplaceTempView("start")

    val endTerminals = sqlContext
      .sql(
        """SELECT tr.id, tr.endStation, st.lat, st.lon
          | FROM trips tr
          | JOIN stations st
          | ON tr.endTerminal = st.id
        """.stripMargin)
      .cache()

    endTerminals.show(5)
    endTerminals.createOrReplaceTempView("end")

    val joined = sqlContext
      .sql(
        """SELECT start.id, distanceOf(start.lat, end.lat, start.lon, end.lon) AS distance
          |FROM start
          |JOIN end
          |ON (start.id = end.id)
        """.stripMargin)

    joined.show(10)

    println("Press [Enter] to quit")
    Console.readLine()

    sc.stop
  }
}