package spark.rdd-persistence

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

// the difference using Kryo vs Java
// In order to configure Kryo serialization we first set
// spark.serializer=org.apache.spark.serializer.KryoSerializer
// spark.kryo.registrator=labs.Lab4KryoRegistrator
// trips dataset using serialized storage, using up much less than the Java serialized version

//class LabKryoRegistrator extends KryoRegistrator {
//  override def registerClasses(kryo: Kryo): Unit = {
//    kryo.register(classOf[Trip])
//    kryo.register(classOf[Station])
//  }
//}

object PersistenceAndMemoryTuningWithKryoSerialization {
  //  val spark = SparkSession
  //    .builder()
  //    .appName("PersistenceAndMemoryTuningWithKryoSerialization")
  //    .master("local[*]")
  //    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
  //    .config("spark.app.id", "PersistenceAndMemoryTuningWithKryoSerialization")  // To silence Metrics warning
  //    .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
  //    .config("spark.kryo.registrator","spark_fundamentals_II.LabKryoRegistrator")
  //    .getOrCreate()

  val conf = new SparkConf()
  conf.setMaster("local[*]")
    .setAppName("PersistenceAndMemoryTuningWithKryoSerialization")
    .set("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .set("spark.app.id", "PersistenceAndMemoryTuningWithKryoSerialization") // To silence Metrics warning
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  conf.registerKryoClasses(Array(classOf[Trip],classOf[Station]))

  val sc = new SparkContext(conf)

  val input = "hdfs://quickstart.cloudera/public/cognitive_class/"

  def benchmark(rdd: RDD[(Int, Int)]): Long = {
    val start = System.currentTimeMillis()
    rdd.collect()
    val end = System.currentTimeMillis()
    (end - start)
  }

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val input1 = sc.textFile(s"${input}trips")
      val header1 = input1.first // to skip the header row
      val trips = input1
        .filter(_ != header1)
        .map(_.split(","))
        .map(utils.Trip.parse(_))

      // calculate the average duration of trips both by start and end terminals in the trips RDD
      val durationsByStart = trips
        .keyBy(_.startTerminal)
        .mapValues(_.duration)

      val durationsByEnd = trips
        .keyBy(_.endTerminal)
        .mapValues(_.duration)

      val resultsStart = durationsByStart
        .aggregateByKey((0, 0))((acc, value) => (acc._1 + value, acc._2 + 1),
          (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      val avgStart = resultsStart.mapValues(i => i._1 / i._2)

      val resultsEnd = durationsByEnd
        .aggregateByKey((0, 0))((acc, value) => (acc._1 + value, acc._2 + 1),
          (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      val avgEnd = resultsEnd.mapValues(i => i._1 / i._2)

      trips.persist(StorageLevel.MEMORY_ONLY_SER)
      // You'll noticed the top one (recent) uses less memory, why?
      val timeWithCachingAndSer = benchmark(avgStart) + benchmark(avgEnd)
      println(s"Duration with Caching and Serialization: $timeWithCachingAndSer")

      trips.unpersist()

      // Run it without any caching.
      val timeWithoutCaching = benchmark(avgStart) + benchmark(avgEnd)
      println(s"Duration without Caching: $timeWithoutCaching")

      // Now run it with the default cache() function.
      trips.persist(StorageLevel.MEMORY_ONLY)

      val timeWithCaching = benchmark(avgStart) + benchmark(avgEnd)
      println(s"Duration with Caching: $timeWithCaching")

      // Run it with persist(StorageLevel.MEMORY_ONLY_SER)
      trips.unpersist()

      // Open up the Spark UI. In this case, it is on port 4041. http://192.168.59.103:4041

      // To have the opportunity to view the web console of Spark: http://localhost:4041/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("SparkContext stopped.")
    }
  }
}