  /** 
   * RDD Persistence 2
   */
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.SparkSession

  object Persistence2 {

    val spark = SparkSession
      .builder()
      .appName("Persistence2")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
      .config("spark.app.id", "Persistence2") // To silence Metrics warning
      .getOrCreate()

    val sc = spark.sparkContext

    def main(args: Array[String]): Unit = {

      Logger.getRootLogger.setLevel(Level.ERROR)

      try {

        // Count web server log requests by user id
        val userReqs = sc.textFile("/loudacre/weblogs/*2.log").
          map(line => line.split(' ')).
          map(words => (words(2),1)).
          reduceByKey((v1,v2) => v1 + v2)

        // Map account data to (userid,"lastname,firstname") pairs
        val accounts = sc.textFile("/loudacre/accounts/*").
          map(line => line.split(',')).
          map(values => (values(0),values(4) + ',' + values(3)))

        // Join account names with request counts
        val accountHits = accounts.join(userReqs).map(pair => pair._2)

        accountHits.filter(pair => pair._2 > 5).count()

        accountHits.persist()

        accountHits.filter(pair => pair._2 > 5).count()
        accountHits.toDebugString

        accountHits.unpersist()

        import org.apache.spark.storage.StorageLevel._

        accountHits.persist(DISK_ONLY)

        accountHits.filter(pair => pair._2 > 5).count()
        accountHits.toDebugString

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