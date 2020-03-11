/**
  * En esta version hay que sacar la temperatura media por estacion y por año. La
  * diferencia con lo anterior es que ahora puede haber m�s a�os aparte del 1800. Por ejemplo.
  * 1800_1801.csv
  * ITE00100554,18000101,TMAX,-75,,,E,
  * ITE00100554,18000101,TMIN,-148,,,E,
  * GM000010962,18000101,PRCP,0,,,E,
  * EZE00100082,18000101,TMAX,-86,,,E,
  * ...
  * EZE00100082,18010115,TMIN,-23,,,E,
  * ITE00100554,18010116,TMAX,56,,,E,
  * ITE00100554,18010116,TMIN,41,,,E,
  * GM000010962,18010116,PRCP,0,,,E,
  * salida
  * 1800 EZE00100082 7.70F
  * 1800 ITE00100554 5.36F
  * 1801 EZE00100082 5.90F
  * 1801 ITE00100554 2.30F
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Temperatures1800_1801 {

 val spark = SparkSession
   .builder()
   .appName("Temperatures1800_1801")
   .master("local[*]")
   .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
   .config("spark.app.id", "Temperatures1800_1801")  // To silence Metrics warning
   .getOrCreate()

 val sc = spark.sparkContext

 val dirTemp = "/loudacre/temperatures/1800_1801.csv"

 def main(args: Array[String]): Unit = {

  Logger.getRootLogger.setLevel(Level.ERROR)

  try {

   /**
     * Calcular la media de las temperaturas  por estacion y año
     */

   val rddTemp = sc
     .textFile(dirTemp)
     .map({case(line) => line.split(",")})
     .map({case(arr) => ( (arr(0), arr(1).substring(0,4)), arr(2), arr(3).toDouble)})
     .cache

   val rddFilt = rddTemp.filter({case(k,p,t) => p == "TMIN" || p == "TMAX"}).map({case(k,p,t) => (k,t)})

   val groupRdd = rddFilt.groupByKey()

   val sumRdd = groupRdd.mapValues({case(v) => ((v.sum / v.size) * 0.1) * (9.0 / 5.0) + 32})

   val pattern = sumRdd.map({case(((e,y),t)) => y + "\t" + e + "\t" + "%.2fF".format(t)})

   pattern.saveAsTextFile("/loudacre/temperatures/media")

   /**
     * Calcular la media de las temperaturas minimas por estacion y año
     */
   val rddFilt1 = rddTemp.filter({case(k, p, t) => p == "TMIN"}).map({case(k, p, t) => (k, t)})

   val rddGroup = rddFilt1.groupByKey()

   val rddSum = rddGroup.mapValues(v => ((v.sum / v.size) * 0.1) * (9.0 / 5.0) + 32 )

   val pattern1 = rddSum.map({case(k,t) => k._2 + " " + k._1 + " " + "%.2fF".format(t)})

   pattern1.saveAsTextFile("/loudacre/temperatures/minMedia")
   /**
     * Calcular la media de las temperaturas maximas por estacion y año
     */
   val rddFilter = rddTemp.filter({case( ((e, y),p,t)) => p == "TMAX" }).keyBy({case( ((e, y),p,t) ) => (e,y)})

   val rddP = rddFilter.mapValues({case( ( (k), p, t) ) => t})

   val rddGroup1 = rddP.groupByKey().mapValues({case(it) => ((it.sum * 0.1) / it.size) * (9.0 / 5.0) + 32 })

   val rddPattern = rddGroup1.map({case( ( (e,y),t ) ) => y + "\t" + e + "\t" + "%.2fF".format(t)})

   rddPattern.saveAsTextFile("/loudacre/temperatures/maxMedia")

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
