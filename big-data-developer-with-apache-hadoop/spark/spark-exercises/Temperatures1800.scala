import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Tenemos mediciones de temperatura del a�o 1800. Hay que averiguar por cada
  * estaci�n la temperatura m�nima del a�o. La temperatura actual son los grados Celsius
  * multiplicados por 10. En el resultado, hay que expresar la temperatura en Fahrenheit.
  * Es decir si t es la temperatura que hay en el fichero hay convertirla a
  * t*0,1*(9.0/5.0) + 32.
  * 1800.csv
  * ITE00100554,18000101,TMAX,-75,,,E,
  * ITE00100554,18000101,TMIN,-148,,,E,
  * GM000010962,18000101,PRCP,0,,,E,
  * EZE00100082,18000101,TMAX,-86,,,E,
  * EZE00100082,18000101,TMIN,-135,,,E,
  * ITE00100554,18000102,TMAX,-60,,I,E,
  * En donde:
  * ITE00100554: C�digo de estaci�n
  * 18000102: A�o mes d�a
  * TMIN: indica que la cifra que sigue es temperatura m�nima.
  * -60: Temperatura m�nima en Celsius multiplicada por 10.
  * El segundo campo siempre empieza por 1800.
  * salida
  * EZE00100082	 7.70F
  * ITE00100554	 5.36F
  */

object Temperatures1800 {

 val spark = SparkSession
   .builder()
   .appName("question54")
   .master("local[*]")
   .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
   .config("spark.app.id", "question54") // To silence Metrics warning
   .getOrCreate()

 val sc = spark.sparkContext

 val dirTemp = "/loudacre/temperatures/1800.csv"

 def main(args: Array[String]): Unit = {

  Logger.getRootLogger.setLevel(Level.ERROR)

  try {

   val rddTemp = sc.textFile(dirTemp).map(line => line.split(",")).map(arr => (arr(0), arr(2), arr(3))).cache

   val filtMin = rddTemp.filter(arr => arr._2 == "TMIN")
   val rddGroup = rddTemp.map({case(est, min, temp) => (est, temp.toFloat)}).reduceByKey({case(v, v1) => if(v < v1)v else v1})
   val rddFar = rddGroup.mapValues(v => (v * 0.1) * (9.0 / 5.0) + 32)
   val rddFormat = rddFar.map({case(e, v) => e + " " + "%.2fF".format(v)})
   rddFormat.saveAsTextFile("/loudacre/temperatures/resMin")

   /**
     * max-temperature
     * Lo mismo que min-temperatury pero calcular la temperatura m�xima.
     */
   val filtMax = rddTemp.filter(arr => arr._2 == "TMAX").cache

   val rddGroup = filtMax.map({case(est, pat,temp) => (est, temp.toDouble)}).reduceByKey({case(v, v1) => if(v > v1) v else v1})
   val rddFar = rddGroup.mapValues(v => (v * 0.1) * (9.0 /5.0) + 32)
   val rddFormat = rddFar.map({case(e, v) => e + " " + "%.2fF".format(v)})
   rddFormat.saveAsTextFile("/loudacre/temperatures/resMax")

   /**
     * max-temperatures_groupByKey
     * Lo mismo que max-temperature pero hay que usar groupByKey.
     */
   val rddGroup = filtMax.map({case(est, pat, temp) => (est, temp.toDouble)}).groupByKey()
   val rddFar = rddGroup.mapValues({case(it) => (it.max * 0.1) * (9.0 / 5.0) + 32})
   val rddFormat = rddFar.map({case(e, t) => e + " " + "%.2fF".format(t)})
   rddFormat.saveAsTextFile("/loudacre/temperatures/resMax_b")

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