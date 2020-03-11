import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * carpeta compartida tres ficheros.
  * empleado nombre: codigo, nombre
  * empleado sueldo
  * empleado, jefe
  *
  * crear un fichero que contenga las lineas de la siguiente manera
  * E01, pedro, 3500, azucena
  * ordenados por idEmpleado
  * idEmpleado, Empleado, Sueldo, Jefe
  *
  * Exactamente lo mismo ordenados por el sueldo de mayor a menor
  */

object Employee {

 val spark = SparkSession
   .builder()
   .appName("Employee")
   .master("local[*]")
   .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
   .config("spark.app.id", "Employee") // To silence Metrics warning
   .getOrCreate()

 val sc = spark.sparkContext

 val dirEmp = "/loudacre/employee/"

 def main(args: Array[String]): Unit = {

  Logger.getRootLogger.setLevel(Level.ERROR)

  try {

   val rddEmp = sc.textFile(s"${dirEmp}EmpleadoNombre.csv").map({case(line) => line.split(",")})
   val rddJefe = sc.textFile(s"${dirEmp}EmpleadoJefe.csv").map({case(line) => line.split(",")})
   val rddSueldo = sc.textFile(s"${dirEmp}EmpleadoSueldo.csv").map({case(line) => line.split(",")})

   val rddE = rddEmp.map({case(arr) => (arr(0), arr(1))})
   val rddJ = rddJefe.map({case(arr) => (arr(0), arr(1))})
   val rddS = rddSueldo.map({case(arr) => (arr(0), arr(1))})

   val rddJoin = rddE.join(rddS).join(rddJ).sortByKey().cache

   val pattern = rddJoin.map({case( (idEmp,((emp, sal),boss) )) => idEmp + " " + emp + " " + sal + " " + boss})
   pattern.saveAsTextFile("/loudacre/employee/res")

   val rddJoinB = rddJoin.map({case( (id,((emp,sal),boss)) ) => (id,emp,sal.toInt,boss) }).sortBy(p => p._3, ascending=false)
   val patternB = rddJoinB.map({case(id,emp,sal,boss) => id + " " + emp + " " + sal + " " + boss})
   patternB.saveAsTextFile("/loudacre/employee/res1")

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