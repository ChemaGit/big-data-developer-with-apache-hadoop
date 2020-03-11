//write	code to go through	a set of activation XML files and
//extract the account number and device	model for each activation, and save the	list to
//a file as

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.xml._

object ActivationsModels {

  val spark = SparkSession
    .builder()
    .appName("ActivationsModels")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "ActivationsModels") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val dir = "/loudacre/activations"

  // Given a string containing XML, parse the string, and
  // return an iterator of activation XML records (Nodes) contained in the string
  def getActivations(xmlstring: String): Iterator[Node] = {
    val nodes = XML.loadString(xmlstring) \\ "activation"
    nodes.toIterator
  }

  // Given an activation record (XML Node), return the model name
  def getModel(activation: Node): String = {
    (activation \ "model").text
  }

  // Given an activation record (XML Node), return the account number
  def getAccount(activation: Node): String = {
    (activation \ "account-number").text
  }

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val records = sc.wholeTextFiles(dir)
      val nodes = records.flatMap(file => getActivations(file._2))
      val result = nodes.map(record => getAccount(record) + ":" + getModel(getModel))
      result.saveAsTextFile("/loudacre/account-models")

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