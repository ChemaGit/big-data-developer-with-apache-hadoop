package spark.spark-common-patterns
/**
  * // download data from IBM Servier
  * // this may take ~30 seconds depending on your internet speed
  * //"wget --quiet https://cocl.us/BD0211EN_Data" !
  * //println("Data Downloaded!")
  * // unzip the folder's content into "resources" directory
  * //"unzip -q -o -d /resources/jupyterlab/labs/BD0211EN/ BD0211EN_Data" !
  * //println("Data Extracted!")
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

object CreatingSparkAppUsingGraphX {
  val spark = SparkSession
    .builder()
    .appName("CreatingSparkAppUsingGraphX")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "CreatingSparkAppUsingGraphX")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val input = "/home/cloudera/CognitiveClass/data/LabData/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      // Users.txt is a set of users and followers is the relationship between the users.
      // Take a look at the contents of these two files.
      println("Users: ")
      println(scala.io.Source.fromFile(s"${input}users.txt").mkString)

      println("Followers: ")
      println(scala.io.Source.fromFile(s"${input}followers.txt").mkString)

      // Create the users RDD and parse into tuples of user id and attribute list
      val users = sc
        .textFile(s"${input}users.txt")
        .map(line => line.split(","))
        .map(parts => (parts.head.toLong, parts.tail))

      users
        .take(5)
        .foreach(u => println(s"${u._1} => ${u._2.mkString("")}"))

      // Parse the edge data, which is already in userId -> userId format
      val followerGraph = GraphLoader.edgeListFile(sc, s"${input}followers.txt")

      // Attach the user attributes
      val graph = followerGraph.outerJoinVertices(users) {
        case (uid, deg, Some(attrList)) => attrList
        case (uid, deg, None) => Array.empty[String]
      }

      // Restrict the graph to users with usernames and names
      val subgraph = graph.subgraph(vpred = (vid, attr) => attr.size == 2)

      // Compute the PageRank
      val pagerankGraph = subgraph.pageRank(0.001)

      // Get the attributes of the top pagerank users
      val userInfoWithPageRank = subgraph.outerJoinVertices(pagerankGraph.vertices) {
        case (uid, attrList, Some(pr)) => (pr, attrList.toList)
        case (uid, attrList, None) => (0.0, attrList.toList)
      }

      println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))

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