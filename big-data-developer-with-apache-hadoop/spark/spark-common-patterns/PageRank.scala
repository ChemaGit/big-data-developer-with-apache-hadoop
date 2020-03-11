package example

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
// $mvn package
// $spark-submit --class example.PageRank --name 'PageRank' --master yarn-client target/pagerank-1.0.jar /files/pagelinks.txt
// $spark-submit --class example.PageRank --name 'PageRank' --master 'local[*]' target/pagerank-1.0.jar /files/pagelinks.txt
// $spark-submit --class example.PageRank --name 'PageRank' --master 'local[8]' target/pagerank-1.0.jar /files/pagelinks.txt

object PageRank {

   // given the list of neighbors for a page and that page's rank, calculate 
   // what that page contributes to the rank of its neighbors
   def computeContribs(neighbors: Iterable[String], rank: Double): Iterable[(String,Double)] = {
     for (neighbor <- neighbors) yield(neighbor, rank/neighbors.size)
   }    

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: example.PageRank <file>")
      System.exit(1)
    }

    val sconf = new SparkConf().setAppName("PageRank").set("spark.ui.port","4141")
    val sc = new SparkContext(sconf)
    sc.setLogLevel("ERROR")

    // read in a file of link pairs (format: url1 url2)
    val linkfile = args(0)
    val links = sc.textFile(linkfile).map(_.split(" "))
                                     .map(pages => (pages(0),pages(1)))
                                     .distinct().groupByKey().cache

    // create initial page ranges of 1.0 for each
    var ranks = links.map(pair => (pair._1,1.0))

    // number of iterations
    val n = 10

    // for n iterations, calculate new page ranks based on neighbor contributions
    for (x <- 1 to n) {
      var contribs = links.join(ranks).flatMap(pair => computeContribs(pair._2._1,pair._2._2)) 
      ranks = contribs
              .reduceByKey(_+_)
              .map(pair => (pair._1,pair._2 * 0.85 + 0.15))
      println("Iteration " + x)
      for (rankpair <- ranks.take(10)) println(rankpair)    
    }	       
    sc.stop()
  }
}
