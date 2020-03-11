// $ mvn package
// $ spark-submit --class example.KMeansCoords --name "KMeansCoords" --master yarn-client target/kmeans-1.0.jar /loudacre/devicestatus_etl/*
// $ spark-submit --class example.KMeansCoords --name "KMeansCoords" --master 'local[*]' target/kmeans-1.0.jar /loudacre/devicestatus_etl/*
// $ spark-submit --class example.KMeansCoords --name "KMeansCoords" --master 'local[8]' target/kmeans-1.0.jar /loudacre/devicestatus_etl/*

package example

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.math.pow

// Find K Means of Loudacre device status locations
// 
// Input data: file(s) with device status data (delimited by ',')
// including latitude (4th field) and longitude (5th field) of device locations 
// (lat,lon of 0,0 indicates unknown location)

object KMeansCoords {

  // The squared distances between two points
  def distanceSquared(p1: (Double,Double), p2: (Double,Double)) = { 
    pow(p1._1 - p2._1,2) + pow(p1._2 - p2._2,2 )
  }

  // The sum of two points
  def addPoints(p1: (Double,Double), p2: (Double,Double)) = {
    (p1._1 + p2._1, p1._2 + p2._2)
  }

  // for a point p and an array of points, return the index in the array of the point closest to p
  def closestPoint(p: (Double,Double), points: Array[(Double,Double)]): Int = {
      var index = 0
      var bestIndex = 0
      var closest = Double.PositiveInfinity

      for (i <- 0 until points.length) {
        val dist = distanceSquared(p,points(i))
        if (dist < closest) {
          closest = dist
          bestIndex = i
        }
      }
      bestIndex
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: example.KMeansCoords <file>") //<file> /loudacre/devicestatus_etl/*
      System.exit(1)
    }

    val sconf = new SparkConf().setAppName("KMeansCoords").set("spark.ui.port","4141")
    val sc = new SparkContext(sconf)
    sc.setLogLevel("ERROR")
    // The device status data file(s)
    val filename = args(0)

    // K is the number of means (center points of clusters) to find
    val K = 5

    // ConvergeDist -- the threshold "distance" between iterations at which we decide we are done
    val convergeDist = .1
    
    // Parse the device status data file
    // Split by delimiter ,
    // Parse  latitude and longitude (4th and 5th fields) into pairs
    // Filter out records where lat/long is unavailable -- ie: 0/0 points
    // TODO
    val kMeans = sc.textFile(filename)
                   .map(line => line.split(","))
                   .map(arr => (arr(3).toDouble, arr(4).toDouble) )
                   .filter({case(lat, lon) => !( (lat == 0.0) && (lon == 0.0) ) })
                   .cache()
    //start with K randomly selected points from the dataset
    // TODO
    val kPoints = kMeans.takeSample(false, K, 42)	
    // loop until the total distance between one iteration's points and the next is less than the convergence distance specified
    var tempDist = Double.PositiveInfinity
    println("Starting KPoints")
    kPoints.foreach(println)
    while (tempDist > convergeDist) {
      // for each point, find the index of the closest kpoint.  map to (index, (point,1))
      // TODO
      val kPointIndex = kMeans.map({case(p) => (closestPoint(p, kPoints), (p, 1)) }) //.keyBy({case(index, v) => index})
      // For each key (k-point index), reduce by adding the coordinates and number of points
      // TODO
      val reduce = kPointIndex.reduceByKey({case( (point1,n1),(point2,n2) ) =>  (addPoints(point1, point2), n1 + n2) })
      // For each key (k-point index), find a new point by calculating the average of each closest point
      // TODO
      val average = reduce.map({case( (k, (p, n)) ) => (k, (p._1/n, p._2/n))}).collectAsMap()
      // calculate the total of the distance between the current points and new points
      // TODO
      tempDist = 0.0
      for (i <- 0 until K) {
        tempDist += distanceSquared(kPoints(i),average(i))
      }
      println("Distance between iterations: " + tempDist)

      // Copy the new points to the kPoints array for the next iteration
      //TODO	
      for (i <- 0 until K) {
        kPoints(i) = average(i)
      }      
    }
   
    // Display the final center points        
    // TODO    
    println("Final K Points")
    kPoints.foreach(println)

    sc.stop()
  }
}