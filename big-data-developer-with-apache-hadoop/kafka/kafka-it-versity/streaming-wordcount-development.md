# STREAMING WORD COUNT - DEVELOPMENT
````text
    - Let us develop the program to perform word count.
    - Add dependencies for spark streaming to build.sbt
    - import org.apache.spark.streaming._ to import all the APIs
    - Create Spark Configuration object with master and app name
    - Pass Spark Configuration object and number of seconds to Streaming Context object. This will facilitate the Streaming Context to queue up the data for the interval equal to number of seconds and apply logic for processing.
    - Develop necessary logic to perform streaming word count
    - At the end use start and awaitTermination for Spark Streaming Context run perpetually
    - Finally, build the jar file using sbt package command
````
````bash
$ gedit build.sbt &
````
````properties
name := "retail"
version := "1.0"
scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.1"
````
````scala
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
val conf = new SparkConf().setAppName("Streaming word count").setMaster("local")
val ssc = new StreamingContext(conf, Seconds(10))

val lines = ssc.socketTextStream("localhost", 44444)
val words = lines.flatMap(line => line.split(" "))
val tuples = words.map(word => (word, 1))
val wordCount = tuples.reduceByKey( (t, v) => t + v)

wordCount.print()
ssc.start()
ssc.awaitTermination()
````

````scala
import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object StreamingWordCount {
  def main(args: Array[String]) {
    val executionMode = args(0)
    val conf = new SparkConf().setAppName("Streaming word count").setMaster(executionMode)
    val ssc = new StreamingContext(conf, Seconds(10))

    val lines = ssc.socketTextStream(args(1), args(2).toInt)
    val words = lines.flatMap(line => line.split(" "))
    val tuples = words.map(word => (word, 1))
    val wordCount = tuples.reduceByKey( (t, v) => t + v)

    wordCount.print()
    ssc.start()
  }
}
````

# STREAMING WORD COUNT - DEPLOY AND RUN
````text
- Develop the project
- Compile into Jar
- Start netcat service
- Run application using Jar
````
````scala
object StreamingWordCount {
  def main(args: Array[String]) {
    val executionMode = args(0)
    val conf = new SparkConf().setAppName("Streaming word count").setMaster(executionMode)
    val ssc = new StreamingContext(conf, Seconds(10))

    val lines = ssc.socketTextStream(args(1), args(2).toInt)
    val words = lines.flatMap(line => line.split(" "))
    val tuples = words.map(word => (word, 1))
    val wordCount = tuples.reduceByKey( (t, v) => t + v)

    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
````
````bash
$ nc -lk localhost 44444
$ spark2-submit --class StreamingWordCount --master yarn  target/scala-2.11/retail_2.11-1.0.jar yarn-client localhost 44444
$ spark-submit --class StreamingWordCount --master yarn  target/scala-2.11/retail_2.11-1.0.jar yarn-client localhost 44444
# also it works too
$ sbt "runMain StreamingWordCount local localhost 44444"
````

