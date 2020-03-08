# KAFKA AND SPARK STREAMING INTEGRATION EXAMPLE
````text
- Add dependencies in build.sbt
- Import necessary packages
- Create set for list of topics and hash map for Kafka parameters
- Create input stream and process the data
- Download dependencies on the gateway node
- Ship and run jar file with jars including kafka dependencies
````

````properties
$ gedit build.sbt &
````

````properties
name := "kafka_spark"
version := "1.0"
scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.1"
libraryDependencies += "org.apache.spark" % "spark-streaming-flume-sink_2.10" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-flume_2.10" % "1.6.0"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.12"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.3.2"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0"
````

````properties
$ gedit KafkaStreamingDepartmentCount.scala &
````

````scala
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

object KafkaStreamingDepartmentCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Streaming Word Count").setMaster(args(0))
    val ssc = new StreamingContext(conf, Seconds(30))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> "quickstart.cloudera:9092")
    val topicSet = Set("kafka_demo")

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)

    val messages = stream.map(s => s._2)
    val departmentMessages = messages.filter(msg => {val endPoint = msg.split(" ")(6); endPoint.split("/")(1) == "department"})
    val departments = departmentMessages.map(rec => {val endPoint = rec.split(" ")(6); (endPoint.split("/")(2),1)})
    val departmentTraffic = departments.reduceByKey( (total,value) => total + value)
    departmentTraffic.saveAsTextFiles("/user/cloudera/kafka_spark")

    ssc.start()
    ssc.awaitTermination()
  }
}
````

````properties
$ start_logs

$ flume-ng agent --name wk --conf-file /home/cloudera/flume_demo/flume_kafka.conf

$ spark-submit \
--class KafkaStreamingDepartmentCount \
--master yarn \
--jars "/usr/lib/oozie/oozie-sharelib-yarn/lib/spark/spark-streaming-kafka_2.10-1.6.0-cdh5.16.1.jar,/usr/lib/oozie/oozie-sharelib-yarn/lib/spark/kafka_2.10-0.9.0-kafka-2.0.2.jar,/usr/lib/oozie/libtools/metrics-core-2.2.0.jar" \
target/scala-2.10/kafka_spark_2.10-1.0.jar yarn-client
````