# FLUME AND SPARK STREAMING INTEGRATION
````text
- Let us see how Flume and Spark Streaming can be integrated!!! Here are the high level steps
  - Define Flume agent with one of the sinks as Spark
  - Define dependencies in build.sbt
- Develop the program
- Run the program by passing the appropriate jar files to –jars
````

# DEFINE FLUME AGENT
````text
- Flume agent is configured for
    - Reading data from access.log using exec type as source
    - Write one copy of unprocessed data to HDFS directly
    - Write another copy of data to 3rd party sink called spark
    - We will use Spark streaming to process data from spark
    - Flume agent can be run as flume-ng agent -n sdc -f sdc.conf
- Google search: spark 1.6.3 and flume integration
````
````properties
# sdc.conf: A multiplex flume configuration
# Source: log file
# Sink 1: Unprocessed data to HDFS
# Sink 2: Spark

# Name the components on this agent
sdc.sources = ws
sdc.sinks = hd spark
sdc.channels = hdmem sparkmem

# Describe/configure the source
sdc.sources.ws.type = exec
sdc.sources.ws.command = tail -F /opt/gen_logs/logs/access.log

# Describe the sink
sdc.sinks.hd.type = hdfs
sdc.sinks.hd.hdfs.path = /user/cloudera/flume_demo

sdc.sinks.hd.hdfs.filePrefix = FlumeDemo
sdc.sinks.hd.hdfs.fileSuffix = .txt
sdc.sinks.hd.hdfs.rollInterval = 120
sdc.sinks.hd.hdfs.rollSize = 1048576
sdc.sinks.hd.hdfs.rollCount = 100
sdc.sinks.hd.hdfs.fileType = DataStream

sdc.sinks.spark.type = org.apache.spark.streaming.flume.sink.SparkSink
sdc.sinks.spark.hostname = localhost
sdc.sinks.spark.port = 44444

# Use a channel sdcich buffers events in memory
sdc.channels.hdmem.type = memory
sdc.channels.hdmem.capacity = 1000
sdc.channels.hdmem.transactionCapacity = 100

sdc.channels.sparkmem.type = memory
sdc.channels.sparkmem.capacity = 1000
sdc.channels.sparkmem.transactionCapacity = 100

# Bind the source and sink to the channel
sdc.sources.ws.channels = hdmem sparkmem
sdc.sinks.hd.channel = hdmem
sdc.sinks.spark.channel = sparkmem
````
````text
$ sudo find -name spark-streaming-flume-sink*
/opt/cloudera/parcels/CDH-5.13.0-1.cdh5.13.0.p0.29/jars/spark-streaming-flume-sink_2.10-1.6.0-cdh5.13.0.jar
/usr/lib/flume-ng/lib/spark-streaming-flume-sink_2.10-1.6.0-cdh5.16.1.jar

$ sudo find -name scala-library*
/usr/lib/flume-ng/lib/scala-library-2.10.5.jar

$ ls -ltr /usr/lib/flume-ng/lib/
/usr/lib/flume-ng/lib/commons-lang-2.6.jar
````

# ADD DEPENDENCIES TO build.sbt
````text
- These dependencies need to be added to build.sbt so that we can access flume API as part of the application
- Read data from /opt/gen_logs/access.log using flume
- Write unprocessed data as well as streaming department count data to HDFS
- Development
    - Update build.sbt
    - Create a new program
    - Compile and build jar
- Run and validate
    - Ship it to the cluster
    - Run flume agent
    - Run spark submit with jars
    - Validate whether files are being generated or not

$ gedit build.sbt &
````
````properties
name := "flume_spark"
version := "1.0"
scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.1"
libraryDependencies += "org.apache.spark" % "spark-streaming-flume-sink_2.10" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-flume_2.10" % "1.6.0"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.12"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.3.2"
````

# DEVELOP THE PROGRAM USING FLUME API
````scala
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext,Seconds}
import org.apache.spark.streaming.flume._

object FlumeStreamingDepartmentCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("Flume Streaming Word Count").
      setMaster(args(0))
    val ssc = new StreamingContext(conf, Seconds(30))

    val stream = FlumeUtils.createPollingStream(ssc, args(1), args(2).toInt)
    val messages = stream.
      map(s => new String(s.event.getBody.array()))
    val departmentMessages = messages.
      filter(msg => {
        val endPoint = msg.split(" ")(6)
        endPoint.split("/")(1) == "department"
      })
    val departments = departmentMessages.
      map(rec => {
        val endPoint = rec.split(" ")(6)
        (endPoint.split("/")(2), 1)
      })
    val departmentTraffic = departments.
      reduceByKey((total, value) => total + value)
    departmentTraffic.saveAsTextFiles("/user/dgadiraju/deptwisetraffic/cnt")

    ssc.start()
    ssc.awaitTermination()

  }
}
````

# RUN THE PROGRAM ON THE CLUSTER
````text
- As these jar files will not be available as part of installation, we need to pass them using-jars

$ spark-submit \
  --class FlumeStreamingDepartmentCount \
--master yarn \
  --jars "/usr/lib/oozie/oozie-sharelib-yarn/lib/spark/spark-streaming-flume-sink_2.10-1.6.0-cdh5.16.1.jar,/usr/lib/oozie/oozie-sharelib-yarn/lib/spark/spark-streaming-flume_2.10-1.6.0-cdh5.16.1.jar,/opt/cloudera/parcels/SPARK2-2.2.0.cloudera4-1.cdh5.13.3.p0.603055/lib/spark2/jars/commons-lang3-3.5.jar,/usr/share/cmf/common_jars/flume-ng-sdk-1.2.0.jar" \
  target/scala-2.11/flume_spark_2.11-1.0.jar yarn-client localhost 44444
````