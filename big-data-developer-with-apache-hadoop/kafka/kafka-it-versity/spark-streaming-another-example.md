# SPARK STREAMING - ANOTHER EXAMPLE
````text
- Get department wise traffic
- Problem Statement-Get department wise traffic every 30 seconds
- Read the data from retail_db logs
- Compute department traffic every 30 seconds
- Save the output to HDFS
- Solution
    - Use Spark Streaming
    - Publish messages from retail_db logs to netcat
    - Create Dstream
    - Process and save the output

$ gedit build.sbt
````
````properties
name := "retail"
version := "1.0"
scalaVersion := "2.11.12"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.1"
````
````text
- data file
  $ ls -ltr /opt/get_logs/logs/access.log
- to start the logs
  $ start_logs

$ gedit StreamingDepartmentCount.scala &
````
````scala
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext,Seconds}

object StreamingDepartmentCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Streaming Word Count").setMaster(args(0))
    val ssc = new StreamingContext(conf, Seconds(30))

    val messages = ssc.socketTextStream(args(1), args(2).toInt)
    val departmentMessages = messages.filter(msg => {val endPoint = msg.split(" ")(6); endPoint.split("/")(1) == "department"})

    val departments = departmentMessages.map(rec => {val endPoint = rec.split(" ")(6); (endPoint.split("/")(2),1)})
    val departmentTraffic = departments.reduceByKey( (total, value) => total + value)

    departmentTraffic.saveAsTextFiles("/user/cloudera/spark_streaming/departments")
    ssc.start()
    ssc.awaitTermination()
  }
}
````
````text
$ sbt package
$ scp target/scala-2.11/retail_2.11-1.0.jar localhost
$ tail_logs | nc -lk localhost 44444
- or
$ start_logs | nc -lk localhost 44444
$ spark2-submit --class StreamingDepartmentCount --master yarn  target/scala-2.11/retail_2.11-1.0.jar yarn-client localhost 44444
$ spark-submit --class StreamingDepartmentCount --master yarn  target/scala-2.11/retail_2.11-1.0.jar yarn-client localhost 44444
- also it works too
$ sbt "runMain StreamingDepartmentCount local localhost 44444"
````

