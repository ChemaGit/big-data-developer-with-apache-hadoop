# APACHE SPARK STREAMING: DATA SOURCES

	* How data sources are integrated with Spark Streaming
	* How receiver-based integration differs from direct integration
	* How Apache Flume and Apache Kafka are integrated with Spark Streaming
	* How to use direct Kafka integration to create a DStream

* STREAMING DATA SOURCE OVERVIEW

	- Basic Data sources
		- Network socket
		- Text file

	- Advanced data sources
		- Kafka
		- Flume
		- Twitter
		- ZeroMQ
		- Kinesis
		- MQTT
		- And more coming in the future....

	- To use advanced data sources, download(if necessary) and link to the required library

* Receiver-Based Data Sources

	- Most data sources are based on receivers
		- Nework data is received on a worker node
		- Receiver distributes data (RDDs) to the cluster as partitions

* Receiver-Based Replication

	- Spark Streaming RDD replication is enabled by default
		- Data is copied to another node as it received

* Receiver-Based Fault Tolerance

	- If the receiver fails, Spark will restart it on a different executor
		- Potential for brief loss of incoming data

* Managing Incoming Data

	- Receivers queue jobs to process data as it arrives
	- Data must be processed fast enough that the job queue does not grow
		- Manage by setting spark.streaming.backpressure.enabled = true
	- Monitor scheduling delay and processing time in Spark UI
	
	
* FLUME AND KAFKA DATA SOURCES

* Overview: Spark Streaming with Flume

	- Two approaches to using Flume
		- Push-based
		- Pull-based

	- Push-based
		- One Spark worker must run a network receiver on a specified node
		- Configure Flume with an Avro sink to send to that receiver

	- Pull-based
		- Uses a custom Flume sink in spark.streaming.flume package
		- Strong reliability and fault tolerance guarantees

* Overview: Spark Streaming with Kafka

	- Apache Kafka is a fast, scalable, distributed publish-subscribe messaging system that provides
		- Durability by persisting data to disk
		- Fault tolerance through replication

	- Two approaches to Spark Streaming with Kafka
		- Receiver-based
		- Direct(receiverless)

* Receiver-Based Kafka Integration

	- Receiver-based
		- Streams(receivers) are configured with a Kafka topic and a partition in that topic
		- To protect from data loss, enable write ahead logs(introduced in Spark 1.2)
		- Scala and Java support added in Spark 1.1
		- Kafka supports partitioning of message topics for scalability
		- Receiver-based streaming allows multiple receivers, each configured for individual topic partitions

* Kafka direct Integration

	- Direct(also called receiverless)
		- Support for efficient zero-loss
		- Support for exactly-once semantics
		- Introduced in Spark 1.3(Scala and Java)
		- Consumes messages in parallel
		- Automatically assigns each topic partition to an RDD partition

* EXAMPLE: USING A KAFKA DIRECT DATA SOURCE

* Scala Example: Direct Kafka Integration

	import org.apache.spark.SparkContext

	import org.apache.spark.streaming.StreamingContext

	import org.apache.spark.streaming.Seconds

	import org.apache.spark.streaming.kafka._

	import kafka.serializer.StringDecoder

	object StreamingRequestCount {

		def main(args: Array[String]) {
			val sc = new SparkContext()
			val ssc = new StreamingContext(sc, Seconds(2))

			val kafkaStream = KafkaUitls.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,Map("metadata.broker.list" -> "broker1:port,broker2:port"),Set("mytopic"))

			val logs = kafkaStream.map(pair => pair._2)

			val userreqs = logs.map(line => (line.split(' ')(2),1)).reduceByKey( (x, y) => x + y)

			userreqs.print()

			ssc.start()
			ssc.awaitTermination()
		}
	}


* Essential Points

	- Spark Streaming integrates with a number of data sources
	- Most use a receiver-based integration
		- Flume, for example
	- Kafka can be integrated using a receiver-based or a direct (receiverless) approach
		The direct approach provides efficient strong reliability	

