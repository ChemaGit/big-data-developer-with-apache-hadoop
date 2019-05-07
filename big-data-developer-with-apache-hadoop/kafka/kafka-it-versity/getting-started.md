KAFKA - GETTING STARTED

kafka-topics --create \
   --zookeeper localhost:2181 \
   --replication-factor 1 \
   --partitions 1 \
   --topic kafkademo

kafka-topics --list \
   kafka-topics --list --zookeeper localhost:2181

kafka-topics --list \
   kafka-topics --list --zookeeper localhost:2181 \
   --topic kafkademo

kafka-console-producer \
  --broker-list quickstart.cloudera:9092 \
  --topic kafkademo

kafka-console-consumer --bootstrap-server quickstart.cloudera:9092 --topic kafkademo --from-beginning

ANATOMY OF A TOPIC

	- Topic is nothing but a file which captures stream of messages
	- Publishers publish message to the topic
	- Topics can be partitioned for scalability
	- Each topic partition can be cloned for reliability
	- Offset - position of the last message consumer have read from each partition of the topic
	- Consumer group can be created to facilitate multiple consumers read from same topic in co-ordinated fashion (offset is tracked at group level)
	- Path to the kafka logs: /var/local/kafka/data/     /var/local/kafka/data/kafkademo-0

ROLE OF KAFKA AND FLUME

	- Life cycle of streaming analytics
		- Get data from source(Flume and/or Kafka)
		- Process data
		- Store it in target
	- Kafka can be used for the most of the applications
	- But existing source applications need to be refactored to publish messages
	- Source applications are mission critical and highly sensitive for any changes
	- In that case, if the messages are already captured in web server logs, one can use Flume to get messages from logs and publish to Kafka Topic

SPARK STREAMING

	- Different contexts in Spark
		- SparkContext(web service)
		- SQLContext(wrapper on top of SparkContext)
		- StreamingContext
		
	- SparkContext vs StreamingContext

		$ spark-shell --master yarn --conf spark.ui.port=12456
		scala> sc
		scala> sqlContext
		scala> sc.stop()
		scala> import org.apache.spark.streaming._
		scala> import org.apache.spark.SparkConf

SETTING UP NETCAT
	- Netcat, which is a web service can be used to get started
	- Start netcat using host name and port number
	- We can start publishing messages to this web service
	
		scala> val conf = new SparkConf().setAppName("streaming").setMaster("yarn-client")
		scala> val ssc = new StreamingContext(conf, Seconds(10))
		scala> exit
		
	- Streaming Context need to have web service
	- We can use nc to simulate simple web service
	
	- If nc is not available on your PC, you need to set it up
	
		$ nc -lk localhost 44444 
		and on other console
		$ nc  localhost 44444 > nc_demo.txt or nc  localhost 4444