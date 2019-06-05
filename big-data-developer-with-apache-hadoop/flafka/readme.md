* INTEGRATING APACHE FLUME AND APACHE KAFKA
	- What to consider when choosing between Apache Flume and Apache Kafka for a use case
	- How Flume and Kafka can work together
	- How to configure a Kafka channel, sink, or source in Flume

* OVERVIEW
	- Both Flume and Kafka are widely used for data ingest
		- Although these tools differ, their functionality has some overlap
		- Some use cases could be implemented with either Flume or Kafka
	- How do you determine which is a better choice for your use case?

* Characteristics of Flume
	- Flume is efficient at moving data from  a single source into Hadoop
	- Offers sinks that write to HDFS, an HBase or Kudu table, or a Solr index
	- Easily configured to support common scenarios, without writing code
	- Can also process and transform data during the ingest process

* Characteristics of Kafka
	- Offers more flexibility than Flume for connecting multiple systems
	- Provides better durability and fault tolerance than Flume
	- Often requires writing code for producers and/or consumers
	- Has no direct support for processing messages or loading into Hadoop

* FLAFKA = FLUME + KAFKA
	- Both systems have strengths and limitations
	- You do not necessarily have to choose between them
		- You can use both when implementing your use case
	- Flafka is the informal name for Flume-Kafka integration
		- It uses a Flume agent to receive messages from or send messages to Kafka
	- It is implemented as a Kafka source, channel, and sink for Flume

* USE CASES

* Using a Flume Kafka Sink as a Producer
	- By using a Kafka sink, Flume can publish messages to a topic
	- In this example, an application uses Flume to publish application events
		- The application sends data to the Flume source when events occur
		- The event data is buffered in the channel until it is taken by the sink
		- The Kafka sink publishes messages to a specified topic
		- Any Kafka consumer can then read messages from the topic for application events

	APPLICATION ---------------------> FLUME AGENT ---------------------------------------------> KAFKA CLUSTER
			 Source(Netcat) --> Channel(Memory) --> Sink(Kafka) --> Topic Message ------> Brokers(n)  ---> Topic Message --> Consumer

* Using a Flume Kafka Source as a Consumer
	- By using a Kafka source, Flume can read messages from a topic
		- It can then write them to your destination of choice using a Flume sink
	- In this example, the producer sends messages to Kafka
		- The Flume agent uses a Kafka source, which acts as a consumer
		- The Kafka source reads messages in a specified topic
		- The message data is buffered in the channel until it is taken by the sink
		- The sink then writes the data into HDFS

	PRODUCER -----------> KAFKA CLUSTER ------------------> FLUME AGENT ---------------------------------------------> HADOOP CLUSTER
	  --> Topic Message --> Brokers(n) --> Topic Message --> Source(Kafka) --> Channel(Memory) --> Sink(HDFS) -------> HADOOP CLUSTER

* Using a Flume Kafka Channel
	- A Kafka channel can be used with any Flume source or sink
		- Provides a scalable, reliable, hig-availability channel
	- In this example, a Kafka channel buffers events
		- The application sends event data to the Flume source
		- The channel publishes event data as messages on a Kafka topic
		- The sink receives event data and stores it to HDFS

	APPLICATION ---------------------> FLUME AGENT ---------------------------------------------> HADOOP CLUSTER
			 Source(Netcat) --> Channel(Kafka) --> Sink(HDFS) --------------------------> HADOOP CLUSTER

* Using a Kafka channel as a consumer(Sourceless Channel)
	- Kafka channels can also be used without a source
		- It can then write events to your destination of choice using a Flume sink
	- In this example, the Producer sends messages to Kafka brokers
		- The Flume agent uses a Kafka channel, which acts as a consumer
		- The Kafka channel reads messages in a specified topic
		- Channel passes messages to the sink, which writes the data into HDFS

	PRODUCER -----------> KAFKA CLUSTER ------------------------------> FLUME AGENT ---------------------> HADOOP CLUSTER
	  --> Topic Message --> Brokers(n) --> Topic Message ---------> Channel(Kafka) --> Sink(HDFS) -------> HADOOP CLUSTER

* INTEGRATING APACHE FLUME AND APACHE KAFKA

* Configuring Flume with a Kafka Source
	- The table below describes some key properties of the Kafka source
		- Name					Description
		  type					org.apache.flume.source.kafka.KafkaSource
		  zookeeperConnect			Zookeeper connection string(example: zkhost:2181)
		  topic					Name of Kafka topic from which messages will be read
		  groupId				Unique ID to use for the consumer group(default: flume)

	PRODUCER -----------> KAFKA CLUSTER ------------------> FLUME AGENT ---------------------------------------------> HADOOP CLUSTER
	  --> Topic Message --> Brokers(n) --> Topic Message --> Source(Kafka) --> Channel(Memory) --> Sink(HDFS) -------> HADOOP CLUSTER

* Example: Configuring Flume with a Kafka Source
	- This is the Flume configuration for the previous example
		- It defines a source for reading messages from a Kafka topic


	# Define names for the source, channel, and sink
	agent1.sources = source1
	agent1.channels = channel1
	agent1.sinks = sink1

	# Define a Kafka source that reads from the calls_placed topic
	# The "type" property line wraps around due to its long value
	agent1.sources.source1.type = org.apache.flume.source.kafka.KafkaSource
	agent1.sources.source1.zookeeperConnect = localhost:2181
	agent1.sources.source1.topic = calls_placed
	agent1.sources.source1.channels = channel1

	# Define the properties of our channel
	agent1.channels.channel1.type = memory
	agent1.channels.channel1.capacity = 10000
	agent1.channels.channel1.transactionCapacity = 1000

	# Define the sink that writes call data to HDFS
	agent1.sinks.sink1.type = hdfs
	agent1.sinks.sink1.hdfs.path = /user/cloudera/calls_placed
	agent1.sinks.sink1.hdfs.fileType = DataStream
	agent1.sinks.sink1.hdfs.fileSuffix = .csv
	agent1.sinks.sink1.hdfs.channel = channel1

* Configuring Flume with a Kafka Sink
	- The table below describes some key properties of the Kafka sink
		- Name					Description
		  type					org.apache.flume.sink.kafka.KafkaSink
		  brokerList 				Comma-separated list of brokers(host:port, ...)
		  topic					The topic in Kafka to which the messages will be published
		  batchSize				How many messages to process in one batch

	APPLICATION ---------------------> FLUME AGENT ---------------------------------------------> KAFKA CLUSTER
			 Source(Netcat) --> Channel(Memory) --> Sink(Kafka) --> Topic Message ------> Brokers(n)  ---> Topic Message --> Consumer

* Example: Configuring Flume with a Kafka Sink
	- This is the Flume configuration for the previous example

	# Define names for the source, channel, and sink
	agent1.sources = source1
	agent1.channels = channel1
	agent1.sinks = sink1

	# Define the properties of the source, which receives event data
	agent1.sources.source1.type = netcat
	agent1.sources.source1.bind = localhost
	agent1.sources.source1.port = 44444
	agent1.sources.source1.channels = channel1

	# Define the properties of our channel
	agent1.channels.channel1.type = memory
	agent1.channels.channel1.capacity = 10000
	agent1.channels.channel1.transactionCapacity = 1000

	# Define the Kafka sink, which publishes to the app_event topic
	agent1.sinks.sink1.type = org.apache.flume.sink.kafka.KafkaSink
	agent1.sinks.sink1.topic = app_events
	agent1.sinks.sink1.brokerList = localhost:9092
	agent1.sinks.sink1.batchSize = 20
	agent1.sinks.sink1.channel = channel1

* Configuring Flume with a Kafka Channel
	- The table below describes some key properties of the Kafka channel
		- Name					Description
		  type					org.apache.flume.channel.kafka.KafkaChannel
		  zookeeperConnect			Zookeeper connection string(example:zkhost:2181)
		  brokerList 				Comma-separated list of brokers(host:port, ...) to contact
		  topic					Name of Kafka topic from which messages will be read(optional, default=flume-channel)
		  parseAsFlumeEvent			Set to false for sourceless configuration(optional, default=true)
		  readSmallestOffset			Set to true to read from the beginning of the Kafka topic(optional, default=false)

	PRODUCER ----------->KAFKA CLUSTER-------------> FLUME AGENT ---------------------------------------------> HADOOP CLUSTER
	  -->Topic(Message)-->Brokers(n) ---------------> Channel(Kafka) --> Sink(HDFS) --------------------------> HADOOP CLUSTER

* Example: Configuring a Sourceless Kafka Channel
	- This is the Flume configuration for the previous example

	# Define names for the source, channel, and sink
	agent1.channels = channel1
	agent1.sinks = sink1

	# Define the properties of the Kafka channel
	# which reads from the calls_placed topic
	agent1.channels.channel1.type = org.apache.flume.channel.kafka.KafkaChannel
	agent1.channels.channel1.topic = calls_placed
	agent1.channels.channel1.brokerList = localhost:9092
	agent1.channels.channel1.zookeeperConnect = localhost:2181
	agent1.channels.channel1.parseAsFlumeEvent = false

	# Define the sink that writes data to HDFS
	agent1.sinks.sink1.type = hdfs
	agent1.sinks.sink1.hdfs.path = /user/cloudera/calls_placed
	agent1.sinks.sink1.hdfs.fileType = DataStream
	agent1.sinks.sink1.hdfs.fileSuffix = .csv
	agent1.sinks.sink1.hdfs.channel = channel1




