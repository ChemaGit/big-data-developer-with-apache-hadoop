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


