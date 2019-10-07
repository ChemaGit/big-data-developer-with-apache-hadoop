````
# What is Apache Kafka?
	- Apache Kafka is a distributed commit log service
		* Widely used for data ingest
		* Conceptually similar to a publish-subscribe messaging system
		* Offers scalability, performance, reliability, and flexibility
		
# Characteristics of Kafka
	-S calable
		* Kafka is a distributed system that supports multiple nodes
	- Fault-tolerant
		* Data is persisted to disk and can be replicated throughout the cluster
	- High throughput
		* Each broker can process hundreds of thousands of messages per second
	- Low latency
		* Data is delivered in a fraction of a second
	- Flexible
		* Decouples the production of data from its consumption
		
# Kafka Use Cases
	- Kafka is used for a variety of use cases, such as
		* Log aggregation
		* Messaging
		* Web site activity tracking
		* Stream processing
		* Event sourcing
		
# Apache Kafka Overview

# Key Terminology
	- Message
		* A single data record passed by Kafka
	- Topic
		* A named log or feed of messages within Kafka
	- Producer
		* A program that reads messages from Kafka			
	- Consumer
		* A program that reads messages from Kafka
		
# Messages
	- Messages in Kafka are variable-size byte arrays
		* Represent arbitrary user-defined content
		* Use any format your application requires
		* Common formats include free-form text, JSON, and Avro
	- There is no explicit limit on message size
		* Optimal performance at a few KB per message
		* Practical limit of 1MB per message
	- Kafka retains all messages for a defined time period and/or total size
		* Administrator can specify retention on global or per-topic basis
		* Kafka will retain messages regardless of whether they were read
		* Kafka discards messages automatically after the retention period or total size is exceeded(whichever limit is reached first)
		* Default retention is one week
		* Retention can reasonably be one year or longer
		
# Topics
	- There is no explicit limit on the number of topics
		* However, Kafka works better with a few large topics than many small ones
	- A topic can be created explicitly or simply by publishing to the topic
		* This behavior is configurable
		* Cloudera recommends that administrators disable auto-creation of topics to avoid accidental creation of large numbers of topics
		
# Producers
	- Producers publish messages to Kafka topics
		* They communicate with Kafka, not a consumer
		* Kafka persists messages to disk on receipt
		
# Consumers
	- A consumer reads messages that were published to Kafka topics
		* They communicate with Kafka, not any producer
	- Consumer actions do not affect other consumers
		* For example, having one consumer display the messages in a topic as they are published does not change what is consumed by other consumers
	- They can come and go without impact on the cluster or other consumers
	
# Producers and Consumers
	- Tools available as part of Kafka
		* Command-line producer and consumer tools
		* Client(producer and consumer) Java APIs
	- A growing number of other APIs are available from third parties
		* Client libraries in many laguages including Python, PHP, C/C++, Go, .NET and Ruby
	- Integrations with other tools and projects include
		* Apache Flume
		* Apache Spark
		* Amazon AWS
		* syslog
	- Kafka also has a large and growing ecosystem
	
# Scaling Kafka
	- Scalability is one of the key benefits of Kafka
	- Two features let you scale Kafka for performance
		* Topic partitions
		* Consumer groups
		
# Topic Partitioning
	- Kafka divides each topic into some number of partitions
		* Topic partitioning improves scalability and throughput
	- A topic partition is an ordered and immutable sequence of messages
		* New messages are appended to the partition as they are received
		* Each message is assigned a unique sequential ID known as an offset
		
# Consumer Groups
	- One or more consumers can form their own consumer group that work together to consume the messages in a topic
	- Each partition is consumed by only one member of a consumer group
	- Message ordering is preserved per partition, but not across the topic
	
# Increasing Consumer Throughput
	- Additional consumers can be added to scale consumer group processing
	- Consumer instances that belong to the same consumer group can be in separate processes or on separate machines
	
# Multiple Consumer Groups
	- Each message published to a topic is delivered to one consumer instance within each subscribing consumer group
	- Kafka scales to large numbers of consumer groups and consumers
	
# Publish and Subscribe to Topic
	- Kafka functions like a traditional queue when all consumer instances belong to the same consumer group
		* In this case, a given message is received by one consumer
	- Kafka functions like traditional publish-subscribe when each consumer instance belongs to a different consumer group
		* In this case, all messages are broadcast to all consumer groups
		
# Kafka Clusters
	- A Kafka cluster consists of one or more brokers-servers running the Kafka broker daemon
	- Kafka depends on the Apache ZooKeeper service for coordination
	
# Apache ZooKeeper
	- Apache ZooKeeper is a coordination service for distributed applications
	- Kafka depends on the ZooKeeper service for coordination
		* Typically running three or five ZooKeeper instances
	- Kafka uses ZooKeeper to keep track of brokers running in the cluster
	- Kafka uses ZooKeeper to detect the addition or removal of consumers
	
# Kafka Brokers
	- Brokers are the fundamental daemons that make up a Kafka cluster
	- A broker fully stores a topic partition on disk, with caching in memory
	- A single broker can reasonably host 1000 topic partitions
	- One broker is elected controller of the cluster(for assignment of topic partitions to brokers, and so on)
	- Each broker daemon runs in its own JVM
		* A single machine can run multiple broker daemons
		
# Topic Replication
	- At topic creation, a topic can be set with a replication count
		* Doing so is recommended, as it provides fault tolerance
	- Each broker can act as a leader for some topic partitions and a follower for others
		* Followers passively replicate the leader
		* I the leader fails, a follower will automatically become the new leader
		
# Messages are Replicated
	- Configure the producer with a list of one or more brokers
		* The producer asks the first available broker for the leader of the desired topic partition
	- The producer then sends the message to the leader
		* The leader writes the messages to its local log
		* Each follower then writes the message to its own log
		* After acknowledgements from followers, the message is commited
		
# Creating Topics from the Command Line
	- Kafka includes a convenient set of command line tools
		* These are hepful for exploring and experimentation
	- The kafka-topics command offers a simple way to create Kafka topics
		* Provide the topic name of your choice, such as device_status
		* You must also specify the ZooKeeper connection string for your cluster
			$ kafka-topics --create \
			               --zookeeper zkhost1:2181,zkhost2:2181,zkhost3:2181 \
			               --replication-factor 3 \
			               --topic device_status
			               
# Displaying Topics from the Command Line
	- Use the --list option to list all topics
		$ kafka-topics --list --zookeeper zkhost1:2181, zkhost2:2181, zdhost3: 2181
	- Use the --help option to list all kafka-topics options
		$ kafka-topics --help
		
# Running a Producer from the Command Line(1)
	- You can run a producer using the kafka-console-producer tool
	- Spcify one or more brokers in the --broker-list option
		* Each broker consists of a hostname, a colon, and a port number
		* If specifying multiple brokers, separate them with commas
	- You must also provide the name of the topic
		$ kafka-console-producer \
		  --broker-list brokerhost1: 9092, brokerhost2:9092 \
		  --topic device_status
		  
# Running a Producer from the Command Line(2)
	- You may see a few log messages in the terminal after the producer starts
	- The producer will then accept input in the terminal window
		* Each line you type will be a message sent to the topic
	- Until you have configured a consumer for this topic, you will see no other output from Kafka
	
# Writing File Contents to Topics Using the Command Line
	- The data can then be sent to a topic using the command line producer
		* The data can then be sent to a topic using the command line producer
	- This example shows how to read input from a file named alerts.txt
		* Each line in this file becomes a separate message in the topic
			$ cat alerts.txt | kafka-console-producer \
			--broker-list brokerhost1:9092, brokerhost2:9092 \
			--topic device_status
	- This technique can be an easy way to integrate with existing programs
	
# Running a Consumer from the Command Line
	- You can run a consumer with the kafka-console-consumer tool
	- This requires the ZooKeeper connection string for you cluster
		* Unlike starting a producer, which instead requires a list of brokers
	- The command also requires a topic name
	- Use --from-beginning to read all available messages
		* Otherwise, it reads only new messages
			$ kafka-console-consumer \
			--zookeeper zkhost1:2181,zkhost2:2181,zkhost3:2181 \
			--topic device_status \
			--from-beginning				 					  				
````		
		
					            																														