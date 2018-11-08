#CAPTURING DATA WITH APACHE FLUME

#What is Apache Flume?
	-Apache Flume is a high-performance system for data collection
		*Name derives from original use case of near-real time log data ingestion
		*Now widely used for collection of any streaming event data
		*Supports aggregating data from many sources into HDFS
	-Originally developed by Cloudera
		*Donated to Apache Software Foundation in 2011
	-Benefits of Flume
		*Horizontally-scalable
		*Extensible
		*Reliable
#Flume's Design Goals: Reliability
	-Channels provide Flume's reliability
	-Examples
		*Memory channel: Fault intolerant, data will be lost if power is lost
		*Disk-based channel: Fault tolerant
		*Kafka channel: Fault tolerant
	-Data transfer between agents and channels is transactional
		*A failed data transfer to a downstream agent rolls back and retries
	-You can configure multiple agents with the same task
		*For example, two agents doing the job of one "collector"-if one agent fails then upstream agents would fail over
#Flume's Design Goals: Scalability
	-Scalability
		*The ability to increase system performance linearly-or better-by adding more resources to the system
		*Flume scales horizontally
			*As load increases, add more agents to the machine, and add more machines to the system
#Flume's Design Goals: Extensibility
	-Extensibility
		*The ability to add new functionality to a system
	-Flume can be extended by adding sources and sinks to existing storage layers or data platforms
		*Flume includes sources that can read data from files, syslog, and standard output from any Linux process
		*Flume includes sinks that can write to files on the local filesystem, HDFS, Kudu, HBase, and so on
		*Developers can write their own sources or sinks
#Common Flume Data Sources
	-Log Files, Sensor Data, Status Updates Network Sockets UNIX syslog, Program Output, Social Media Posts
	
#Large-Scala Deployment Example
	-Flume collects data using configurable agents
		*Agents can receive data from many sources, including other agents
		*Large-scale deployments use multiple tiers for scalability and reliability
		*Flume supports inspection and modification of in-flight data
		
#CAPTURING DATA WITH APACHE FLUME

#Flume Events
	-An event is the fundamental unit of data in Flume
		*Consists of a body(payload) and a collection of headers(metadata)
	-Headers consist of name-value pairs
		*Headers are mainly used for directing output
#Components in Flume's Architecture
	-Source
		*Receives events from the external actor that generates them
	-Sink
		*Sends an event to its destination
	-Channel
		*Buffers events from the source until they are drained by the sink
	-Agent
		*Configures and hosts the source, channel, and sink
		*A java process that runs in a JVM
		
#Flume Data Flow														
	-How syslog data might be captured to HDFS
		1. Server running a syslog daemon logs a message
		2. Flume agent configured with syslog source retrieves event
		3. Source pushes event to the channel, where it is buffered in memory
		4. Sink pulls data from the channel and writes it to HDFS
		
#SOURCES
#Notable Built-In Flume Sources
	-Syslog
		*Captures messages from UNIX syslog daemon over the network
	-Netcat
		*Captures any data written to a socket on an arbitrary TCP port
	-Exec
		*Executes a UNIX program and reads events from standard output
	-Spooldir
		*Extracts events from files appearing in a specified(local) directory
	-HTTP Source
		*Retrieves events from HTTP requests
	-Kafka
		*Retrieves events by consuming messages from a Kafka topic
		
#SINKS
#Some Interesting Built-In Flume Sinks
	-Null
		*Discards all events(Flume equivalent of /dev/null)
	-Logger
		*Logs event to INFO level using SLF4J
	-IRC
		*Sends event to a specified Internet Relay Chat channel
	-HDFS
		*Writes event to a file in the specified directory in HDFS
	-Kafka
		*Sends event as a message to a Kafka topic
	-HBaseSink
		*Stores event in HBase
		
#CHANNELS
#Built-In Flume Channels
	-Memory
		*Stores events in the machine's RAM
		*Extremely fast, but not reliable(memory is volatile)
	-File
		*Stores events on the machine's local disk
		*Slower than RAM, but more reliable(data is written to disk)
	-Kafka
		*Uses Kafka as a scalable, reliable, and highly available channel between any source and sink type

#CONFIGURATION
#Flume Agent Configuration File
	-Configure Flume agents through a Java properties file
		*You can configure multiple agents in a single file
	-The configuration file uses hierarchical references
		*Assign each component a user-defined ID
		*Use that ID in the names of additional properties
			# Define sources, sinks, and channel for agent named 'agent1'
			agent1.sources = mysource
			agent1.sinks = mysink
			agent1.channels = mychannel
			
			# Sets a property "foo" for the source associated with agent1
			agent1.sources.mysource.foo = bar
			
			# Sets a property "baz" for the sink associated with agent1
			agent1.sinks.mysink.baz = bat
			
#Example: Configuring Flume Components			
	-Example: Configure a Flume agent to collect data from remote spool directories and save to HDFS
		*/var/flume/incoming ==> Memory Channel ==> HDFS: /loudacre/logdata/	
		      src1                      ch1                      sink1																							
             --------------------------------------------------------
                                       agent1
                                       
		agent1.sources = src1
		agent1.sinks = sink1
		agent1.channels = ch1
		
		agent1.channels.ch1.type = memory
		
		agent1.sources.src1.type = spooldir
		agent1.sources.src1.spooldir = /var/flume/incoming
		agent1.sources.src1.channels = ch1    #Connects source and channel
		
		agent1.sinks.sink1.type = hdfs
		agent1.sinks.sink1.hdfs.path = /loudacre/logdata
		agent1.sinks.sink1.channel = ch1	#Connects sink and channel
	
	-Properties vary by component type(source, channel, and sink)
		*Properties also vary by subtype(such as netcat source, syslog source)
		*See the Flume user guide for full details on configuration
		
#Aside: HDFS Sink Configuration
	-Path may contain patterns based on event headers, such as timestamp
	-The HDFS sink writes uncompressed SequenceFiles by default
		*Specifying a codec will enable compression
		
			agent1.sinks.sink1.type = hdfs
			agent1.sinks.sink1.hdfs.path = /loudacre/logdata/%y-%m-%d
			agent1.sinks.sink1.hdfs.codeC = snappy
			agent1.sinks.sink1.channel = ch1
	-Setting fileType parameter to DataStream writes raw data
		*Can also specify a file extension, if desired
		
			agent1.sinks.sink1.type = hdfs
			agent1.sinks.sink1.hdfs.path = /loudacre/logdata/%y-%m-%d
			agent1.sinks.sink1.hdfs.fileType = DataStream
			agent1.sinks.sink1.hdfs.fileSuffix = .txt
			agent1.sinks.sink1.hdfs.channel = ch1
			
#Starting a Flume Agent
	-Typical command invocation
		*The --name argument must match the agent's name in the configuration file
		*Setting root logger as shown will display log messages in the terminal
		
			$flume-ng agent \
			--conf /etc/flume-ng/conf \
			--conf-file /path/to/flume.conf \
			--name agent1 \
			-Dflume.root.logger=INFO,console
								
		
		                                       
											