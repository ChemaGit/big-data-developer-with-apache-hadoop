/** Question 62
 * Problem Scenario 27 : You need to implement near real time solutions for collecting information when submitted in file with below information.
 * Data
 * echo "IBM,100,20160104" >> /tmp/spooldir/bb/.bb.txt
 * echo "IBM,103,20160105" >> /tmp/spooldir/bb/.bb.txt
 * mv /tmp/spooldir/bb/.bb.txt /tmp/spooldir/bb/bb.txt
 * After few mins
 * echo "IBM,100.2,20160104" >> /tmp/spooldir/dr/.dr.txt
 * echo "IBM,103.1,20160105" >> /tmp/spooldir/dr/.dr.txt
 * mv /tmp/spooldir/dr/.dr.txt /tmp/spooldir/dr/dr.txt
 * Requirements:
 * You have been given below directory location (if not available than create it) /tmp/spooldir .
 * You have a finacial subscription for getting stock prices from BloomBerg as well as Reuters 
 * and using ftp you download every hour new files from their respective ftp site in directories /tmp/spooldir/bb and /tmp/spooldir/dr respectively.
 * As soon as file committed in this directory that needs to be available in hdfs in /tmp/flume/finance location in a single directory.
 * Write a flume configuration file named flume7.conf and use it to load data in hdfs with following additional properties .
 * 1. Spool /tmp/spooldir/bb and /tmp/spooldir/dr
 * 2. File prefix in hdfs should be events 
 * 3. File suffix should be .log
 * 4. If file is not commited and in use than it should have _ as prefix.
 * 5. Data should be written as text to hdfs
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : Create directory 
$ mkdir /tmp/spooldir
$ mkdir /tmp/spooldir/bb
$ mkdir /tmp/spooldir/dr

//Step 2: create hdfs directory
$ hdfs dfs -mkdir /tmp/flume/finance

//Step 3 : Create flume configuration file, with below configuration for 
agent1.sources = source1 source2 
agent1.sinks = sink1 
agent1.channels = channel1 

agent1.sources.source1.channels = channel1 
agent1.sources.source2.channels = channel1 
agent1.sinks.sink1.channel = channel1 

agent1.sources.source1.type = spooldir 
agent1.sources.source1.spoolDir = /tmp/spooldir/bb 

agent1.sources.source2.type = spooldir 
agent1.sources.source2.spoolDir = /tmp/spooldir/dr 

agent1.sinks.sink1.type = hdfs 
agent1.sinks.sink1.hdfs.path = /tmp/flume/finance 
agent1.sinks.sink1.hdfs.filePrefix = events 
agent1.sinks.sink1.hdfs.fileSuffix = .log 
agent1.sinks.sink1.hdfs.inUsePrefix = _ 
agent1.sinks.sink1.hdfs.fileType = DataStream 

agent1.channels.channel1.type = file

//Another possible solution for the agent 

# Name the components on this agent
a1.sources = r1 r2
a1.sinks = k1 k2
a1.channels = c1 c2

# Describe/configure the source
a1.sources.r1.type = spooldir
a1.sources.r1.spoolDir = /tmp/spooldir/bb
a1.sources.r1.fileHeader = false

a1.sources.r2.type = spooldir
a1.sources.r2.spoolDir = /tmp/spooldir/dr
a1.sources.r2.fileHeader = false


# Describe the sink
a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = /tmp/flume/finance
a1.sinks.k1.hdfs.filePrefix = events
a1.sinks.k1.hdfs.fileSuffix = .log
a1.sinks.k1.hdfs.fileType = DataStream
a1.sinks.k1.hdfs.inUseSuffix = -

a1.sinks.k2.type = hdfs
a1.sinks.k2.hdfs.path = /tmp/flume/finance
a1.sinks.k2.hdfs.filePrefix = events
a1.sinks.k2.hdfs.fileSuffix = .log
a1.sinks.k2.hdfs.fileType = DataStream
a1.sinks.k2.hdfs.inUseSuffix = -


# Use a channel which buffers events in memory
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

a1.channels.c2.type = memory
a1.channels.c2.capacity = 1000
a1.channels.c2.transactionCapacity = 100

# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sources.r2.channels = c2
a1.sinks.k1.channel = c1
a1.sinks.k2.channel = c2

//Step 4 : Run below command which will use this configuration file and append data in hdfs. 
//Start flume service:  
$ flume-ng agent --conf /home/training/flumeconf --conf-file /home/training/flumeconf/flume7.conf --name agent1 -Dflume.root.logger=INFO,console

//Step 5 : Open another terminal and create a file in /tmp/spooldir/ 
$ echo "IBM,100,20160104" >> /tmp/spooldir/bb/.bb.txt 
$ echo "IBM,103,20160105" >> /tmp/spooldir/bb/.bb.txt 
$ mv /tmp/spooldir/bb/.bb.txt /tmp/spooldir/bb/bb.txt 

//After few mins 
$ echo "IBM,100.2,20160104" >> /tmp/spooldir/dr/.dr.txt 
$ echo "IBM,103.1,20160105" >> /tmp/spooldir/dr/.dr.txt 
$ mv /tmp/spooldir/dr/.dr.txt /tmp/spooldir/dr/dr.txt