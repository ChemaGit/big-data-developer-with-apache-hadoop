/** Question 92
 * Problem Scenario 26 : You need to implement near real time solutions for collecting
 * information when submitted in file with below information. You have been given below
 * directory location (if not available than create it) /tmp/nrtcontent. Assume your departments
 * upstream service is continuously committing data in this directory as a new file (not stream
 * of data, because it is near real time solution). As soon as file committed in this directory
 * that needs to be available in hdfs in /tmp/flume location
 * Data
 * echo "I am preparing for CCA175 from ABCTECH.com" > /tmp/nrtcontent/.he1.txt
 * mv /tmp/nrtcontent/.he1.txt /tmp/nrtcontent/he1.txt
 * After few mins
 * echo "I am preparing for CCA175 from TopTech.com" > /tmp/nrtcontent/.qt1.txt
 * mv /tmp/nrtcontent/.qt1.txt /tmp/nrtcontent/qt1.txt
 * Write a flume configuration file named flume6.conf and use it to load data in hdfs with following additional properties.
 * 1. Spool /tmp/nrtcontent
 * 2. File prefix in hdfs sholuld be events
 * 3. File suffix should be .log
 * 4. If file is not commited and in use than it should have as prefix.
 * 5. Data should be written as text to hdfs
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : Create directory 
$ mkdir /tmp/nrtcontent 

//Step 2 : Create flume configuration file, with below configuration for source, sink and channel and save it in flume6.conf. 
# Name the components on this agent
agent1.sources = source1 
agent1.sinks = sink1 
agent1.channels = channel1 

# Bind the source and sink to the channel
agent1.sources.source1.channels = channel1 
agent1.sinks.sink1.channel = channel1 

# Describe / configure the source
agent1.sources.source1.type = spooldir 
agent1.sources.source1.spoolDir = /tmp/nrtcontent 

#Describe / configure the sink
agent1.sinks.sink1.type = hdfs 
agent1.sinks.sink1.hdfs.path = /tmp/flume 
agent1.sinks.sink1.hdfs.filePrefix = events 
agent1.sinks.sink1.hdfs.fileSuffix = .log 
agent1.sinks.sink1.hdfs.inUsePrefix = _ 
agent1.sinks.sink1.hdfs.fileType = DataStream 

//Step 4 : Run below command which will use this configuration file and append data in hdfs. Start flume service: 
$ flume-ng agent --conf /home/cloudera/flumeconf --conf-file /home/cloudera/flumeconf/flume6.conf --name agent1 -Dflume.root.logger=INFO,console

//Step 5 : Open another terminal and create a file in /tmp/nrtcontent 
$ echo "I am preparing for CCA175 from ABCTechm.com" > /tmp/nrtcontent/.he1.txt 
$ mv /tmp/nrtcontent/.he1.txt /tmp/nrtcontent/he1.txt 

//After few mins 
$ echo "I am preparing for CCA175 from TopTech.com" > /tmp/nrtcontent/.qt1.txt 
$ mv /tmp/nrtcontent/.qt1.txt /tmp/nrtcontent/qt1.txt