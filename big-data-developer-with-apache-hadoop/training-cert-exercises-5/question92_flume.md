# Question 92
````text
   Problem Scenario 26 : You need to implement near real time solutions for collecting
   information when submitted in file with below information. You have been given below
   directory location (if not available than create it) /tmp/nrtcontent. Assume your departments
   upstream service is continuously committing data in this directory as a new file (not stream
   of data, because it is near real time solution). As soon as file committed in this directory
   that needs to be available in hdfs in /user/cloudera/question92/flume location
   Data
   echo "I am preparing for CCA175 from ABCTECH.com" >> /tmp/nrtcontent/.he1.txt
   mv /tmp/nrtcontent/.he1.txt /tmp/nrtcontent/he1.txt
   After few mins
   echo "I am preparing for CCA175 from TopTech.com" > /tmp/nrtcontent/.qt1.txt
   mv /tmp/nrtcontent/.qt1.txt /tmp/nrtcontent/qt1.txt
   Write a flume configuration file named flume_92.conf and use it to load data in hdfs with following additional properties.
   1. Spool /tmp/nrtcontent
   2. File prefix in hdfs sholuld be events
   3. File suffix should be .log
   4. If file is not commited and in use than it should have inUse as prefix.
   5. Data should be written as text to hdfs
````   
  
````properties  
$ mkdir /tmp/nrtcontent
````

````properties
# Name the components on this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1

# Describe/configure the source
a1.sources.r1.type = spooldir
a1.sources.r1.spoolDir = /tmp/nrtcontent

# Describe the sink
a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = /user/cloudera/question92/flume
a1.sinks.k1.hdfs.filePrefix = events
a1.sinks.k1.hdfs.fileType = DataStream
a1.sinks.k1.hdfs.fileSuffix = .log
a1.sinks.k1.hdfs.inUsePrefix = inUse

# Use a channel which buffers events in memory
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
````

````properties
$ flume-ng agent --conf /home/cloudera/flume_demo --conf-file /home/cloudera/flume_demo/flume_92.conf --name a1 -Dflume.root.logger=INFO,console

$ echo "I am preparing for CCA175 from ABCTECH.com" >> /tmp/nrtcontent/.he1.txt
$ mv /tmp/nrtcontent/.he1.txt /tmp/nrtcontent/he1.txt
// After few mins
$ echo "I am preparing for CCA175 from TopTech.com" > /tmp/nrtcontent/.qt1.txt
$ mv /tmp/nrtcontent/.qt1.txt /tmp/nrtcontent/qt1.txt

$ hdfs dfs -ls /user/cloudera/question92/flume
$ hdfs dfs -cat /user/cloudera/question92/flume/*.log
````