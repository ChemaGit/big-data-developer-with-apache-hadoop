/** Question 17
  * Problem Scenario 28 : You need to implement near real time solutions for collecting
  * information when submitted in file with below
  * Data
  * echo "IBM,100,20160104" >> /tmp/spooldir2/.bb.txt
  * echo "IBM,103,20160105" >> /tmp/spooldir2/.bb.txt
  * mv /tmp/spooldir2/.bb.txt /tmp/spooldir2/bb.txt
  * After few mins
  * echo "IBM,100.2,20160104" >> /tmp/spooldir2/.dr.txt
  * echo "IBM,103.1,20160105" >> /tmp/spooldir2/.dr.txt
  * mv /tmp/spooldir2/.dr.txt /tmp/spooldir2/dr.txt
  * You have been given below directory location (if not available than create it) /tmp/spooldir2
  * As soon as file committed in this directory that needs to be available in hdfs in
  * /tmp/flume/primary as well as /tmp/flume/secondary location.
  * However, note that/tmp/flume/secondary is optional, if transaction failed which writes in
  * this directory need not to be rollback.
  * Write a flume configuration file named flumeS.conf and use it to load data in hdfs with following additional properties .
  * 1. Spool /tmp/spooldir2 directory
  * 2. File prefix in hdfs sholuld be events
  * 3. File suffix should be .log
  * 4. If file is not committed and in use than it should have _ as prefix.
  * 5. Data should be written as text to hdfs
  */

$ mkdir /tmp/spooldir2
  $ gedit /home/cloudera/flume/flumeS.conf

# flumeS.conf: A single-node Flume configuration

# Name the components on this agent
  a1.sources = r1
a1.sinks = k1 k2
  a1.channels = c1 c2

# Describe/configure the source
a1.sources.r1.type = spooldir
a1.sources.r1.spoolDir = /tmp/spooldir2

a1.sources.r1.selector.type = replicating
a1.sources.r1.selector.optional = c2

# Describe the sink
a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = /tmp/flume/primary
  a1.sinks.k1.hdfs.fileType = DataStream
a1.sinks.k1.hdfs.filePrefix = events
a1.sinks.k1.hdfs.fileSuffix = .log
a1.sinks.k1.hdfs.inUsePrefix = _

a1.sinks.k2.type = hdfs
a1.sinks.k2.hdfs.path = /tmp/flume/secondary
  a1.sinks.k2.hdfs.fileType = DataStream
a1.sinks.k2.hdfs.filePrefix = events
a1.sinks.k2.hdfs.fileSuffix = .log
a1.sinks.k2.hdfs.inUsePrefix = _

# Use a channel which buffers events in memory
  a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

a1.channels.c2.type = file

# Bind the source and sink to the channel
  a1.sources.r1.channels = c1 c2
  a1.sinks.k1.channel = c1
a1.sinks.k2.channel = c2

$ bin/flume-ng agent --conf /home/cloudera/flume --conf-file /home/cloudera/flume/flumeS.conf --name a1 -Dflume.root.logger=INFO,console


$ echo "IBM,100,20160104" >> /tmp/spooldir2/.bb.txt
$ echo "IBM,103,20160105" >> /tmp/spooldir2/.bb.txt
$ mv /tmp/spooldir2/.bb.txt /tmp/spooldir2/bb.txt
// After few mins
$ echo "IBM,100.2,20160104" >> /tmp/spooldir2/.dr.txt
$ echo "IBM,103.1,20160105" >> /tmp/spooldir2/.dr.txt
$ mv /tmp/spooldir2/.dr.txt /tmp/spooldir2/dr.txt

$ hdfs dfs -ls /tmp/flume/primary
$ hdfs dfs -cat /tmp/flume/primary/*

$ hdfs dfs -ls /tmp/flume/secondary
$ hdfs dfs -cat /tmp/flume/secondary/*

  
