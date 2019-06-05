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
  * As soon as file committed in this directory that needs to be available in hdfs in /user/cloudera/flume/finance location in a single directory.
  * Write a flume configuration file named flume7.conf and use it to load data in hdfs with following additional properties .
  * 1. Spool /tmp/spooldir/bb and /tmp/spooldir/dr
  * 2. File prefix in hdfs should be events
  * 3. File suffix should be .log
  * 4. If file is not commited and in use than it should have _ as prefix.
  * 5. Data should be written as text to hdfs
  */
$ mkdir /tmp/spooldir
  $ mkdir /tmp/spooldir/bb
  $ mkdir /tmp/spooldir/dr

# example.conf: A single-node Flume configuration

# Name the components on this agent
  a1.sources = r1 r2
  a1.sinks = k1
a1.channels = c1

# Describe/configure the source
a1.sources.r1.type = spooldir
a1.sources.r1.spoolDir = /tmp/spooldir/bb

a1.sources.r2.type = spooldir
a1.sources.r2.spoolDir = /tmp/spooldir/dr

# Describe the sink
a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = /user/cloudera/flume/finance
  a1.sinks.k1.hdfs.filePrefix = events
a1.sinks.k1.hdfs.fileSuffix = .log
a1.sinks.k1.hdfs.fileType = DataStream
a1.sinks.k1.hdfs.inUsePrefix = _


# Use a channel which buffers events in memory
  a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

# a1.channels.c1.type = file

# Bind the source and sink to the channel
  a1.sources.r1.channels = c1
a1.sources.r2.channels = c1
a1.sinks.k1.channel = c1

$ flume-ng agent --conf /home/cloudera/flume_demo --conf-file /home/cloudera/flume_demo/flume_finance.conf --name a1 -Dflume.root.logger=INFO,console

$ echo "IBM,100,20160104" >> /tmp/spooldir/bb/.bb.txt
$ echo "IBM,103,20160105" >> /tmp/spooldir/bb/.bb.txt
$ mv /tmp/spooldir/bb/.bb.txt /tmp/spooldir/bb/bb.txt
// After few mins
$ echo "IBM,100.2,20160104" >> /tmp/spooldir/dr/.dr.txt
$ echo "IBM,103.1,20160105" >> /tmp/spooldir/dr/.dr.txt
$ mv /tmp/spooldir/dr/.dr.txt /tmp/spooldir/dr/dr.txt

$ hdfs dfs -cat /user/cloudera/flume/finance/*.log