/** Question 34
  * Problem Scenario 21 : You have been given log generating service as below.
  * start_logs (It will generate continuous logs)
  * tail_logs (You can check , what logs are being generated)
  * stop_logs (It will stop the log service)
  * Path where logs are generated using above service : /opt/gen_logs/logs/access.log
  * Now write a flume configuration file named flume1.conf , using that configuration file dumps
  * logs in HDFS file system in a directory called flume1. Flume channel should have following
  * property as well. After every 100 message it should be committed, use non-durable/faster
  * channel and it should be able to hold maximum 1000 events
  */
$ gedit /home/cloudera/flume_demo/flume1.conf &

# example.conf: A single-node Flume configuration

# Name the components on this agent
  a1.sources = r1
a1.sinks = k1
a1.channels = c1

# Describe/configure the source
a1.sources.r1.type = exec
a1.sources.r1.command = tail -F /opt/gen_logs/logs/access.log

a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = /user/cloudera/flume1
  a1.sinks.k1.hdfs.filePrefix = events
a1.sinks.k1.hdfs.fileSuffix = .log
a1.sinks.k1.hdfs.fileType = DataStream

# Use a channel which buffers events in memory
  a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

# Bind the source and sink to the channel
  a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1

$ flume-ng agent --conf /home/cloudera/flume_demo/ --conf-file /home/cloudera/flume_demo/flume1.conf --name a1 -Dflume.root.logger=INFO,console

$ start_logs
  // after some time
  $ stop_logs

$ hdfs dfs -ls /user/cloudera/flume1
$ hdfs dfs -cat /user/cloudera/flume1/*.log