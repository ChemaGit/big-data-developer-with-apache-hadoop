# Problem 7: 
````text
1. This step comprises of three substeps. Please perform tasks under each subset completely  
- using sqoop pull data from MYSQL orders table into /user/cloudera/problem7/prework as AVRO data file using only one mapper
- Pull the file from /user/cloudera/problem7/prework into a local folder named flume-avro
- create a flume agent configuration such that it has an avro source at localhost and port number 11112,  
  a jdbc channel and an hdfs file sink at /user/cloudera/problem7/sink
- Use the following command to run an avro client flume-ng avro-client -H localhost -p 11112 -F <<Provide your avro file path here>>
2. The CDH comes prepackaged with a log generating job. start_logs, stop_logs and tail_logs. 
  Using these as an aid and provide a solution to below problem. The generated logs can be found at path /opt/gen_logs/logs/access.log
- run start_logs
- write a flume configuration such that the logs generated by start_logs are dumped into HDFS at location /user/cloudera/problem7/step2. 
  The channel should be non-durable and hence fastest in nature.
  The channel should be able to hold a maximum of 1000 messages and should commit after every 200 messages.
- Run the agent.
- confirm if logs are getting dumped to hdfs.
- run stop_logs.
````
````bash
# 1. This step comprises of three substeps. Please perform tasks under each subset completely
# using sqoop pull data from MYSQL orders table into /user/cloudera/problem7/prework as AVRO data file using only one mapper
$ sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username root \
--password cloudera \
--table orders \
--delete-target-dir \
--target-dir /user/cloudera/problem7/prework \
--as-avrodatafile \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

$ hdfs dfs -ls /user/cloudera/problem7/prework/
$ avro-tools tojson hdfs://quickstart.cloudera/user/cloudera/problem7/prework/part-m-00000.avro | head -n 20
$ avro-tools cat --offset 1 --limit 5 hdfs://localhost/user/cloudera/problem7/prework/part-m-00000.avro -
$ avro-tools getschema hdfs://quickstart.cloudera/user/cloudera/problem7/prework/part-m-00000.avro >> /home/cloudera/files/orders.avsc
# Pull the file from /user/cloudera/problem7/prework into a local folder named flume-avro
$ mkdir /home/cloudera/flume-avro
$ hdfs dfs -get /user/cloudera/problem7/prework/part-m-00000.avro /home/cloudera/flume-avro
$ mv part-m-00000.avro orders.avro
````
````properties
# create a flume agent configuration such that it has an avro source at localhost and port number 11112,  
# a jdbc channel and an hdfs file sink at /user/cloudera/problem7/sink
# Agent Name = step1

# Name the source, channel and sink
step1.sources = avro-source
step1.channels = jdbc-channel
step1.sinks = file-sink

# Source configuration
step1.sources.avro-source.type = avro
step1.sources.avro-source.port = 11112
step1.sources.avro-source.bind = quickstart.cloudera

# Describe the sink
step1.sinks.file-sink.type = hdfs
step1.sinks.file-sink.hdfs.path = /user/cloudera/problem7/sink
step1.sinks.file-sink.hdfs.fileType = DataStream
step1.sinks.file-sink.hdfs.fileSuffix = .avro
step1.sinks.file-sink.serializer = avro_event
step1.sinks.file-sink.serializer.compressionCodec = snappy

# Describe the type of channel --  Use memory channel if jdbc channel does not work
step1.channels.jdbc-channel.type = jdbc

# Bind the source and sink to the channel
step1.sources.avro-source.channels = jdbc-channel
step1.sinks.file-sink.channel = jdbc-channel
````
````bash
# Use the following command to run an avro client flume-ng avro-client -H localhost -p 11112 -F <<Provide your avro file path here>>
# Run the flume agent
$ flume-ng agent --conf /home/cloudera/flume_demo --conf-file /home/training/flume_demo/flume9.conf --name step1 -Dflume.root.logger=INFO,console

# Run the flume Avro client
$ flume-ng avro-client -H localhost -p 11112 -F /home/cloudera/flume-avro/orders.avro
````
````text
2. The CDH comes prepackaged with a log generating job. start_logs, stop_logs and tail_logs. 
   Using these as an aid and provide a solution to below problem. 
   The generated logs can be found at path /opt/gen_logs/logs/access.log
- run start_logs
$ start_logs
- write a flume configuration such that the logs generated by start_logs are dumped into HDFS at location /user/cloudera/problem7/step2. 
  The channel should be non-durable and hence fastest in nature.
  The channel should be able to hold a maximum of 1000 messages and should commit after every 200 messages.
````
````properties
# Name the components on this agent
step2.sources = r1
step2.sinks = k1
step2.channels = c1

# Describe/configure the source
step2.sources.r1.type = exec
step2.sources.r1.command = tail -F /opt/gen_logs/logs/access.log

# Use a channel which buffers events in memory
step2.channels.c1.type = memory
step2.channels.c1.capacity = 1000
step2.channels.c1.transactionCapacity = 200

# Describe the sink
step2.sinks.k1.type = hdfs
step2.sinks.k1.hdfs.path = /user/cloudera/problem7/step2/
step2.sinks.k1.hdfs.filePrefix = logs
step2.sinks.k1.hdfs.fileType = DataStream
step2.sinks.k1.hdfs.fileSuffix = .log

# Bind the source and sink to the channel
step2.sources.r1.channels = c1
step2.sinks.k1.channel = c1
````

````text
# Run the agent.
$ flume-ng agent --conf /home/cloudera/flume_demo --conf-file /home/cloudera/flume_demo/flume10.conf --name step2 -Dflume.root.logger=INFO,console
-confirm if logs are getting dumped to hdfs.
$ hdfs dfs -cat /user/cloudera/problem7/step2/l*
# run stop_logs.
$ stop_logs
````
