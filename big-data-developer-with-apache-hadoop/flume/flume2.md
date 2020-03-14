# Problem Scenario 21 : You have been given log generating service as below.
````text
startlogs (It will generate continuous logs)
taillogs (You can check , what logs are being generated)
stoplogs (It will stop the log service)
Path where logs are generated using above service : /opt/gen_logs/logs/access.log
Now write a flume configuration file named flume1.conf , using that configuration file dumps
logs in HDFS file system in a directory called flume1. Flume channel should have following
property as well. After every 100 message it should be committed, use non-durable/faster
channel and it should be able to hold maximum 1000 events
````

# Solution
````properties
# Step 1 : Create flume configuration file, with below configuration for source, sink and channel.
#Define source , sink , channel and agent,
agent1.sources = source1
agent1.sinks = sink1
agent1.channels = channel1
# Describe/configure source1
agent1.sources.source1.type = exec
agent1.sources.source1.command = tail -F /opt/gen logs/logs/access.log
## Describe sink1
agent1.sinks.sink1.type = hdfs
agent1.sinks.sink1.hdfs.path = flume1
agent1.sinks.sink1.hdfs.fileType = DataStream
# Now we need to define channell property.
agent1.channels.channel1.type = memory
agent1.channels.channel1.capacity = 1000
agent1.channels.channel1.transactionCapacity = 100
# Bind the source and sink to the channel
agent1.sources.source1.channels = channel1
agent1.sinks.sink1.channel = channel1
````
````bash
# Step 2 : Run below command which will use this configuration file and append data in hdfs.
# Start log service using : 
$ start_logs
# Start flume service:
$ flume-ng agent --conf /home/cloudera/flumeconf --conf-file /home/cloudera/flumeconf/flume1.conf --name agent1 -Dflume.root.logger=DEBUG,INFO,console
# Wait for few mins and than stop log service.
$ stop_logs
````

