# FLUME - WEB SERVER LOGS TO HDFS
````bash
$ ls -ltr /opt/gen_logs
$ cd /opt/gen_logs
  

$ mkdir wslogstohdfs
$ cp example.conf wslogstohdfsf/
$ cd wslogstohdfs
$ mv example.conf wshdfs.conf
$ vi wshdfs.conf
  :%s/a1/wh
:%s/r1/ws
  :%s/c1/mem
````

## example.conf: A single-node Flume configuration
````properties
# Name the components on this agent
wh.sources = ws
wh.sinks = k1
wh.channels = mem

# Describe/configure the source
wh.sources.ws.type = exec
wh.sources.ws.command = tail -F /opt/get_logs/logs/access.log

# Describe the sink
wh.sinks.k1.type = logger

# Use a channel which buffers events in memory
wh.channels.mem.type = memory
wh.channels.mem.capacity = 1000
wh.channels.mem.transactionCapacity = 100

# Bind the source and sink to the channel
wh.sources.ws.channels = mem
wh.sinks.k1.channel = mem

## $ flume-ng agent --name wh --conf-file /home/josem32832/flume_demo/wslogstohdfs/wshdfs.conf
````
