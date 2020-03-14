# To get the data from web server logs to HDFS with FLUME
````properties
wh.sources = ws
wh.sinks = hd
wh.channels = mem

# Describe/configure the source
wh.sources.ws.type = exec
wh.sources.ws.command = tail -F /opt/gen_logs/logs/access.log

# Describe the sink
wh.sinks.hd.type = hdfs
wh.sinks.hd.hdfs.path = hdfs://nn01.itversity.com:8020/user/josem32832/flume_demo

wh.sinks.hd.hdfs.filePrefix = FlumeDemo
wh.sinks.hd.hdfs.fileSuffix = .txt
wh.sinks.hd.hdfs.rollInterval = 120
wh.sinks.hd.hdfs.rollSize = 1048576
wh.sinks.hd.hdfs.rollCount = 100
wh.sinks.hd.hdfs.fileType = DataStream

# Use a channel which buffers events in memory
wh.channels.mem.type = memory
wh.channels.mem.capacity = 1000
wh.channels.mem.transactionCapacity = 100

# Bind the source and sink to the channel
wh.sources.ws.channels = mem
wh.sinks.hd.channel = mem

# $ flume-ng agent --name wh --conf-file /home/josem32832/flume_demo/wslogstohdfs/wshdfs.conf
````