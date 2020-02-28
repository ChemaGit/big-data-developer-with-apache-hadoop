# Question 36
````text
   Problem Scenario 25 : You have been given below comma separated employee
   information. That needs to be added in /home/cloudera/flume_demo/in.txt file (to do tail source)
   sex,name,city
   1,alok,mumbai
   1,jatin,chennai
   1,yogesh,kolkata
   2,ragini,delhi
   2,jyotsana,pune
   1,valmiki,banglore
   Create a flume conf file using fastest non-durable channel, which write data in hive
   warehouse directory, in two separate tables called flumemaleemployee1 and flumefemaleemployee1
   (Create hive table as well for given data). Please use tail source with /home/cloudera/flumetest/in.txt file.
   flumemaleemployee1 : will contain only male employees data
   flumefemaleemployee1 : Will contain only woman employees data
````   

````properties  
# copy data without header
$ gedit /home/cloudera/flume_demo/in.txt &

$ gedit /home/cloudera/flume_demo/flume4.conf &
````

````properties
# example.conf: A single-node Flume configuration

# Name the components on this agent
a1.sources = r1
a1.sinks = k1 k2
a1.channels = c1 c2

# Describe/configure the source
a1.sources.r1.type = exec
a1.sources.r1.command = tail -F /home/cloudera/flume_demo/in.txt

## we create a header with the first field of the file
a1.sources.r1.interceptors = i1
a1.sources.r1.interceptors.i1.type = regex_extractor
a1.sources.r1.interceptors.i1.regex = (\\d)
a1.sources.r1.interceptors.i1.serializers = t1
a1.sources.r1.interceptors.i1.serializers.t1.name = sex

## select the header value sex
a1.sources.r1.selector.type = multiplexing
a1.sources.r1.selector.header = sex
a1.sources.r1.selector.mapping.1 = c1
a1.sources.r1.selector.mapping.2 = c2
a1.sources.r1.selector.default = c1

# Describe the sink
a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = /user/hive/warehouse/flumemaleemployee1
a1.sinks.k1.hdfs.filePrefix = events
a1.sinks.k1.hdfs.fileSuffix = .log
a1.sinks.k1.hdfs.fileType = DataStream

a1.sinks.k2.type = hdfs
a1.sinks.k2.hdfs.path = /user/hive/warehouse/flumefemaleemployee1
a1.sinks.k2.hdfs.filePrefix = events
a1.sinks.k2.hdfs.fileSuffix = .log
a1.sinks.k2.hdfs.fileType = DataStream

# Use a channel which buffers events in memory
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

a1.channels.c2.type = memory
a1.channels.c2.capacity = 1000
a1.channels.c2.transactionCapacity = 100

# Bind the source and sink to the channel
a1.sources.r1.channels = c1 c2
a1.sinks.k1.channel = c1
a1.sinks.k2.channel = c2
````

````properties
$ flume-ng agent --conf /home/cloudera/flume_demo --conf-file /home/cloudera/flume_demo/flume4.conf --name a1 -Dflume.root.logger=INFO,console

$ hive
hive> CREATE TABLE flumemaleemployee1(sex int,name string,city string) \
      ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' \
      STORED AS TEXTFILE \
      LOCATION '/user/hive/warehouse/flumemaleemployee1';

hive> CREATE TABLE flumefemaleemployee1(sex int,name string,city string) \
      ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' \
      STORED AS TEXTFILE \
      LOCATION '/user/hive/warehouse/flumefemaleemployee1';

hive> select  from flumemaleemployee1;
hive> select  from flumefemaleemployee1;
````