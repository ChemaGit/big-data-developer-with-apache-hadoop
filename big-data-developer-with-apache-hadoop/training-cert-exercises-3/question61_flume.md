# Question 61
````text
   Problem Scenario 24 : You have been given below comma separated employee information.
   Data Set:
   name,salary,sex,age
   alok,100000,male,29
   jatin,105000,male,32
   yogesh,134000,male,39
   ragini,112000,female,35
   jyotsana,129000,female,39
   valmiki,123000,male,29
   Requirements:
   Use the netcat service on port 44444, and nc above data line by line. Please do the following activities.
   1. Create a flume conf file using fastest channel, which write data in hive warehouse directory, in a table called flumemaleemployee (Create hive table as well tor given data).
   2. While importing, make sure only male employee data is stored.
````  
````properties
$ hive
hive> use hadoopexam;
hive> CREATE TABLE flumemaleemployee(name string, salary int, sex string, age int) 
      ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
      LOCATION '/user/hive/warehouse/hadoopexam.db/flumemaleemployee';
hive> show tables;
````

````properties
# example.conf: A single-node Flume configuration

# Name the components on this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1

# Describe/configure the source
a1.sources.r1.type = netcat
a1.sources.r1.bind = quickstart.cloudera
a1.sources.r1.port = 44444
a1.sources.r1.interceptors = i1
a1.sources.r1.interceptors.i1.type = regex_filter
a1.sources.r1.interceptors.i1.excludeEvents = true
a1.sources.r1.interceptors.i1.regex = (female)

# Describe the sink
a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = /user/hive/warehouse/hadoopexam.db/flumemaleemployee
a1.sinks.k1.hdfs.fileType = DataStream
a1.sinks.k1.hdfs.hdfs.rollCount = 1
a1.sinks.k1.hdfs.hdfs.batchSize = 1

# Use a channel which buffers events in memory
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
````

````properties
$ flume-ng agent --conf /home/cloudera/flume_demo --conf-file /home/cloudera/flume_demo/flumemaleemployee.conf --name a1 -Dflume.root.logger=INFO,console

$ nc quickstart.cloudera 44444

> alok,100000,male,29
> jatin,105000,male,32
> yogesh,134000,male,39
> ragini,112000,female,35
> jyotsana,129000,female,39
> valmiki,123000,male,29

hive> select * from flumemaleemployee;
hive> exit;
````