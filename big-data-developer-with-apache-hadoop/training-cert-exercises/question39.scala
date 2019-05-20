/** Question 39
  * Problem Scenario 22 : You have been given below comma separated employee information.
  * name,salary,sex,age
  * alok,100000,male,29
  * jatin,105000,male,32
  * yogesh,134000,male,39
  * ragini,112000,female,35
  * jyotsana,129000,female,39
  * valmiki,123000,male,29
  * Use the netcat service on port 44444, and nc above data line by line. Please do the following activities.
  * 1. Create a flume conf file using fastest channel, which write data in hive warehouse
  * directory, in a table called flumeemployee (Create hive table as well tor given data).
  * 2. Write a hive query to read average salary of all employees.
  */
# example.conf: A single-node Flume configuration

# Name the components on this agent
  a1.sources = r1
a1.sinks = k1
a1.channels = c1

# Describe/configure the source
a1.sources.r1.type = netcat
a1.sources.r1.bind = quickstart.cloudera
a1.sources.r1.port = 44444

# Describe the sink
a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = /user/hive/warehouse/hadoopexam.db/flumeemployee
a1.sinks.k1.hdfs.filePrefix = events
a1.sinks.k1.hdfs.fileSuffix = .log
a1.sinks.k1.hdfs.fileType = DataStream
a1.sinks.k1.hdfs.batchSize = 1

# Use a channel which buffers events in memory
  a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

# Bind the source and sink to the channel
  a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1

$ flume-ng agent --conf /home/cloudera/flume_demo --conf-file /home/cloudera/flume_demo/flume5.conf --name a1 -Dflume.root.logger=INFO,console

$ nc quickstart.cloudera 44444

nc> alok,100000,male,29
nc> jatin,105000,male,32
nc> yogesh,134000,male,39
nc> ragini,112000,female,35
nc> jyotsana,129000,female,39
nc> valmiki,123000,male,29

$ hive
  hive> use hadoopexam;
hive> CREATE TABLE flumeemployee(name string,salary int,sex string,age int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/hive/warehouse/hadoopexam.db/flumeemployee';
hive> show tables;
hive> SELECT * FROM flumeemployee;
hive> SELECT ROUND(AVG(salary),2) AS avg_salary FROM flumeemployee;
hive> exit;