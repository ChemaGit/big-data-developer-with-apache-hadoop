/**
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

//Explanation: Solution : 
//Step 1 : Create hive table for flumeemployee.' 
$ hive
hive> show databases;
hive> use default;
hive> CREATE TABLE flumeemployee ( name string, salary int, sex string, age int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
hive> show tables;
//Step 2 : Create flume configuration file, with below configuration for source, sink and channel and save it in flume4.conf. 

#Define source , sink , channel and agent, 
agent1.sources = source1 
agent1.sinks = sink1 
agent1.channels = channel1 

# Describe/configure source1 
agent1.sources.source1.type = netcat 
agent1.sources.source1.bind = 127.0.0.1 
agent1.sources.source1.port = 44444
 
## Describe sink1  
agent1.sinks.sink1.type = hdfs 
agent1.sinks.sink1.hdfs.path = /user/hive/warehouse/flumeemployee 
agent1.sinks.sink1..hdfs.writeFormat = Text 
agent1.sinks.sink1.hdfs.fileType = DataStream 

# Now we need to define channel1 property. 
agent1.channels.channel1.type = memory 
agent1.channels.channel1.capacity = 1000 
agent1.channels.channel1.transactionCapacity = 100 

# Bind the source and sink to the channel 
agent1.sources.source1.channels = channel1 
agent1.sinks.sink1.channel = channel1 

//Step 3 : Run below command which will use this configuration file and append data in hdfs. Start flume service: 
$ flume-ng agent --conf /home/cloudera/flumeconf --conf-file /home/cloudera/flumeconf/flume4.conf --name agent1 -Dflume.root.logger=ERROR,DEBUG,INFO,console

//Step 4 : Open another terminal and use the netcat service. 
$ nc localhost 44444 
//Step 5 : Enter data line by line. 
alok,100000,male,29 
jatin,105000,male,32 
yogesh,134000,male,39 
ragini,112000,female,35 
jyotsana,129000,female,39 
valmiki,123000,male,29 
//Step 6 : Open hue and check the data is available in hive table or not. 
//step 7 : Stop flume service by pressing ctrl+c 
//Step 8 : Calculate average salary on hive table using below query. You can use either hive command line tool or hue. 
$ hive
hive> show databases;
hive> use default;
hive> show tables;
hive> select avg(salary) from flumeemployee;
hive> quit;