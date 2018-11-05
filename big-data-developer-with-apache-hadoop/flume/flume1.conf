/**
 * Problem Scenario 25 : You have been given below comma separated employee
 * information. That needs to be added in /home/cloudera/flumetest/in.txt file (to do tail source)
 * sex,name,city
 * 1,alok,mumbai
 * 1,jatin,chennai
 * 1,yogesh,kolkata
 * 2,ragini,delhi
 * 2,jyotsana,pune
 * 1,valmiki,banglore
 * Create a flume conf file using fastest non-durable channel, which write data in hive
 * warehouse directory, in two separate tables called flumemaleemployee1 and flumefemaleemployee1
 * (Create hive table as well for given data). Please use tail source with /home/cloudera/flumetest/in.txt file.
 * flumemaleemployee1 : will contain only male employees data 
 * flumefemaleemployee1 : Will contain only woman employees data
 */
//Step 1: We create the hive's tables
$ hive
hive> show databases;
hive> use default;
hive> show tables;
hive> create table flumemaleemployee1(sex int, name string, city string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
hive> create table flumefemaleemployee1(sex int, name string, city string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
hive> show tables;
hive> quit;

//Step 2 : Create below directory and file 
$ mkdir /home/cloudera/flumetest/ 
$ cd /home/cloudera/flumetest/ 
$ gedit in.txt &
//Copy the data without header
// 1,alok,mumbai
// 1,jatin,chennai
// 1,yogesh,kolkata
// 2,ragini,delhi
// 2,jyotsana,pune
// 1,valmiki,banglore

//Step 3: create flume2.conf into /home/cloudera/flumeconf/

agent.sources = tailsrc 
agent.channels = mem1 mem2 
agent.sinks = std1 std2 

agent.sources.tailsrc.channels = mem1 mem2
agent.sources.tailsrc.type = exec 
agent.sources.tailsrc.command = tail -F /home/cloudera/flumetest/in.txt 
agent.sources.tailsrc.batchSize = 1 

## we create a header with the first field of the file
agent.sources.tailsrc.interceptors = i1 
agent.sources.tailsrc.interceptors.i1.type = regex_extractor 
agent.sources.tailsrc.interceptors.i1.regex = (\\d)
agent.sources.tailsrc.interceptors.i1.serializers = t1
agent.sources.tailsrc.interceptors.i1.serializers.t1.name = sex 

## select the header value sex
agent.sources.tailsrc.selector.type = multiplexing 
agent.sources.tailsrc.selector.header = sex 
agent.sources.tailsrc.selector.mapping.1 = mem1 
agent.sources.tailsrc.selector.mapping.2 = mem2 
agent.sources.tailsrc.selector.default = mem1

agent.sinks.std1.type = hdfs 
agent.sinks.std1.channel = mem1 
agent.sinks.std1.hdfs.batchSize = 1 
agent.sinks.std1.hdfs.path = /user/hive/warehouse/flumemaleemployee1 
agent.sinks.std1.hdfs.rollInterval = 1 
agent.sinks.std1.hdfs.fileType = DataStream 

agent.sinks.std2.type = hdfs 
agent.sinks.std2.channel = mem2 
agent.sinks.std2.hdfs.batchSize = 1 
agent.sinks.std2.hdfs.path = /user/hive/warehouse/flumefemaleemployee1 
agent.sinks.std2.hdfs.rollInterval = 1 
agent.sinks.std2.hdfs.fileType = DataStream 

agent.channels.mem1.type = memory 
agent.channels.mem1.capacity = 100 
agent.channels.mem2.type = memory 
agent.channels.mem2.capacity = 100 

//Step 4: launch flume-agent
$ bin/flume-ng agent --conf /home/cloudera/flumeconf --conf-file /home/cloudera/flumeconf/flume2.conf --name agent1 -Dflume.root.logger=INFO,console

//Step 5: Check the data
$ hdfs dfs -cat /user/hive/warehouse/flumefemaleemployee1/FlumeData.1540993941892
$ hdfs dfs -cat /user/hive/warehouse/flumemaleemployee1/FlumeData.1540993941892

hive> select * from flumefemaleemployee1;
hive> select * from flumemaleemployee1;