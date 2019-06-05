/*
Question 1: Correct
Please read this before starting this question,
This question is just for reference and not actual practice question.
There is only 1% chance of getting a flume question in exam.
If you can run netcat on your system, run flume agent command first and then

curl telnet://localhost:56565

You can access flume documentation for help during exam.
You might or might not be provided with flume configuration template file.
Again, there is extremely rare chances of getting a flume question in exam

Instructions:
Create a flume agent configuration such that
consume data from a data source running at localhost ,port number 9083
use a channel that buffers event in memory with buffer capacity as 1000 and transaction capacity as 200
flush the output to an hdfs file sink

You have been provided with flume configuration template file, complete this template file with required configuration and then use the following command to run flume agent

$ flume-ng agent -n telnet-agent -f f.config

Output Requirement
Output should be saved in HDFS Location /user/practice2/problem3/hdfs/sink
Prefix for the files should be .log


Flume Template File
telnet-agent.sources = r1
telnet-agent.sinks = k1
telnet-agent.channels = c1
# Describe/configure the source
telnet-agent.sources.r1.type =
telnet-agent.sources.r1.bind =
telnet-agent.sources.r1.port =
# Describe the sink
telnet-agent.sinks.k1.type =
telnet-agent.sinks.k1.hdfs.path =
telnet-agent.sinks.k1.hdfs.fileSuffix =
telnet-agent.sinks.k1.hdfs.writeFormat =
telnet-agent.sinks.k1.hdfs.fileType =
# Use a channel which buffers events in memory
telnet-agent.channels.c1.type =
telnet-agent.channels.c1.capacity =
telnet-agent.channels.c1.transactionCapacity =
# Bind the source and sink to the channel
telnet-agent.sources.r1.channels = c1
telnet-agent.sinks.k1.channel = c1
*/

telnet-agent.sources = r1
telnet-agent.sinks = k1
telnet-agent.channels = c1
# Describe/configure the source
telnet-agent.sources.r1.type = netcat
telnet-agent.sources.r1.bind = localhost
telnet-agent.sources.r1.port = 44444
# Describe the sink
telnet-agent.sinks.k1.type = hdfs
telnet-agent.sinks.k1.hdfs.path = /user/practice2/problem3/hdfs/sink
  telnet-agent.sinks.k1.hdfs.fileSuffix = .log
telnet-agent.sinks.k1.hdfs.writeFormat = Text
telnet-agent.sinks.k1.hdfs.fileType = DataStream
# Use a channel which buffers events in memory
  telnet-agent.channels.c1.type = memory
telnet-agent.channels.c1.capacity = 1000
telnet-agent.channels.c1.transactionCapacity = 200
# Bind the source and sink to the channel
  telnet-agent.sources.r1.channels = c1
telnet-agent.sinks.k1.channel = c1

$ flume-ng agent --name telnet-agent --conf-file /home/cloudera/flume_demo/problem1.conf
$ nc localhost 44444
Hi, How are you?
  I'm fine thank you
  Where are you?
  I'm from Madrid, Spain
Good, nice to meet you
  nice to meet you too
Bye for now

$ hdfs dfs -ls /user/practice2/problem3/hdfs/sink
$ hdfs dfs -cat $ hdfs dfs -ls /user/practice2/problem3/hdfs/sink/*.log