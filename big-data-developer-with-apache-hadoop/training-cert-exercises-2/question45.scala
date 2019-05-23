/** Question 45
  * Problem Scenario 93 : You have to run your Spark application with locally 8 thread or
  * locally on 8 cores. Replace XXX with correct values.
  * spark-submit --class com.hadoopexam.MyTask XXX \ -deploy-mode cluster /SPARK_HOME/lib/hadoopexam.jar 10
  */
XXX ==> --master "local[8]"

$ spark-submit --class com.hadoopexam.MyTask \
  --master "local[8]" \
  --deploy-mode cluster /SPARK_HOME/lib/hadoopexam.jar 10
/**
Explanation: Solution XXX: --master local[8] 
Notes : The master URL passed to Spark can be in one of the following formats: Master URL Meaning local Run Spark locally with one worker thread (i.e. no parallelism at all). 
local[K] Run Spark locally with K worker threads (ideally, set this to the number of cores on your machine). 
local[*] Run Spark locally with as many worker threads as logical cores on your machine. 
spark://HOST:PORT Connect to the given Spark standalone cluster master. The port must be whichever one your master is configured to use, which is 7077 by default. 
mesos://HOST:PORT Connect to the given Mesos cluster. The port must be whichever one your is configured to use, which is 5050 by default. Or, for a Mesos cluster using ZooKeeper, use mesos://zk://.... 
To submit with --deploy-mode cluster, the HOST:PORT should be configured to connect to the MesosClusterDispatcher.
yarn Connect to a YARN cluster in client or cluster mode depending on the value of - deploy-mode.
The cluster location will be found based on the HADOOP CONF DIR or YARN CONF DIR variable.
  */