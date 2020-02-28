# Question 30
````text
   Problem Scenario 94 : You have to run your Spark application on yarn with each executor 20GB and number of executors should be 50. Please replace XXX, YYY, ZZZ
   export HADOOP_CONF_DIR=DIR
   ./bin/spark-submit \
   --class com.hadoopexam.MyTask \
   XXX\
   --deploy-mode cluster \ # can be client for client mode
   YYY\
   ZZZ \
   /path/to/hadoopexam.jar \
   1000
````   
````properties  
XXX => --master yarn
YYY => --executor-memory 20GB
ZZZ => --num-executors 50

$ spark-submit \
--class com.hadoopexam.MyTask \
--master yarn \
--deploy-mode cluster \ # can be client for client mode
--executor-memory 20GB \
--num-executors 50 /path/to/hadoopexam.jar 1000
````
