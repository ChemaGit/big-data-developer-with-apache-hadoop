/**
 * Problem Scenario 94 : You have to run your Spark application on yarn with each executor 20GB and number of executors should be 50. Please replace XXX, YYY, ZZZ
 * export HADOOP_CONF_DIR=DIR 
 * ./bin/spark-submit \
 * --class com.hadoopexam.MyTask \
 * XXX\
 * --deploy-mode cluster \ # can be client for client mode
 * YYY\
 * ZZZ \
 * /path/to/hadoopexam.jar \
 * 1000
 */
Explanation: Solution XXX: --master yarn 
                      YYY: --executor-memory 20G 
                      ZZZ: --num-executors 50

$ ./bin/spark-submit --class com.hadoopexam.MyTask --master yarn --deploy-mode cluster --executor-memory 20G --num-executors 50 /path/to/hadoopexam.jar 1000
