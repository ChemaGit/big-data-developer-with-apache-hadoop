/** Question 18
  * Problem Scenario 95 : You have to run your Spark application on yarn with each executor
  * Maximum heap size to be 512MB and Number of processor cores to allocate on each
  * executor will be 1 and Your main application required three values as input arguments V1 V2 V3.
  * Please replace XXX, YYY, ZZZ
  * ./bin/spark-submit --class com.hadoopexam.MyTask --master yarn-cluster --num-executors 3 --driver-memory 512m XXX YYY lib/hadoopexam.jar ZZZ
  */
XXX => executor-memory 512MB
  YYY => executor-cores 1
ZZZ => V1 V2 V3


  ./bin/spark-submit \
  --class com.hadoopexam.MyTask \
  --master yarn-cluster \
  --num-executors 3 \
  --driver-memory 512m \
--executor-memory 512MB \
--executor-cores 1 \
  lib/hadoopexam.jar V1 V2 V3

Notes : spark-submit on yarn options Option Description archives Comma-separated list of archives to be extracted into the working directory of each executor.
The path must be globally visible inside your cluster;
see Advanced Dependency Management.
--executor-cores Number of processor cores to allocate on each executor. Alternatively, you can use the spark.executor.cores property,
--executor-memory Maximum heap size to allocate to each executor. Alternatively, you can use the spark.executor.memory property.
--num-executors Total number of YARN containers to allocate for this application. Alternatively, you can use the spark.executor.instances property. queue YARN queue to submit to.