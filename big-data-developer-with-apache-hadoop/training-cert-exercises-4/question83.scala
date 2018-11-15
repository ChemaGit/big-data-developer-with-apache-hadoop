/** Question 83
 * Problem Scenario 96 : Your spark application required extra Java options as below. -XX:+PrintGCDetails-XX:+PrintGCTimeStamps
 * Please replace the XXX values correctly
 * ./bin/spark-submit --name "My app" --master local[4] --conf spark.eventLog.enabled=false --conf XXX hadoopexam.jar
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: 
Solution XXX: "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" 
//Notes: ./bin/spark-submit \ --class <main-class> --master <master-url> \ --deploy-mode <deploy-mode> \ -conf <key>=<value> \ # other options <application-jar> \ [application-arguments] 
//Here, conf is used to pass the Spark related configs which are required for the application to run like any specific property(executor memory) 
//or if you want to override the default property which is set in Spark-default.conf.

XXX ===> "spark.executor.extraJavaOptions=-XX:+PrintGCDetails-XX:+PrintGCTimeStamps"

$ ./bin/spark-submit --name "My app" --master local[4] --conf spark.eventLog.enabled=false --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails-XX:+PrintGCTimeStamps" hadoopexam.jar