# Question 83
````text
   Problem Scenario 96 : Your spark application required extra Java options as below. -XX:+PrintGCDetails-XX:+PrintGCTimeStamps
   Please replace the XXX values correctly
   ./bin/spark-submit --name "My app" --master local[4] --conf spark.eventLog.enabled=false --conf XXX hadoopexam.jar
````   
 
````properties 
XXX => "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"

$ ./bin/spark-submit \
   --name "My app" \
   --master local[4] \
   --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
   spark.eventLog.enabled=false \
   hadoopexam.jar
````