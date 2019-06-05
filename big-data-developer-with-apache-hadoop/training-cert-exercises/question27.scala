/** Question 27
  * Problem Scenario 92 : You have been given a spark scala application, which is bundled in jar named hadoopexam.jar.
  * Your application class name is com.hadoopexam.MyTask
  * You want that while submitting your application should launch a driver on one of the cluster node.
  * Please complete the following command to submit the application.
  * spark-submit XXX -master yarn \ YYY \ SPARK HOME/lib/hadoopexam.jar 10
  */
XXX --> --class com.hadoopexam.MyTask
YYY --> --deploy-mode cluster

$ spark-submit --class com.hadoopexam.MyTask \
  --master yarn \
  --deploy-mode cluster \
SPARK HOME/lib/hadoopexam.jar 10