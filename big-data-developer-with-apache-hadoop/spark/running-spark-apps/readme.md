Spark Shell vs. Spark Applications
  #The Spark shell allows interactive exploration and manipulation of data: REPL using Python or Scala
  #Spark applications run as independent programs
    -Python, Scala, or Java
    -For jobs such as ETL processing, streaming, and so on
    
The Spark Context
  #Every Spark program needs a SparkContext object
    -The interactive shell creates one for you
  #In you own Spark application you create you own SparkContext object
    -Named sc by convention
    -Call sc.stop() when program terminates
    
Building a Spark Application: Scala or Java
  #Scala or Java Spark applications must be compiled and assembled into JAR files
    -JAR file will be passed to worker nodes
  #Build details will differ depending on 
    -Version of Hadoop(HDFS)
    -Deployment platform(YARN, Mesos, Spark Standalone)     
  #Consider using and IDE: IntelliJ or Eclipse are two popular IDEs
  
Running an Apache Spark Application
  #The easiest way to run a Spark application is using the spark-submit script
    $ spark-submit --class WordCount MyJarFile.jar /user/home/words.txt
    
Spark Application Cluster Options
  #Spark can run
    -Locally: No distributed processing
    -Locally with multiple worker threads
    -On a cluster               
  