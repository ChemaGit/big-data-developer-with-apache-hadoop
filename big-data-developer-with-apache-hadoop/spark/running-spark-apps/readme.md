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
  #Local mode is useful for	development	and	testing
  #Production use is almost	always on a	cluster
  
Supported Cluster Resource Managers  
  #Hadoop YARN
  	-Most common for production sites
  	-Allows sharing cluster resources with other applications
  #Spark standalone
  	-Useful for learning, testing, development or small systems
  #Apache Mesos	 
  
Runnig a Spark Application locally
  #Local options
    -local[*] runs locally with as many threads as cores (default)
    -local[n] runs locally with n threads
    -local runs locally with a single thread
    
    $ spark-shell --master local[*]
    $ spark-shell --master local[8]
    $ spark-shell --master local
    
  #Language Python
    $ spark-submit --master 'local[3]' WordCount.py fileURL
  #Language Scala
    $ spark-submit --master 'local[3]' --class WordCount MyJarFile.jar fileURL 
    
Running a Spark Application on a Cluster
  #Use spark-submit --master to specify cluster option
    -Cluster options
      yarn-client
      yarn-cluster
      spark://masternode:port(Spark Standalone)
      mesos://masternode:port(Mesos)
  #Language Python
    $ spark-submit --master yarn-cluster WordCount.py fileURL
  #Language Scala
    $ WordCount MyJarFile.jar fileURL
                     	
Starting the Spark Shell on a Cluster
  #The Spark shell can also be run on a cluster
  #pyspark and spark-shell both hava a --master option
    -yarn(client mode only)
    -Spark or Mesos cluster manager URL  
    -local[*] runs with as many threads as cores (default)
    -local[n] runs locally with n worker threads
    -local runs locally without distrubuted processing
  #Language Python
    $ pyspark --master yarn
  #Language Scala
    $ spark-shell --master yarn    
    
Options	when Submitting	a Spark	Application	to a Cluster	
  -Some	other spark-submit options for	clusters 
	#--jars: Additional JAR file(Scala and Java only)
	#--py-file: Additional Python files(Python only)
	#--driver-java-options: Parameters to pass to the driver JVM
	#--executor-memory: Memory per executor(for example: 1000m, 2g)(Default: 1g)
	#--packages: Maven coordinates of an external libray to include
  -Plus several YARN-specific options
  	#--num-executors: Number of executors to start
  	#--executor-cores: Number cores to allocate for each executor
  	#--queue: YARN queue to submit the application to
  -Show all available options
    #--help
     		  
Dynamic Resource Allocation
  #Dynamic allocation in YARN is enabled by default starting in CDH 5.5
    -Enabled at a site level in YARN, not application level
    -Can be disabled for an individual application
      -Specify the --num-executors flag when using spark-submit
      -Or set properly spark.dynamicAllocation.enabled to false  
      
 The Spark Application Web UI
	#Viewing Spark Job History
		-Spark UI is only available while the app is running
		-Use Spark History Server to view metrics for a completed app
	#Accesing the History Server
		-For local jobs, access by URL
		-For YARN jobs, click History link YARN UI
		
Submitting a Spark Application Locally
 $spark-submit --class stubs.CountJPGs target/countjpgs-1.0.jar /loudacre/weblogs/*
 $spark-submit --class stubs.CountJPGs --master 'local[*]' target/countjpgs-1.0.jar /loudacre/weblogs/*
 
Submitting a Spark Application to the Cluster
  $spark-submit --class stubs.CountJPGs --master yarn-client target/countjpgs-1.0.jar /loudacre/weblogs/*
 
		
      
    
              
  