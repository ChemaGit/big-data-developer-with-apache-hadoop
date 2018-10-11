#Configuring Apache Spark Properties
  Spark Application Configuration
    - Spark provides numerous properties for configuring your application
      	*Some examples properties
      	     + spark.master
      	     + spark.app.name
      	     + spark.local.dir ==> Where to estore local files such as shuffle output (default: /tmp)
      	     + spark.ui.port ==> Port to run the Spark Application UI(default 4040)
      	     + spark.executor.memory ==> How much memory to allocate to each Executor (default 1g)
      	     + And many more: See Spark Configuration page in the Spark documentation for more details.
    - Spark application Configuration Options
    	*Spark applications can be configured
    		+ Declaratively, using spark-submit options or properties files
    		+ Programmatically, within your application code
    - Declarative Configurations options 		
    	*spark-submit script ==> Examples
    		+ spark-submit --driver-memory 500M
    		+ spark-submit --conf spark.executor.cores=4
        *Properties file
        	+ Tab-or space-separated list of properties and values
        	+ Load with ==> spark-submit --properties-file filename
        	+ Example:
        		spark.master	yarn-cluster
        		spark.local.dir	/tmp
        		spark.ui.port	4041
        *Site defaults properties file
        	+ SPARK_HOME/conf/spark-defaults.conf
        	+ Template file provided	
    - Setting Configuration Properties Programmatically
    	*Spark configuration settings are part of the Spark context
    	*Configure using a SparkConf object
    	*Some example set functions
    		+ setAppName(name)
    		+ setMaster(master)
    		+ set(property-name, value)
    	*set functions return a SparkConf object to support chaining
    - SparkConf Example (Scala)
    	
    	import org.apache.spark.SparkConf
    	
    		object WordCount {
    			def main(args: Array[String]) {
    				if(args.length < 1) {
    					System.err.println("Usage: WordCount <file>")
    					System.exit(1)
    				}
    			}
    			
    			val sconf = new SparkConf()
    				.setAppName("Word Count")
    				.set("spark.ui.port", "4141")
    			val sc = new SparkContext(sconf)
    			
    			val counts = sc.textFile(args(0)).
    				flatMap(line => line.split("\\W")).
    				map(word => (word)).
    				reduceByKey(_ + _)
    				
    			counts.take(5).foreach(println)
    			sc.stop()	
    		}	
    		
    		
     		
    		    				
    -You can view the Spark property settings in the Spark Application UI
    	*Environment tab	
    	
#Logging
	-Spark uses Apache Log4j for logging
		*Allows for controlling logging at runtime using a properties file
			+ Enable or disable logging set logging levels select output destination
		*For more info see http://logging.apache.org/log4j/1.2/
	-Log4j provides several logging levels
		*TRACE, DEBUG, INFO, WARN, ERROR, FATAL, OFF
	-Log file locations depend on your cluster management platform
	-YARN
		*If log aggregation is off, logs are stored locally on each worker node
		*If log aggregation is on, logs are stored in HDFS
			+ Default /var/log/hadoop-yarn
			+ Access with yarn logs command 
				$ yarn application -list
					........
				$ yarn logs -applicationId <appid>
	
	-Configuring Spark Logging
		*Logging levels can be set for the cluster, for individual applications, or even for specific components of subsystems.
		*Default for machine: SPARK_HOME/conf/log4j.properties
			+ Start by copying log4j.properties.template ==> Located in /usr/lib/spark/conf
			+ Copy log4j.properties.template to log4j.properties
				-May require administrator privileges
			+Modify the rootCategory or repl.Main settings
				-log4j.rootCategory=DEBUG, console   ==> Default for all Spark applications
				-log4j.logger.org.apache.spark.repl.Main=DEBUG  ==> Default override for Spark shell (Scala)			
		*Logging in the Spark shell can be configured interactively
			+ The setLogLevel method sets the logging level temporarily
				- sc.setLogLevel("ERROR")		
				
#EXAMPLES
	#FROM COMMAND LINE
		$ spark-submit --class stubs.CountJPGs --master yarn-client --name 'Count JPGs' target/countjpgs-1.0.jar /loudacre/weblogs/*
		
	#SETTINGS CONFIGURATION OPTIONS	IN A PROPERTIES FILE
		-Using a text editor, create a file in the current working directory called myspark.conf, containing settings for the propertiess shown below:
		
			spark.app.name				My Spark App
			spark.master				yarn-client
			spark.executor.memory		400M
			
		$ spark-submit --properties-file myspark.conf --class stubs.CountJPGs target/countjpgs-1.0.jar /loudacre/weblogs/*
		
#SETTING LOGGINGS LEVELS
	-Copy the template file
		/usr/lib/spark/conf/log4j.properties.template  
		     to   
		/usr/lib/spark/conf/log4j.properties	
		     You will need to use superuser privileges to this, so use the sudo command
       $ sudo cp /usr/lib/spark/conf/log4j.properties.template /usr/lib/spark/conf/log4j.properties
       
    -Load the new log4j.properties file into an editor. Again, you will need to use sudo:
    	$ sudo gedit /usr/lib/spark/conf/log4j.properties
    -The first line currently reads:
    	log4j.rootCategory=INFO, console
    -Replace INFO with DEBUG and save the file and rerun your Spark application
    	log4j.rootCategory=DEBUG, console		 
    -Edit the file again to replace DEBUG with WARN and try again.
    
    -You can also override the current setting temporarily by calling sc.setLogLevel with your preferred setting.
    	> sc.setLogLevel("ERROR")	   		     	 
			
	    		  	     