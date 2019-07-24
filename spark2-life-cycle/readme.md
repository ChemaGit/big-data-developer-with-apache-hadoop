# Development life cycle of Spark 2 applications using Scala (using IntelliJ)
	- As part of this session we will see end to end development life cycle to build Spark 2 applications using Scala as programming languages. 
	- We will be using IntelliJ IDE to build application.
    		- Define Problem Statement
    		- Create Project (using IntelliJ)
    		- Define necessary dependencies in build.sbt
    		- Externalize Properties
    		- Develop Application (using Spark Data Frames)
    		- Validate locally
    		- Build Jar file and get it ready to run on the EMR cluster

# Problem Statement

	- Let us define problem statement and see the steps to develop the application.
    		- Get Daily Product Revenue (for completed and closed orders)
    		- We need orders and order_items to come up with the solution
    		- Final output need to be sorted in ascending order by date and descending order by revenue
    		- Save order_date, product_id and revenue in the form of JSON

# Create Project

	- Let us see how to create new project appropriately.
    		- Click on New Project
    		- Make sure to choose Scala and SBT
    		- Make sure JDK
    		- Make sure you choose Scala 2.11.x
    		- Spark 2.3.0 is compatible with 2.11 not 2.12

# Dependencies

	- We need couple of dependencies to develop the solution to be defined in build.sbt.
    		- spark core – libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.0"
    		- spark hive or spark sql – libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.3.0"

# Externalize Properties

	- Let us see the relevance of externalizing properties.
    		- We need to control the way application run in different environments such as dev, test, prod etc
    		- We need to define input path and output path
    		- These things can be either passed as arguments or externalize properties
    		- In typical applications we will have tens of properties and passing all those arguments can be unmanageable
    		- It is better to externalize properties
    		- There is package called com.typesafe config which provide necessary APIs to load properties file and give us properties as map like object.
    		- To use this we need to add dependency – libraryDependencies += "com.typesafe" % "config" % "1.3.2"
````properties
name := "bdclouddemo"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.0"
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.3.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.2"
````


    	- Create properties file with dev and prod categories (under src/main/resources)

````properties
dev.execution.mode = local
dev.input.base.dir = /Users/itversity/Research/data/retail_db_json
dev.output.base.dir = /Users/itversity/Research/data/bdclouddemo/scala

prod.execution.mode = yarn-client
prod.input.base.dir = s3://itversitydata/retail_db_json
prod.output.base.dir = s3://itversitydata/bdclouddemo/scala
````

# Develop Application

	- Let us develop the application to get daily product revenue for completed and closed orders.
    		- Write code to load application.properties
    		- Create SparkSession object
    		- Read data from JSON files and create data frames out of them
    		- Use APIs such as select, where, groupBy, agg with sum, orderBy etc to process data
    		- Save final output in the form of JSON

````scala
package retail_db

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by itversity on 10/08/18.
  */
object GetDailyProductRevenue {
  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load()
    val envProps = props.getConfig(args(0))
    val spark = SparkSession.
      builder.
      appName("Daily Product Revenue").
      master(envProps.getString("execution.mode")).
      getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "2")

    import spark.implicits._

    val inputBaseDir = envProps.getString("input.base.dir")
    val orders = spark.read.json(inputBaseDir + "/orders")
    val orderItems = spark.read.json(inputBaseDir + "/order_items")

    val dailyProductRevenue = orders.where("order_status in ('CLOSED', 'COMPLETE')").
      join(orderItems, $"order_id" === $"order_item_order_id").
      groupBy("order_date", "order_item_product_id").
      agg(sum($"order_item_subtotal").alias("revenue")).
      orderBy($"order_date", $"revenue" desc)

    val outputBaseDir = envProps.getString("output.base.dir")
    dailyProductRevenue.write.json(outputBaseDir + "/daily_product_revenue")
  }

}
````

# Validate Locally

	- Make sure you run the job locally.
    		- Right click and run the application. It will fail for first time, but will create application in run wizard
    		- Go to edit configurations in run wizard and pass dev as first argument
    		- Now right click again and run
    		- If it return with exit code 0, the run is successful
    		- Now go to the output directory to validate that data is generated properly.

# Build jar file

	- As development and validation using IDE is done, now it is time to build jar file so that we can run on actual clusters.
    		- Right click on the project and copy path
    		- Go to terminal and paste the path
    		- Build jar file using sbt run
    		- We can copy the jar file to the cluster and run where ever you want





