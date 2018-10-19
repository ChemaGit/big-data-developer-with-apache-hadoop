Structured data: SQL, Dataframes, and Datasets
With our newfound understanding of the cost of data movement in a Spark job, and some experience optimizing jobs for data locality last week, this week we'll focus on how we can more easily achieve similar optimizations. Can structured data help us? We'll look at Spark SQL and its powerful optimizer which uses structure to apply impressive optimizations. We'll move on to cover DataFrames and Datasets, which give us a way to mix RDDs with the powerful automatic optimizations behind Spark SQL.

#Dataframes and Apache Spark SQL
	-What is Spark SQL?
		*Spark module for structured data processing
	-What does Spark SQL provide?
		*The DataFrame API-a library for working with data as tables
			Defines DataFrames containing rows and columns
		*Catalyst Optimizer-an extensible optimization framework
		*A SQL engine and command line interface
#SQLContext
	-The main Spark SQL entry point is a SQL context object
		*Requires a SparkContext object
		*The SQL context in Spark SQL is similar to Spark context in core Spark
	-There are two implementations
		*SQLContext
			Basic implementation
		*HiveContext
			Reads and writes Hive/HCatalog tables directly
			Supports full HiveQL language
			Requires the Spark application be linked with Hive libraries
			Cloudera recommends using HiveContext
#Creating a SQL Context
	-The Spark shell creates a HiveContext instance automatically
		*Call sqlContext
		*You will need to create one when writing a Spark application
		*Having multiple SQL context objects is allowed
	-A SQL context object is created based on the Spark context
		import org.apache.spark.sql.hive.HiveContext
		val sqlContext = new HiveContext(sc)
		import sqlContext.implicits._
		
#DataFrames
	-DataFrames are the main abstraction in Spark SQL
		*Analogous to RDDs in core Spark
		*A distributed collection of structured data organized into named columns
		*Built on a base RDD containing Row objects
#Creating DataFrames
	-DataFrames can be created
		*From an existing structured data source
			Such as a Hive table, Parquet file, or JSON file
		*From an existing RDD
		*By performing an operation or query on another DataFrame
		*By programmatically defining a schema
#Creating a DataFrame from a Data Source
	-sqlContext.read returns a DataFrameReader object
	-DataFrameReader provides the functionality to load data into a DataFrame
	-Convenience functions
		json(filename)
		parquet(filename)
		orc(filename)
		table(hive-tablename)
		jdbc(url, table, options)
#Example: Creating a DataFrame from a JSON File
	val sqlContext = new HiveContext(sc)
	import sqlContext.implicits._
	val peopleDF = sqlContext.read.json("people.json")	
#Example: Creating a DataFrame from a Hive/Impala Table
	val sqlContext = new HiveContext(sc)
	import sqlContext.implicits._
	val customerDF = sqlContext.read.table("customers")					
#Loading from a Data Source Manually
	-You can specify settings for the DataFrameReader
		*format: Specify a data source type
		*option: a key/value setting for the underlying data source
		*schema: Specify a schema instead of inferring from the data source
	-Then call the generic base function load
		sqlContext.read.format("com.databricks.spark.avro").load("/loudacre/accounts_avro")
		
		sqlContext.read.format("jdbc")
		               .option("url","jdbc:mysql://localhost/loudacre")
		               .option("dbtable", "accounts")
		               .option("user","training")
		               .option("password","training")
		               .load()					
#Data Sources		               										