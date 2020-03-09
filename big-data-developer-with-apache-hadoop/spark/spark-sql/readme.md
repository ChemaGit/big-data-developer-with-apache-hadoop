# Structured data: SQL, Dataframes, and Datasets
````text
    With our newfound understanding of the cost of data movement in a Spark job, 
    and some experience optimizing jobs for data locality last week, we'll focus on how we can more easily achieve similar optimizations. 
    Can structured data help us? We'll look at Spark SQL and its powerful optimizer which uses structure to apply impressive optimizations. 
    We'll move on to cover DataFrames and Datasets, which give us a way to mix RDDs with the powerful automatic optimizations behind Spark SQL.
````   

# Dataframes and Apache Spark SQL
````text
	- What is Spark SQL?
		* Spark module for structured data processing
	- What does Spark SQL provide?
		* The DataFrame API-a library for working with data as tables
			Defines DataFrames containing rows and columns
		* Catalyst Optimizer-an extensible optimization framework
		* A SQL engine and command line interface
````

# SQLContext
````text
	- The main Spark SQL entry point is a SQL context object
		* Requires a SparkContext object
		* The SQL context in Spark SQL is similar to Spark context in core Spark
	- There are two implementations
		* SQLContext
			Basic implementation
		* HiveContext
			Reads and writes Hive/HCatalog tables directly
			Supports full HiveQL language
			Requires the Spark application be linked with Hive libraries
			Cloudera recommends using HiveContext
````

# Creating a SQL Context
````text
	- The Spark shell creates a HiveContext instance automatically
		* Call sqlContext
		* You will need to create one when writing a Spark application
		* Having multiple SQL context objects is allowed
	- A SQL context object is created based on the Spark context
````
````scala
		import org.apache.spark.sql.hive.HiveContext
		val sqlContext = new HiveContext(sc)
		import sqlContext.implicits._
````			



# DataFrames		
````text
	- DataFrames are the main abstraction in Spark SQL
		* Analogous to RDDs in core Spark
		* A distributed collection of structured data organized into named columns
		* Built on a base RDD containing Row objects
````

# Creating DataFrames
````text
	-DataFrames can be created
		* From an existing structured data source
			Such as a Hive table, Parquet file, or JSON file
		* From an existing RDD
		* By performing an operation or query on another DataFrame
		* By programmatically defining a schema
    - Creating a DataFrame from a Data Source
        - sqlContext.read returns a DataFrameReader object
        - DataFrameReader provides the functionality to load data into a DataFrame
        - Convenience functions
            json(filename)
            parquet(filename)
            orc(filename)
            table(hive-tablename)
            jdbc(url, table, options)
````            
````scala    
    // Example: Creating a DataFrame from a JSON File
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    val peopleDF = sqlContext.read.json("people.json")	

    // Example: Creating a DataFrame from a Hive/Impala Table  
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    val customerDF = sqlContext.read.table("customers")	
````      
````text  				
    - Loading from a Data Source Manually
        -You can specify settings for the DataFrameReader
            * format: Specify a data source type
            * option: a key/value setting for the underlying data source
            * schema: Specify a schema instead of inferring from the data source
	-Then call the generic base function load
````	
````scala	
    sqlContext.read.format("com.databricks.spark.avro").load("/loudacre/accounts_avro")		
    sqlContext.read.format("jdbc")
                   .option("url","jdbc:mysql://localhost/loudacre")
                   .option("dbtable", "accounts")
                   .option("user","training")
                   .option("password","training")
                   .load()	
````		               				
    - Data Sources		             
        - Spark SQL built-in data source types
            * table
            * json
            * parquet
            * jdbc
            * orc
        - You can also use third party data source libraries, such as
            * Avro
            * HBase
            * CSV
            * MySQL
````            
            
# DataFrame Basic Operations(1)
````text
	-Basic operations deal with DataFrame metadata(rather than its data)
	-Some examples
		* schema -> returns a schema object describing the data
		* printSchema -> diplays the schema as a visual tree
		* cache / persist -> persist the DataFrame to disk or memory
		* columns -> returns an array containing the names of the columns
		* dtypes -> returns an array of (column name, type) pairs
		* explain -> prints debug information about the DataFrame to the console
````		
````scala		
	// Example: Displaying column data types using dtypes
    val peopleDF = sqlContext.read.json("people.json")
	peopleDF.dtypes.foreach(println)
      (age, LongType)
      (name, StringType)
      (pcode, StringType)	
````      	
		  			
# Working with Data in a DataFrame
	-Queries-create a new DataFrame
		*DataFrames are immutable
		*Queries are analogous to RDD transformations
	-Actions-return data to the driver
		*Actions trigger "lazy" execution of queries
# DataFrame Actions		
	-Some DataFrame actions
		*collect returns all rows as an array of Row objects
		*take(n) returns the first n rows as an array of Row objects
		*count returns the number of rows
		*show(n) displays the first n rows(default = 20)
# DataFrame Queries(1)
	-DataFrame query methods return new DataFrames
		*Queries can be chained like transformations
	-Some query methods
		*distinct returns a new DataFrame with distinct elements of this DF
		*join joins this DataFrame with a second DataFrame
			Variants for inside, outside, left, and right joins					
		*limit returns a new DataFrame with the first n rows of this DF
		*select returns a new DataFrame with data from one or more columns of the base DataFrame
		*where returns a new DataFrame with rows meeting specified query criteria(alias for filter)			
# DataFrame Queries(2)
	-Examples
		> peopleDF.limit(3).show()
		> peopleDF.select("age")
		> peopleDF.select("name", "age")	
		> peopleDF.where("age > 21")	    	
# Querying DataFrames using Columns(1)
	-Some DataFrame queries take one or more columns or column expressions
		*Required for more sophisticated operations
	-Some examples
		*select
		*sort
		*join
		*where
# Querying DataFrames using Columns(2)
	-Columns can be referenced in multiple ways
		*Python
			> ageDF = peopleDF.select(peopleDF['age'])
			> ageDF = peopleDF.select(peopleDF.age)
		*Scala
			> val ageDF = peopleDF.select(peopleDF("age"))	
			> val ageDF = peopleDF.select($"age")			
# Querying DataFrames using Columns(3)
	-Column references can also be column expressions
		*Python
			> peopleDF.select(peopleDF['name'], peopleDF['age'] + 10)
		*Scala
			> peopleDF.select(peopleDF("name"), peopleDF("age") + 10)		
# Querying DataFrames using Columns(4)
	-Example: Sorting by columns(descending)
		*Python
			> peopleDF.sort(peopleDF['age'].desc())  //.asc and .desc are column expression methods used with sort
		*Scala
			> peopleDF.sort(peopleDF("age").desc)			
# Joining DataFrames(1)						
	-A basic inner join when join column is in both DataFrames
		> peopleDF.join(pcodesDF, "pcode")
	-Specify type of join as inner(default), outer, left_outer, right_outer, or leftsemi
		Python >> peopleDF.join(pcodesDF, "pcode", "left_outer")
		Scala >> peopleDF.join(pcodesDF, Array("pcode"), "left_outer")
	-Use a column expression when column names are different
		Python >> peopleDF.join(zcodesDF, peopleDF.pcode == zcodesDF.zip)
		Scala >> peopleDF.join(zcodesDF, $"pcode" === $"zip")		
# SQL Queries
	-When using HiveContext, you can query Hive/Impala tables using HiveQL
		*Returns a DataFrame
			> sqlContext.sql("""Select * From customers Where name Like "A%" """)
	-You can also perform some SQL queries with a DataFrame
		*First, register the DataFrame as a "table" with the SQL context
			> peopleDF.registerTempTable("people")
			> sqlContext.sql("""Select * From customers Where name Like "A%" """)
	-You can query directly from Parquet or JSON files without needing to create a DataFrame or register a temporary table
		> sqlContext.sql("""Select * From json.'/user/training/people.json' Where name Like "A%" """)
# Other Query Functions
	-DataFrames provide many other data manipulation and query functions such as
		*Aggregation such as groupBy, orderBy, and agg
		*Multi-dataset operations such as join, unionAll, and intersect
		*Statistics such as avg, sampleBy, corr, and cov
		*Multi-variable functions rollup and cube
		*Window-based analysis functions
		
# Saving DataFrames
	-Data in DataFrames can be saved to a data source
	-Use DataFrame.write to create a DataFrameWriter
	-DataFrameWriter provides convenience functions to externally save the data represented by a DataFrame
		*jdbc inserts into a new or existing table in a database
		*json saves as JSON file
		*parquet saves as a Parquet file
		*orc saves as an ORC file
		*text saves as a text file (string data in a single column only)
		*saveAsTable saves as a Hive/Impala table(HiveContext only)
			> peopleDF.write.saveAsTable("people")
# Options for Saving DataFrames
	-DataFrameWriter option methods
		*format -> specifies a data source type
		*mode -> determines the behavior if file or table already exists: overwrite, append, ignore or error(default is error)
		*partitionBy -> stores data in partitioned directories in the form column=value(as with Hive/Impala partitioning)	
		*options -> specifies properties for the target data source
		*save -> is the generic base function to write the data
			> peopleDF.write.format("parquet").mode("append").partitionBy("age").saveAsTable("people")
# DataFrames and RDDs
	-DataFrames are built on RDDs
		*Base RDDs contain Row objects
		*Use rdd to get the underlying RDD
			> peopleRDD = peopleDF.rdd
	-Row RDDs have all the standard Spark actions and transformations
		*Actions: collect, take, count, and so on
		*Transformations: map, flatMap, filter, and so on
	-Row RDDs can be transformed into pair RDDs to use map-reduce methods
	-DataFrames also provide convenience methods(such as map, flatMap, and foreach) for converting to RDDs
# Working with Row Objects
	-The sintax for extracting data from Row objects depends on language
	-Python
		*Column names are object attributes
			row.age -> returns age column value from row
	-Scala
		*Use Array-like syntax to return values with type Any
			row(n) -> returns element in the nth column
			row.fieldIndex("age") -> returns index of the age column
		*Use methods to get correctly typed values
			row.getAs[Long]("age")		
		*Use type-specific get methods to return typed values
			row.getString(n) returns nth column as a string
			row.getInt(n) returns nth column as an integer
			And so on
# Example: Extracting Data from Row Objects
	-Extract data from Row objects
		*Python
			> peopleRDD = peopleDF.map(lambda row: (row.pcode, row.name))
			> peopleByPCode = peopleRDD.groupByKey()
		*Scala
			> val peopleRDD = peopleDF.map(row => (row(row.fieldIndex("pcode")),row(row.fieldIndex("name"))))
			> val peopleByPCode = peopleRDD.groupByKey()
# Converting RDDs to DataFrames
	-You can also create a DF from an RDD using createDataFrame
		*Python
			from pyspark.sql.types import *
			schema = StructType([StructField("age",IntegerType(),True),
									StructField("name",StringType(),True),
									StructField("pcode",StringType(),True)])
			myrdd = sc.parallelize([(40,"Abram","01601"),(16,"Lucia","87501")])
			mydf = sqlContext.createDataFrame(myrdd,schema)
		*Scala
			import org.apache.spark.sql.types._
			import org.apache.spark.sql.Row
			val schema = StructType(Array(
							StructField("age", IntegerType, true),
							StructField("name", StringType, true),
							StructField("pcode", StringType, true)))
			val rowrdd = sc.parallelize(Array(Row(40,"Abram","01601"),Row(16,"Lucia","87501")))
			val mydf = sqlContext.createDataFrame(rowrdd,schema)
# Comparing Impala to Spark SQL
	-Spark SQL is built on Spark, a general purpose processing engine
		*Provides convenient SQL-like access to structured data in a Spark application
	-Impala is a specialized SQL engine
		*Much better performance for querying
		*Much more mature than SparkSQL
		*Robust security using Sentry
	-Impala is better for
		*Interactive queries
		*Data analysis
	-Use SparkSQL for						
		*ETL
		*Access to structured data required by a Spark application
# Comparing SparkSQL with Hive on Spark
	-SparkSQL
		*Provides the DataFrame API allow structured data processing in a Spark application
		*Programmers can mix SQL with procedural processing
	-Hive on Spark
		*Hive provides a SQL abstraction layer over MapReduce or Spark
			*Allows non-programmers to analyze data using familiar SQL
		*Hive on Spark replaces MapReduce as the engine underlying Hive
			*Does not affect the user experience of Hive
			*Except queries run many times faster!					
# Spark 2.x
	-Spark 2.0 is the next major release of Spark
	-Several significant changes related to SparkSQL, including
		*SparkSession replaces SQLContext and HiveContext
		*Support for ANSI-SQL as well as HiveQL
		*Support for subqueries
		*Support for Datasets
# Spark Datasets
	-Datasets are an alternative to RDDs for structured data
		*A strongly-typed collection of objects, mapped to a relational schema
		*Unified with the DataFrame API-DFs are Datasets of Row objects
		*Use the Spark Catalyst optimizer as DFs do for better performance
		*Word count using RDDs: Scala
			val countsRDD = sc.textFile(filename)
			                  .flatMap(line => line.split(" "))
			                  .map(word => (word, 1))
			                  .reduceByKey((v1, v2) => v1 + v2)
	 	*Word count using Datasets: Scala
	 		val countDS = sqlContext.read.text(filename).as[String]
	 		                        .flatMap(line => line.split(" "))
	 		                        .groupBy(word => word)
	 		                        .count()		                  								
																											




											