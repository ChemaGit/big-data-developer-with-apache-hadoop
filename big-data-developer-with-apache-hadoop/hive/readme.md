#WORKING WITH TABLES IN APACHE HIVE

#Hive Overview
	-Apache Hive is a high-level abstraction on top of MapReduce
		*Interprets a language called HiveQL, based on SQL-92
		*Generates MapReduce jobs that run on the Hadoop cluster
		*Hive does not turn your cluster into a database server!
	-Easier and faster than writing your own Java MapReduce jobs
		*But amount of execution time will be about the same
	-HiveQL Query
		SELECT id, fname, lname, city, unpaid
		FROM accounts
		JOIN billing
		ON accounts.acct_num = billing.acct_num
		WHERE state = 'CA'
		AND unpaid > 100
		ORDER BY unpaid DESC;
#How Data is Stored in Hive
	-Hive's queries operate on tables, as with a relational database
		*But Hive tables are just a facade for a directory of data in HDFS
		*Default file format is delimited text, but Hive supports many others
	-How does Hive know the structure and location of tables?
		*These are specified when tables are created
		*This metadata is stored in an RDBMS such as MySQL
		*Kite datasets created with a repo:hive URI are already known to Hive
#Basic Architecture
	-A system called HiveServer 2 is the container for Hive's execution engine
		*Available in Hive 0.11(CDH 4.1) and later
	-Accessible via Beeline(command shell), JDBC, ODBC, or Hue(Web UI)
	
#ACCESSING HIVE

#Accessing Hive Using the Beeline Shell
	-You can execute HiveQL with Beeline, a replacement for the old Hive shell
		*Start Beeline by specifying login credentials and a JDBC URL
		*Connection details vary based on cluster settings(ask your sysadmin)
			$ beeline -n alice -p swordfish -u jdbc:hive2://dev.loudacre.com:10000/default	
			$ beeline -n training -p training -u jdbc:hive2://localhost:10000/hadoopexam
			$ beeline -n training -p training -u jdbc:hive2://localhost:10000/default
	-You will then see some starup messages followed by a prompt
		*Use this prompt to enter HiveQL statements(terminated by semicolons)
		*Type !quit or press Ctrl+d to exit Beeline and return to the shell
#Executing HiveQL Statements in Batch Mode
	-You can specify a single HiveQL statement from the command line using the -e option for Beeline
		$ beeline -n training -p training -u jdbc:hive2://dev.loudacre.com:10000/default -e 'SELECT * FROM employees WHERE salary > 50000'	
	-Alternatively, save HiveQL statements to a text file and use the -f option
		*Especially convenient for automation
			$ beeline -n training -p training -u jdbc:hive2://loudacre.com:10000/default -f myqueries.hql
#Accessing Hive with Hue
	-To use Hue, browse to http://hue_server:88888/
	-Hue provides a Web-based interface to Hive
		*Launch by clicking on Query Editors -> Hive
	-This interface supports
		*Creating tables
		*Running queries
		*Browsing tables
		*Saving queries for later execution
		*Save the query results: Download in XLS Format, Download in CSV Format, Save to HDFS or as a new Hive Table, View results full screen
		
#WORKING WITH TABLES IN APACHE HIVE

#Basic Query Syntax

#Exploring Hive Tables(1)
	-The SHOW TABLES command lists all tables in the current Hive database
	-The DESCRIBE command lists the fields in the specified table
		> DESCRIBE vendors;
#Basic HiveQL Syntax
	-Hive keyword are not case-sensitive, but often capitalized by convention
	-Statements may span lines and are terminated by a semicolon
	-Comments begin with -- (double hyphen)
		*Supported in Hive scripts and Hue, but not in Beeline
			$ cat nearby_customers.hql
			
			SELECT acct_num, first_name, last_name
				FROM accounts
				WHERE zipcode = '94306'; -- Loudacre headquarters
				
#Selecting Data from a Hive Table
	-The SELECT statement retrieves data from Hive tables
		*Can specify an ordered list of individual columns
			> SELECT first_name, last_name, city FROM accounts;
			> SELECT * FROM accounts;
			> SELECT acct_num AS id, total * 0.1 AS commission FROM sales;
#Limiting and Sorting Query Results
	> SELECT acct_num, city FROM accounts LIMIT 10;	
	> SELECT acct_num, city FROM customers ORDER BY city DESC LIMIT 10;			
#Using a WHERE Clause to Restrict Results
	> SELECT * FROM accounts WHERE acct_num = 1287;
	> SELECT * FROM accounts WHERE first_name = 'Anne'
	> SELECT * FROM accounts WHERE first_name LIKE 'Ann%' AND (city = 'Seattle' OR city = 'Portland');
#JOINS in Hive
	-Joining disparate data sets is a common use of Hive
		*Caution: note the JOIN .. ON syntax required by Hive
		*For best performance, list the largest table last in your query
			> SELECT emp_name, dept_name FROM employees JOIN departments ON (employees.dept_id = departments.id);
#Hive Functions
	-Hive offers dozens of built-in functions to use in queries
		*Many are identical to those found in SQL
		*Others are Hive-specific
	-Example function invocation
		*Function names are not case-sensitive
			> SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM accounts;
#Getting Help with Functions
	-The SHOW FUNCTIONS command lists all available function
	-Use DESCRIBE FUNCTION to learn about a specific function
		> DESCRIBE FUNCTION UPPER;
			UPPER(str) - Returns str with all characters changed to uppercase
	-DESCRIBE FUNCTION EXTENDED shows additional information
		> DESCRIBE FUNCTION EXTENDED UPPER;
			UPPER(str) - Returns str with all characters changed to uppercase
			Synonyms: upper, ucase
			Example:
				> SELECT UPPER('Facebook') FROM src LIMIT 1;
				'FACEBOOK'
#Common Built-In Functions
	-These built-in functions operate on one value at a time
		*Function Description					Example Invocation				Input			Output
		*Rounds to specified places			ROUND(total_price,2)				23.492			23.49
		*Returns nearest integer				CEIL(total_price)					23.492			24
		above the supplied value
		*Returns nearest integer				FLOOR(total_price)				23.492			23
		below the supplied value
		*Extracts the year from the			YEAR(order_dt)						2015-06-14		2015
		supplied timestamp														16:51:05
		*Extract portion of string			SUBSTRING(name,0,3)				Benjamin		Ben
		*Converts timestamp from 			TO_UTC_TIMESTAMP(ts, 'PST')		2015-08-27
		specified timezone to UTC												15:03:14
		*Converts to another type			CAST(weight as INT)				3.581			3
		
#Record Grouping for Use with Aggregate Functions
	-GROUP BY groups selected data by one or more columns
		*Columns not part of the aggregation must be listed in GROUP BY
			SELECT region, state, COUNT(id) AS num
			FROM vendors
			GROUP BY region, state;
#Built-In Aggregate Functions
	-Hive has many built-in aggregate functions, including:
		*Function Description								Example Invocation
		*Count all rows									COUNT(*)
		*Count number of non-null values				COUNT(first_name)
		for a given field														
		*Count number of unique,							COUNT(DISTINCT fname)
		non-null values for field
		*Returns the largest value						MAX(salary)
		*Returns the smallest value						MIN(salary)
		*Returns total of selected values				SUM(price)
		*Returns the average of all supplied			AVG(salary)
		values								 														
		
#Working with Tables in Apache Hive

#Creating and Populating Hive Tables

#Hive Data Types
	-Each column in a Hive table is assigned a specific data type
		*These are specified when the table is created
		*Hive returns NULL values for non-conforming data in HDFS
	-Here are some common data types in Hive
		*Hive also supports a few complex types such as maps and arrays
		Name				Descripción					           Example Value		
		STRING            Character data (of any length)	       Alice
		BOOLEAN           True or False	                         TRUE
		TIMESTAMP         Instant in	Gme	                         2015-09-14 17:01:29
		INT               Range: same as Java int                84127213
		BIGINT            Range: same as Java long               7613292936514215317
		FLOAT             Range: same as Java float              3.14159
       DOUBLE            Range:	same as Java double             3.1415926535897932385
#Creating a Table in Hive
	-The following example creates a new table named products
		*Data stored in text file format with four delimited fields per record
		CREATE TABLE products (
			id	INT,
			name STRING,
			price INT,
			data_added TIMESTAMP
		);       
					
		*Default field delimiter is \001 (Ctrl-A) and line ending is \n (newline)
		*Example of corresponding records for the table above
		   1^AUSB Cable^A799^A2015-08-11 17:23:19\n          
          2^AScreen Cover^A3499^A2015-08-12 08:41:26\n 
#Changing the Default Field Delimiter When Creating a Table
	-If you have tab-delimited data, you would create the table like this
		*Data stored as text with four tab-delimited fields per record
		CREATE TABLE products (
			id	INT,
			name STRING,
			price INT,
			data_added TIMESTAMP
		)
		ROW FORMAT DELIMITED
		FIELDS TERMINATED BY '\t';
		*Example of corresponding records for the table above
			1\tUSB Cable\t799\t2015-08-11 17:23:19\n
       		2\tScreen Cover\t3499\t2015-08-12 08:41:26\n
#Creating a Table with Sequence File Format
	-Creating tables that will be populated with SequencesFiles is also easy
		CREATE TABLE products (
			id	INT,
			name STRING,
			price INT,
			data_added TIMESTAMP
		)
		STORED AS SEQUENCEFILE;
	-STORED AS TEXTFILE is the default and is rarely specified explicitly
#Creating a Table with Avro File Format			       		
	-Avro-based tables must specify a few more details
		*Of these, only the table name and schema URL typically change
		*Field names and types are defined in the schema
		CREATE TABLE products
		ROW FORMAT SERDE
			'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
		STORED AS 
		INPUTFORMAT
			'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
		OUTPUTFORMAT
			'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
		TBLPROPERTIES
			('avro.schema.url'='hdfs://dev.loudacre.com:8020/schemas/products.avsc');
#Removing a Table
	-Use DROP TABLE to remove a table from Hive
		> DROP TABLE products;
	-Add IF EXISTS to avoid error if table does not already exist
		*Useful for scripting setup tasks
			> DROP TABLE IF EXISTS products;
	-Caution: dropping a table is a destructive operation
		*This will remove metadata and may remove data from HDFS
		*Hive does not have a rollback or undo feature
#Specifying Table Data Location
	-By default, table data is stored beneath Hive's "warehouse" directory
		*Path will be /user/hive/warehouse/<tablename>
	-Storing data below Hive's warehouse directory is not always ideal
		*Data might already exist in a different location
	-Use LOCATION clause during creation to specify alternate directory
		> CREATE TABLE products (
			id	INT,
			name STRING,
			price INT,
			data_added TIMESTAMP		
		  )															             
		  LOCATION '/loudacre/products';
#Self-Managed(External) Tables
	-Related to this is Hive's management of the data
		*Dropping a table removes data in HDFS
	-Use EXTERNAL when creating the table to avoid this behavior
		*Dropping this table affects only metadata
		*This is a better choice if you intend use this data outside of Hive
		*Almost always used in conjunction with the LOCATION keyword	
			> CREATE EXTERNAL TABLE products (
				id	INT,
				name STRING,
				price INT,
				data_added TIMESTAMP		
			  )															             
			  LOCATION '/loudacre/products';	
#Loading Data Into Hive Tables
	-Remember, each Hive table is mapped to a directory in HDFS
		*Can populate a table by adding one or more files to this directory
		*Place file(s) directly in the table's directory(subdirectories not allowed)
			$ hdfs dfs -mv newaccounts.csv /user/hive/warehouse/accounts/
	-Hive also provides a LOAD DATA command
		*Equivalent to the above, but does not require you to specify table path
			> LOAD DATA INPATH 'newaccounts.csv' INTO TABLE accounts;
	-Unlike an RDBMS, Hive does not validate data on insert
		*Missing or invalid data will be represented as NULL in query results
#Appending Selected Records to a Table
	-Another way to populate a table is througs a query
		*Use INSERT INTO to append results to an existing Hive table
			> INSERT INTO TABLE accounts_copy
				SELECT * FROM accounts;
		*Specify a WHERE clause to control which records are appended
			> INSERT INTO TABLE loyal_customers
				SELECT * FROM accounts
				WHERE YEAR(acct_create_dt) = 2008
					AND acct_close_dt IS NULL;
#Creating and Populating a Tables using CTAS
	-You can also create and populate a table with a single statement
		*This technique is known as "Create Table As Select" (CTAS)
		*Column names and types are derived from the source table
			> CREATE TABLE loyal_customers AS
				SELECT * FROM accounts
				WHERE YEAR(acc_create_dt) = 2008
					AND acct_close_dt IS NULL;
		*New table uses Hive's default format, but you can override this
			> CREATE TABLE california_customers
			  STORED AS SEQUENCEFILE
			  AS
			  	SELECT * FROM accounts
			  	WHERE state = 'CA';
			  	
#HOW HIVE READS DATA

#Recap: Reading Data in MapReduce
	-Recall: a MapReduce job's InputFormat class controls how records are read from files
		*TextInputFormat (default) supplies each line of text as the value
		*It is your responsibility to parse content into specific fields
#How Hive Reads Records from files
	-Hive queries are executed as MapReduce jobs
		*Records are read using the InputFormat associated with the table
		*Similarly, records are written using the table's OutputFormat
		*These formats are specified or implied during table creation
	-As with MapReduce, TextInputFormat is used by default
		*Each line from the input file(s) is considered a record
			CREATE TABLE sales(
				id INT,
				salesperson STRING,
				price INT		
			)
			ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
#Specifying the Input and Output Format in Hive
	-The file type implies which input and output formats are used for the table
		CREATE TABLE people (
			id INT,
			name STRING
		)
		STORED AS SEQUENCEFILE;
	-This is an alternative to specifying input and output formats explicitly	
		CREATE TABLE people (
			id INT,
			name STRING
		)
		STORED AS
			INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat'
			OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'
#How Hive Extracts Fields from Records
	-You have learned how Hive reads records from a file
		*How does Hive retrieve fields from those records?
	-Hive uses a class called a SerDe to extracts fields from each record
		*SerDe is short for "Serializer/Deserializer"
	-The default SerDe(LazySimpleSerDe) parses fields based on delimiter
		*Used if ROW FORMAT DELIMITED specified in CREATE TABLE
		*Also used if ROW FORMAT is missing from CREATE TABLE
			CREATE TABLE sales(
				id INT,
				salesperson STRING,
				price INT		
			)
			ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
#Common SerDe Implementations
	-Hive in CDH ships with many SerDes, including:
		Class Name									Reads and Writes Records
		LaxySimpleSerDe							Using specified field delimiters(default)
		AvroSerDe									Using Avro, according to specified schema
		ColumnarSerDe								Using the RCFile columnar format
		ParquetHiveSerDe							Using the Parquet columnar format
		RegexSerDe									Using the supplied regular expression
		
		*See Hive documentation for fully-qualified class names
	-Not all SerDes are compatible with all file formats
		*Cannot use AvroSerDe with SequenceFiles, for example
		
#Specifying the SerDe Type
	-A SerDe can be specified explicitly in the CREATE TABLE statement
		CREATE TABLE products
		ROW FORMAT SERDE
			'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
		STORED AS
		INPUTFORMAT
			'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
		OUTPUTFORMAT
			'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
		TBLPROPERTIES
			('avro.schema.url'='hdfs://dev.loudacre.com:8020/schemas/products.avsc');
															
#USING THE REGEXSERDE IN HIVE
#About the RegexSerDe
	-Hadoop is a great fit for semi-structured data
		*Many formats typically lack a consistent delimiter character
			08/14/2015@15:25:47 415-555-2854:312-555-7819 "Call placed"
			08/14/2015@15:25:53 415-555-2854:312-555-7819 "Call dropped"
					
	-RegexSerDe can help you to parse such data
		*Regular expressions provide great flexibility
		*Example: using RegexSerDe allows you to perform queries on log files
	-Points to note about RegexSerDe
		*Only supports STRING column type in Hive 0.10 and earlier
		*Backslashes in regular expressions must be escaped with a backslash
		
#Using the RegexSerDe
	-Input
		08/14/2015@15:25:47 415-555-2854:312-555-7819 "Call placed"
		08/14/2015@15:25:53 415-555-2854:312-555-7819 "Call dropped"
	-SerDe	
		CREATE TABLE calls (
			event_date STRING,
			event_time STRING,
			from_number STRING,
			to_number STRING,
			event_desc STRING)
		ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
		WITH SERDEPROPERTIES ("input.regex" =
			"(.*?)@(.*?) (.*?):(.*?) \"([^\"]*)\"");
	-Table		
		event_date	 	event_time	 	from_number	 	to_number	 	event_desc	
		08/14/2015 	15:25:47 		415-555-2854 		312-555-7819  Call placed
		08/14/2015    15:25:53 		415-555-2854 		312-555-7819  Call dropped
		 
#Supporting Fixed-Width Formats with RegexSerDe
	-Hive does not have a specific SerDe to support fixed-width formats
		*Yet these are commonly produced by older applications
		*Fixed-width data is easy to read with RegexSerDe
			Raw Data
				2015-08-1212345673661845I love the guitar solo
				2015-08-1216431444025652Hard to dance to this one
				2015-08-1412957863971103It needs more cowbell
			SerDe(Excerpt)
				CREATE TABLE ... WITH SERDEPROPERTIES ("input.regex" =
					"(.{10})(\\d{6})(\\d{7})(\\d)(.*)";
			Resulting Table
				comment_date	 acct_num	 prod_id	 rating	 comments	
				2015-08-12 	123456 		7366184 		5 		I love the guitar solo
				2015-08-12 	164314 		4402565 		2 		Hard to dance to this one
				2015-08-14 	129578 		6397110 		3 		It needs more cowbell
				
#DEVELOPING USER-DEFINED FUNCTIONS

#Why Create User-Defined Functions?
	-User-defined functions allow you to extend the capabilities of Hive
		*You might need to reformat values in a query
		*You might need to add new mathematical functions
		*You might need to include support for custom processing
#Types of User-Defined Functions
	-There are three distinct types of user-defined functions
	-UDF(User-Defined Function)
		*Operates on exactly one record and returns a single value
		*Examples include SUBSTRING and ROUND
	-UDAF(User-Defined Aggregate Function)
		*Operates on multiple records and returns a single value
		*Usually used in conjunction with GROUP BY clause
		*Examples include SUM and AVG
	-UDTF(User-Defined Table Function)
		*Operates on a single record but returns multiple records
		*The EXPLODE function is an example of a UDTF
	-UDFs are the most common
		*We will focus on them in this chapter

#IMPLEMENTING A USER-DEFINED FUNCTION
#Maven Configuration for Custom UDF Development
	-To develop a custom UDF, first add the following to you pom.xml
		<dependency>
			<artifactId>hive-exec</artifactId>
			<groupId>org.apache.hive</groupId>
			<version>${hive.version}</version>
		</dependency>
	-Value of hive.version varies based on your CDH release
		*Use the Maven repository Web UI to search for available versions
		*https://repository.cloudera.com/
#Developing and Using a Custom UDF
	-The easiest way to develop a UDF is to extend the UDF class
		*Implementation differs for UDAFs and UDTFs
	-Your UDF must implement one or more methods named evaluate
		*Each method can accept a different type of argument
		*Methods should accept and return Writable types
	-The UDF should also use a Description annotation
		*Provides information used in SHOW FUNCTION command	
#Simple UDF Example
	-The simplest UDF implementation just returns a constant value
	
		package com.loudacre.example;
		
		import org.apache.hadoop.hive.ql.exec.Description;
		import org.apache.hadoop.hive.ql.exec.UDF;
		import org.apache.hadoop.io.DoubleWritable;
		
		@Description (
			name="KPM",
			value="_FUNC_() - returns the number of kilometers per mile",
				extended = "Example:\n"
				+ " > SELECT _FUNC_() * distance FROM locations LIMIT 1;\n"
				+ " 482.802")
		public class KilometersPerMile extends UDF {
			private final DoubleWritable result = new DoubleWritable(1.60934);
			public DoubleWritable evaluate() {
				return result;
			}
		}
		
#Price Formatting UDF Example(1)
	-This example can accept either one or two arguments
		*Example: FORMAT_CENTS(375) returns $3.75
		*Example: FORMAT_CENTS(375, '€') returns €3.75
	-The class defines two Text objects as member variables
		*It is more efficient to reuse objects than create new instances
	-This evaluate method invokes the two-argument version
		*By supplying the default symbol			
		
		package com.loudacre.example;
		
		import org.apache.hadoop.hive.ql.exec.Description;
		import org.apache.hadoop.hive.ql.exec.UDF;
		import org.apache.hadoop.io.IntWritable;
		import org.apache.hadoop.io.Text;

		@Description(
			name = "FORMAT_CENTS",
			value = "_FUNC_(cents) - Formats INT value as dollars and cents.",
			extended = "Example:\n"
				+ " > SELECT _FUNC_(price_in_cents) FROM sales;\n"
				+ " > SELECT _FUNC_(price_in_eurocents, '€') FROM sales;\n"
		)
		
		public class FormatCents extends UDF {
			private static final Text DEFAULT_SYMBOL = new Text("$");
			private final Text result = new Text();
			
			public Text evaluate(IntWritable centsAsWritable) {
				return evaluate(centsAsWritable, DEFAULT_SYMBOL);
			}
			
			public Text evaluate(IntWritable centsAsWritable, Text symbol) {
				int cents = centsAsWritable.get();
				StringBuilder sb = new StringBuilder();
				if (cents <= 9) {
					sb.append("00");
				} else if (cents <= 99) {
					sb.append("0");
				}
				sb.append(cents);
				sb.insert(sb.length() - 2, ".");
				sb.insert(0, symbol);
				result.set(sb.toString());
				return result;
			}
		}
#Deploying Custom Libraries To Hive			
	-The first step in deployment is to package your UDF as a JAR file
		$ mvn package
	-Deployment may require assistance from your system administrator
		*Your account may lack permissions to make the necessary changes
	-Older versions of Hive used the ADD JAR command
		*This approach does not work with HiveServer2
		*We need to update the HiveServer2 daemon's classpath
	-Edit /etc/hive/conf/hive-site.xml on the node running HiveServer2
		*Add or edit the hive.aux.jars.path property
		*Set its value to the fully-qualified URI of your JAR file(s)
		*Multiple URIs are separated by commas for this property
			<property>
				<name>hive.aux.jars.path</name>
				<value>file:///home/bob/myudf.jar,file:///opt/lib/foo.jar</value>
			</property>
	-HiveServer2 will add these JARs to the distributed cache
	-We could also upload the JAR file to HDFS
		*In this case, the URI would begin with hdfs://instead of file://
	-Edit /etc/default/hive-server2 on the node running HiveServer2
		*Set the AUX_CLASSPATH environment variable to include all your JARS
		*The value follows the typical classpath format(colon delimited)
		*The value must always consist of local paths
		*This step adds the libraries to HiveServer2's runtime classpath
			export AUX_CLASSPATH=/home/bob/myudf.jar:/opt/lib/foo.jar
	-Finally, restart the hive-server2 service
		*You must redeploy the JAR and restart the service whenever you modify the code
			$ sudo service hive-server2 restart
	-Error messages in Beeline can be cryptic
	-If you have problems with a newly-registered UDF, check the logs
		-Typically located in the /var/log/hive directory
#Registering a User-Defined Function in Hive
	-Your UDF must be registered before using it in a query
	-This can be done using the CREATE TEMPORARY FUNCTION command
		> CREATE TEMPORARY FUNCTION KPM
			AS 'com.loudacre.example.KilometersPerMile';
		> SELECT KPM() * miles_to_warehouse AS km_to_warehouse
			FROM store_locations
			WHERE store_id=1234;
	-Repeat this step during every Hive session where the function is used		
											
						
	
												
					
			
					
		
		
											
			
			
			
								  											  											  			  				