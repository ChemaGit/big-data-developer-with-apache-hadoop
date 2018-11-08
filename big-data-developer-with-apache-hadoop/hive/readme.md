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