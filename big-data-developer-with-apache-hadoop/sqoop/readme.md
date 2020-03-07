# Examples in Sqoop
````text
    - Importing/exporting Relational Data with Apache Sqoop
    - Sqoop exchanges data between a database and HDFS
    - Sqoop is a command-line utility with several subcommands, called tools
    - imports are performed using Hadoop MapReduce jobs
    - sqoop also generates a Java source file for each table being imported
    - the file remains after import, but can be safely deleted
    - the import-all-tables tool imports an entire database
    	-stored as comma-delimited files
    	-default base location is your HDFS home directory
    	-data will be in subdirectories corresponding to name of each table
    - the import tool imports a single table(we can import only specified columns, or only matching rows from a single table)
    - we can specify a different delimiter(by default comma)
    - sqoop supports storing data in a compressed file
    - Sqoop supports importing data as Parquet or Avro files
    - Sqoop can export data from Hadoop to RDBMS
````
