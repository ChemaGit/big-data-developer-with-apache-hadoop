Recently I got certified as Hadoop and Spark Developer - CCA175 and I wanted to share my experience about the test and training in this repository.

With all those that want to try it I share a huge part of the theory and especially a lot of hands-on, more than 150 which will surely be helpful for those who want to try the test or just learn about all the technologies around the test which are part of the Big Data ecosystem.
Technologies such as: Hadoop, Spark, Spark Streaming, Sqoop, Flume, Kafka, Hive.....

Some aspects of the exam: it takes two hours, it requires 70% marks to pass and it can have between eight and twelve questions (in my case there were nine).

The test is taken online through a virtual machine that you will access from your computer's web browser (chrome), and it will be neccessary to have a webcam through which a proctor will be watching you so you cannot copy anything, also you will have available web links to documentations about the tools that you can use in the exam, such as the official documentation for Spark, Sqoop, Hive and so on.

In the exam, you won't be forced to use a specific tool, I mean, what is important is the final result, if you acomplish it with Pig, Hive, Spark, Impala, Flume, etc..., it will be perfect. What is clear is that there are tools more adequate than others depending on the case.

From my own experience I suggest studying the following subjects:

1) Import and export using Sqoop and in both cases you have to consider using and changing of field delimiters as well as line delimiters.

2) In the particular case of importing be aware of compression (e.g. gzip, snappy, etc) and format files (text, avro, parquet, etc...), at the same time to know how and when to use the arguments: -m, --split-by --where, --query, --columns, --warehouse-dir, --target-dir, to mention some of them.

3) Create a Hive table which data source will be text files, or data that are serialized in parquet, orc or avro format.

4) CTAS (Create table as select) and export from Hive a file to HDFS as well as to local file system as a query result.

5) Using Spark to read data in several formats (Text, JSON, ORC, Parquet, Avro or Sequence) or even compressed and from them undertake a data transformation and export the result to one of the possible formats that were mentioned previously.

6) The Cloudera virtual machine has Eclipse and Sublime installed, I suggest you to do scripts in Sublime and save them to return to them in the case it were neccesary.