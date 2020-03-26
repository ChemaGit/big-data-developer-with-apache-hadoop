# CLOUDERA CCA Spark and Hadoop Developer Exam (CCA175) with Scala

### Cloudera has recently changed the syllabus. The exam is right now all about Spark, this include Spark RDD, Spark Dataframes, and Spark SQL. You can check out the syllabus on the following link 
[https://www.cloudera.com/about/training/certification/cca-spark.html][Cloudera web page]

### And visit my repository about Spark 2.0 where you can find a lot of examples with Spark RDD, Pair RDD, Dataframes and Spark SQL
[https://github.com/ChemaGit/Apache-Spark-2.0-with-Scala] [Apache Spark 2 whith Scala]

````text
- Recently I got certified as Hadoop and Spark Developer - CCA175 and 
  I wanted to share my experience about the test and training in this repository.

- With all those that want to try it I share a huge part of the theory and especially a lot of hands-on, 
  more than 150 which will surely be helpful for those who want to try the test or just learn about 
  all the technologies around the test which are part of the Big Data ecosystem.
- Technologies such as: Hadoop, Spark, Spark Streaming, Sqoop, Flume, Kafka, Hive.....

- Some aspects of the exam: it takes two hours, it requires 70% marks to pass 
  and it can have between eight and twelve questions (in my case there were nine).

- The test is taken online through a virtual machine that you will access from your computer's web browser (chrome), 
  and it will be neccessary to have a webcam through which a proctor will be watching you so you cannot copy anything, 
  also you will have available web links to documentations about the tools that you can use in the exam, 
  such as the official documentation for Spark, Sqoop, Hive and so on.

- In the exam, you won't be forced to use a specific tool, I mean, 
  what is important is the final result, if you acomplish it with Pig, Hive, Spark, Impala, Flume, etc..., it will be perfect. 
  What is clear is that there are tools more adequate than others depending on the case.

- From my own experience I suggest studying the following subjects:

    1) Import and export using Sqoop and in both cases you have to consider 
       using and changing of field delimiters as well as line delimiters.
    
    2) In the particular case of importing be aware of compression (e.g. gzip, snappy, etc) 
       and format files (text, avro, parquet, etc...), at the same time to know how and when to use the arguments: 
       -m, --split-by --where, --query, --columns, --warehouse-dir, --target-dir, to mention some of them.
    
    3) Create a Hive table which data source will be text files, or data that are serialized in parquet, orc or avro format.
    
    4) CTAS (Create table as select) and export from Hive a file to HDFS as well as to local file system as a query result.
    
    5) Using Spark to read data in several formats (Text, JSON, ORC, Parquet, Avro or Sequence) 
       or even compressed and from them undertake a data transformation and export the result 
       to one of the possible formats that were mentioned previously.
    
    6) The Cloudera virtual machine has Eclipse and Sublime installed, 
       I suggest you to do scripts in Sublime and save them to return to them in the case it were neccesary.
    
    7) In the case of Spark, since I have more experience with Scala, 
       what I did was to launch the scripts with spark-shell with the following spark-shell command: 
       -i <file>.scala as well as from the console using :load <file>.scala
    
    8) In the case of Spark, since the Spark version was 1.6, I preferred to work with Dataframes 
       instead of having to do the operations with RDDs.
    
    9) Do a lot of exercises. In my case I created a GitHub repository where I did some exercises, 
       some made up by me, others from the website ITversity, others from Cloudera University and IBM Cognitive Class.
    
    10) It is important to do a good time schedule, so I suggest taking a couple of minutes to read the questions and 
        going after the easy ones to start and if at any moment you get stuck, you have to take the next one immediately.
    
    11) In the same way, while an operation is running (it can take one minute more or less), 
        take some time at least to read the next question.
    
    12) It's very important to be extremely careful with the data source and if possible, do a data backup.
    
    13) The virtual machine console has a letter font size that is quite small, so it would be a good idea to do a zoom in.

- So, that's all and good luck with your exam.
````



[Cloudera web page]: https://www.cloudera.com/about/training/certification/cca-spark.html