/** Question 47
  * Problem Scenario 42 : You have been given a file (files/sales.txt), with the content as
  * given in below.
  * spark10/sales.txt
  * Department,Designation,costToCompany,State
  * Sales,Trainee,12000,UP
  * Sales,Lead,32000,AP
  * Sales,Lead,32000,LA
  * Sales,Lead,32000,TN
  * Sales,Lead,32000,AP
  * Sales,Lead,32000,TN
  * Sales,Lead,32000,LA
  * Sales,Lead,32000,LA
  * Marketing,Associate,18000,TN
  * Marketing,Associate,18000,TN
  * HR,Manager,58000,TN
  * And want to produce the output as a csv with group by Department,Designation,State with additional columns with sum(costToCompany) and TotalEmployeeCountt
  * and average cost
  * Use both RDD and SQL solutions
  * Should get result like
  * Dept,Desg,state,empCount,totalCost,avgCost
  * Sales,Lead,AP,2,64000
  * Sales,Lead,LA,3,96000
  * Sales,Lead,TN,2,64000
  *
  * save the result as text-file in using gzip compression at /user/cloudera/question47/text-gzip
  * save the result as avro-file using snappy compression at /user/cloudera/question47/avro-snappy
  * save the result as parquet-file using snappy compression at /user/cloudera/question47/parquet-snappy
  * save the result as json-file using bzip2 compression at /user/cloudera/question47/json-bzip
  * save the result as sequence file without compression at /user/cloudera/question47/sequence
  * save the result as orc file without compression at /user/cloudera/question47/orc
  * save sales.txt and the result as jdbc tables (t_sales, t_sales_cost) in database mysql:hadoopexam
  * save sales.txt and the result as hive tables in parquet-snappy format in database hadoopexam
  */
$ hdfs dfs -put /home/cloudera/files/sales.txt /user/cloudera/files/
  $ hdfs dfs -cat /user/cloudera/files/sales.txt

// SPARK-RDD
val sales = sc.textFile("/user/cloudera/files/sales.txt").map(line => line.split(",")).map(r => ( (r(0),r(1),r(3)), (r(2).toInt,1)))
val agg = sales.aggregateByKey( (0,0) )( ( (u: (Int, Int),v: (Int, Int)) => (u._1 + v._1,u._2 + v._2)) , ( (v:(Int,Int),c: (Int,Int)) => (v._1 + c._1,v._2 + c._2)))
val avg = agg.mapValues({case(t,c) => (c,t,t/c)}) // [((String, String, String), (Int, Int, Int))]
val resultRdd = avg.map({case( ((dep,deg,state),(empC,tCost,avg)) ) => "%s,%s,%s,%d,%d,%d".format(dep,deg,state,empC,tCost,avg) })

// SPARK-SQL
val sales = sc.textFile("/user/cloudera/files/sales.txt").map(line => line.split(",")).map(r => (r(0),r(1),r(2).toInt,r(3))).toDF("department","designation","costToCompany","state")
sales.show()
sales.registerTempTable("sales")

val resultSql = sqlContext.sql("""SELECT department,designation,state,count(department) as empCount,sum(costToCompany) as totalCost,avg(costToCompany) as avgCost FROM sales GROUP BY department,designation,state""")
// [department: string, designation: string, state: string, empCount: bigint, totalCost: bigint, avgCost: double]

// OUTPUTS
// save the result as text-file in using gzip compression at /user/cloudera/question47/text-gzip
resultSql.rdd.map(r => r.mkString(",")).repartition(1).saveAsTextFile("/user/cloudera/question47/text-gzip", classOf[org.apache.hadoop.io.compress.GzipCodec])
// save the result as avro-file using snappy compression at /user/cloudera/question47/avro-snappy
import com.databricks.spark.avro._
sqlContext.setConf("spark.sql.avro.compression.codec","snappy")
resultSql.write.avro("/user/cloudera/question47/avro-snappy")
// save the result as parquet-file using snappy compression at /user/cloudera/question47/parquet-snappy
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
resultSql.write.parquet("/user/cloudera/question47/parquet-snappy")
// save the result as json-file using bzip2 compression at /user/cloudera/question47/json-bzip
resultSql.toJSON.saveAsTextFile("/user/cloudera/question47/json-bzip",classOf[org.apache.hadoop.io.compress.BZip2Codec])
// save the result as sequence file without compression at /user/cloudera/question47/sequence
resultSql.rdd.map(r => (r(0).toString, r.mkString(","))).saveAsSequenceFile("/user/cloudera/question47/sequence")
// save the result as orc file without compression at /user/cloudera/question47/orc
resultSql.write.orc("/user/cloudera/question47/orc")
// save sales.txt and the result as jdbc tables (t_sales, t_sales_cost) in database mysql:hadoopexam
val props = new java.util.Properties()
props.setProperty("user","root")
props.setProperty("password","cloudera")
sales.write.jdbc("jdbc:mysql://quickstart:3306/hadoopexam","t_sales",props)
resultSql.write.jdbc("jdbc:mysql://quickstart:3306/hadoopexam","t_sales_cost",props)
// save sales.txt and the result as hive tables in parquet-snappy format in database hadoopexam
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
sales.write.parquet("/user/hive/warehouse/hadoopexam.db/t_sales")
resultSql.write.parquet("/user/hive/warehouse/hadoopexam.db/t_sales_cost")
sqlContext.sql("use hadoopexam")
sqlContext.sql("""CREATE TABLE t_sales(department string,designation string,costToCompany int,state string) STORED AS PARQUET LOCATION "/user/hive/warehouse/hadoopexam.db/t_sales" TBLPROPERTIES("parquet.compression"="snappy") """)
sqlContext.sql("""CREATE TABLE t_sales_cost(department string, designation string, state string, empCount bigint, totalCost bigint, avgCost double) STORED AS PARQUET LOCATION "/user/hive/warehouse/hadoopexam.db/t_sales_cost" TBLPROPERTIES("parquet.compression"="snappy") """)


$ hdfs dfs -ls /user/cloudera/question47/text-gzip
$ hdfs dfs -text /user/cloudera/question47/text-gzip/part-00000.gz

$ hdfs dfs -ls /user/cloudera/question47/avro-snappy
$ avro-tools getmeta hdfs://quickstart.cloudera/user/cloudera/question47/avro-snappy/part-r-00000-34602f71-3525-487b-8161-9deb23361757.avro
$ avro-tools cat hdfs://quickstart.cloudera/user/cloudera/question47/avro-snappy/part-r-00000-34602f71-3525-487b-8161-9deb23361757.avro -

$ hdfs dfs -ls /user/cloudera/question47/parquet-snappy
$ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/question47/parquet-snappy/part-r-00000-1701ec93-0f1f-4ae6-9884-85c50fcd6c87.snappy.parquet
$ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/question47/parquet-snappy/part-r-00000-1701ec93-0f1f-4ae6-9884-85c50fcd6c87.snappy.parquet

$ hdfs dfs -ls /user/cloudera/question47/json-bzip
$ hdfs dfs -text /user/cloudera/question47/json-bzip/part-00000.bz2

$ hdfs dfs -ls /user/cloudera/question47/sequence
$ hdfs dfs -text /user/cloudera/question47/sequence/part-00000

$ hdfs dfs -ls /user/cloudera/question47/orc
$ hdfs dfs -text /user/cloudera/question47/orc/part-r-00000-c7e2aed1-5963-4b99-b9a9-d8ed7fa7e472.orc

$ mysql
  mysql> use hadoopexam;
mysql> show tables;
mysql> select * from t_sales;
mysql> select * from t_sales_cost;
mysql> exit;

$ hive
  hive> use hadoopexam;
hive> show tables;
hive> describe formatted t_sales;
hive> select * from t_sales;
hive> describe formatted t_sales_cost;
hive> select * from t_sales_cost;
hive> exit;