package example

// Copy example data to HDFS home directory first:
// hdfs dfs -put $DEVSH/examples/example-data/people.json
// hdfs dfs -put $DEVSH/examples/example-data/pcodes.json
// hdfs dfs -put $DEVSH/examples/example-data/zcodes.json
// hdfs dfs -put $DEVDATA/static_data/accounts_avro/ /loudacre/

// mvn package
// spark-submit --class example.SQLExample --name 'SQLExample' --master yarn-client target/sql-example-1.0.jar
// spark-submit --class example.SQLExample --name 'SQLExample' --master 'local[*]' target/sql-example-1.0.jar
// spark-submit --class example.SQLExample --name 'SQLExample' --master 'local[8]' target/sql-example-1.0.jar
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object SQLExample {
   def main(args: Array[String]) {
   
      val sc = new SparkContext()
      sc.setLogLevel("WARN")

      val sqlContext = new HiveContext(sc)
      import sqlContext.implicits._

      // Display the names of tables in the Hive Metastore
      val tables = sqlContext.tableNames()
      tables.foreach(println)

      // Example JSON data source

      val peopleDF = sqlContext.read.json("people.json")
      peopleDF.show
      peopleDF.dtypes.foreach(println)

      // Example Hive table data source
      val deviceDF = sqlContext.read.table("device")
      deviceDF.show

      // Example third party data source
      // import com.databricks.spark.avro._
      val accountsAvro = sqlContext.read.format("com.databricks.spark.avro").load("/loudacre/accounts_avro")
      accountsAvro.show

      // JDBC example -- these two examples are equivalent  
      val dbaccountsDF = sqlContext.read.format("jdbc").option("url","jdbc:mysql://localhost/loudacre").option("dbtable","accounts").option("user","training").option("password","training").load()
      //dbaccountsDF = sqlContext.read.option("user","training").option("password","training").jdbc("jdbc:mysql://localhost/loudacre","accounts")
      dbaccountsDF.show
  
      // Example actions
      peopleDF.count
      peopleDF.limit(3).show

      // Example queries

      peopleDF.select($"name").show 

      // Other example queries
      peopleDF.select(peopleDF("name"),peopleDF("age")+10).show
      peopleDF.sort(peopleDF("age").desc).show


      // Examples of join
      val pcodesDF = sqlContext.read.json("pcodes.json")
      // basic inner join when column names are equal
      peopleDF.join(pcodesDF, "pcode").show
      // left outer join with equal column names
      peopleDF.join(pcodesDF, Array("pcode"), "left_outer").show
      // create the same DF but with different column name for zip codes
      val zcodesDF = pcodesDF.withColumnRenamed("pcode","zip")
      // inner join with differing column names
      peopleDF.join( zcodesDF, $"pcode" === $"zip").show

         
      // RDD example: convert to (pcode,name) pair RDDs and group by pcode
      val peopleRDD = peopleDF.rdd
      val peopleByPCode = peopleRDD.
        map(row => 
                (row(row.fieldIndex("pcode")),
                 row(row.fieldIndex("name")))).
        groupByKey()


      val schema = StructType( Array (
            StructField("age", IntegerType, true),
            StructField("name", StringType, true),
            StructField("pcode", StringType, true)))

       val rowrdd=sc.parallelize(Array(Row(40, "Abram", "01601"),Row(16,"Lucia","87501")))
       val mydf = sqlContext.createDataFrame(rowrdd,schema)
       mydf.show(2)
       
       
       sc.stop()

   }
}