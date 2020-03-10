package spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object Datasets {
  
  val sconf = new SparkConf
  val sc = new SparkContext(sconf) 
  
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("My App")
      .config("spark.master", "local")
      .getOrCreate()

  case class Person(id: Int, age: Int, country: String)
  case class Listing(street: String, zip: Int, price: Int)
  
  // For implicit conversions like converting RDDs to DataFrames    
  import spark.implicits._  
  

  
	def main(args: Array[String]) {

    try {
      /**
        * Let's say we've just done the following computation on a DataFrame
        * representing a data set of Listings of homes for sale; we've computed the
        * average price of for sale per zipcode.
        */
      val listingsDF = sc.textFile("dir").map(line => line.split(",")).map(f => new Listing(f(0), f(1).toInt, f(2).toInt)).toDF
      val mostExpensive = listingsDF.groupBy($"zip").max("price")
      val lessExpensive = listingsDF.groupBy($"zip").min("price")

      val averagePricesDF = listingsDF.groupBy($"zip").avg("price")
      val averagePrices = averagePricesDF.collect() // averagePrices: Array[org.apache.spark.sql.Row]
      //Oh right, I have to cast things because Rows don't have type information
      //associated with them. How many columns were my result again? And what were their types?

      val averagePricesAgain = averagePrices.map {row => (row(0).asInstanceOf[String], row(1).asInstanceOf[Int])} // java.lang.ClassCastException

      averagePrices.head.schema.printTreeString()
      // root
      // |-- zip: integer (nullable = true)
      // |-- avg(price): double (nullable = true)

      val averagePricesAgainB = averagePrices.map{row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[Double])}
      // mostExpensiveAgain: Array[(Int, Double)]

      /**
        * Wouldn't it be nice if we could have both Spark SQL optimizations and type safety?
        * DATASET
        * What is a Dataset
        * - Datasets can be thought of as typed distributed collections of data.
        * - Dataset API unifies the DataFrame and RDD APls. Mix and match!
        * - Datasets require strucutred/semi-structured data. Schemas and
        *   Encoders core part of Datasets.
        *
        * Think of Datasets as a compromise between RDDs & DataFrames.
        * You get more type information on Datasets than on DataFrames, and you
        * get more optimizations on Datasets than you get on RDDs.
        */
      //the average home price per zipcode with Datasets.
      val listingsDS = sc.textFile("dir").map(line => line.split(",")).map(f => new Listing(f(0), f(1).toInt, f(2).toInt)).toDS()
      //We can freely mix APIs
      val avgDS = listingsDS.groupByKey(l => l.zip) // looks like groupByKey on RDDs
        .agg(avg($"price").as[Double]) // looks like our DataFrame operators!

      /**
        * - Datasets are a something in the middle between DataFrames and RDDs
        * - You can still use relational DataFrame operations as we learned in previous sessions on Datasets.
        * - Datasets add more typed operations that can be used as well.
        * - Datasets let you use higher-order functions like map, flatMap, filter again!
        *
        * Datasets can be used when you want a mix of functional and relational
        * transformations while benefiting from some of the optimizations on DataFrames.
        */

      /**
        * Creating Datasets
        * - From a DataFrame: just use the toDS convenience method.
        *   myDF.toDS // requires import spark.implicits._
        * - to read in data from JSON from a file, which can be done with
        *   the read method on the SparkSession object and then converted to a Dataset:
        * - From an RDD: Just use the toDS convenience method
        *   myRDD.toDS  // requires import spark.implicits._
        * - From common Scala types: Just use the toDS convenience method.
        */
      val myDS = spark.read.json("peoplel.json").as[Person]

      val myDSFromScalaType = List("yay","ohnoes","hooray!").toDS()  // requires import spark.implicits._

      /**
        * Typed Columns
        * To create a TypedColumn, all you have to do is call as[...] on your (untyped) Column.
        * $"price".as[Double] // this now represents a TypedColumn.
        */

      /**
        * Transformations on DataSets
        * The Dataset API includes both untyped and typed transformations.
        * untyped transformations the transformations we learned on DataFrames.
        * typed transformations typed variants of many DataFrame transformations +
        * additional transformations such as ROD-like higher-order functions map, flatMap, etc.
        *
        * Common (Typed) Transformations on Datasets
        * - map[U](f: T => U): Dataset[U] ==>> Apply function to each element in the Dataset and return a Dataset of the result.
        * - flatMap[U](f: T => TraversableOnce[U]): Dataset[U] ==>> Apply a function to each element in the Dataset and return a
        *   Dataset of the contents of the iterators returned.
        * - filter(pred: T => Boolean): Dataset[T] ==>> Apply predicate function to each element in the Dataset
        *   and return a Dataset of elements that have passed the predicate condition, pred.
        * - distinct(): Dataset[T] ==>> Return Dataset with duplicates removed.
        * - groupByKey[K](f: T => K): KeyValueGroupedDataset[K, T] ==>> Apply function to each element in the Dataset
        *   and return a Dataset of the result.
        * - coalesce(numPartitions: Int): Dataset[T] ==>> Apply a function to each element in the Dataset and return
        *   a Dataset of the contents of the iterators returned.
        * - repartition(numPartitions: Int): Dataset[T] ==>> Apply predicate function to each element in the Dataset
        *   and return a Dataset of elements that have passed the predicate condition, pred.
        */

      /**
        * Grouped operations on DataSets
        * Like on DataFrames, Datasets have a special set of aggregation operations meant to be
        * used after a call to groupByKey on a Dataset.
        * - calling groupByKey on a Dataset returns a KeyValueGroupedDataset
        * - KeyValueGroupedDatasetcontains a number of aggregation operations which return Datasets.
        *
        * Note: using groupBy on a Dataset, you will get back a RelationalGroupedDataset whose
        * aggregation operators will return a DataFrame. Therefore, be careful to avoid groupBy if you
        * would like to stay in the Dataset API.
        *
        * Some KeyValueGroupedDataset Aggregation Operations
        * - reduceGroups(f: (V, V) => V): Dataset[(K, V)] ==>> Reduces the elements of each group of data using the specified
        *   binary function. The given function must be commutative and associative or the result may be non-deterministic.
        * - agg[U](col: TypedColumn[V, U]): Dataset[(K, U)] ==>> Computes the given aggregation, returning a Dataset of tuples for
        *   each unique key and the result of computing this aggregation over all elements in the group.
        * - mapGroups[U](f: (K, Iterator[V]) => U): Dataset[U] ==>> Applies the given function to each group of data. For each unique
        *   group, the function will be passed the group key and an iterator that contains all of the elements in the group.
        *   The function can return an element of arbitrary type which will be returned as a new Dataset.
        * - flatMapGroups[U](f: (K, Iterator[V]) => TraversableOnce[U]): Dataset[U] ==>> Applies the given function
        *   to each group of data. For each unique group, the function will be passed the group key and
        *   an iterator that contains all of the elements in the group. The function can return an iterator
        *   containing elements of an arbitrary type which will be returned as a new Dataset.
        *
        * Using the General agg Operation: agg[U](col: TypedColumn[V, U]): Dataset[(K, U)]
        * - Typically, we simply select one of these operations from function, such as avg,
        *   choose a column for avg to be computed on, and we pass it to agg (it's a typed column):
        *   someDS.agg(avg($"column").as[Double])
        */

      /**
        * Challenge: Emulate the semantics of reduceByKey on a Dataset using Dataset operations
        *            presented so far. Assume we'd have the following data set:
        * Find a way to use Datasets to achive the same result that you would get if you
        * put this data into an RDD and called:
        */
      val keyValues = List((3, "Me"), (1, "Thi"), (2, "Se"), (3, "ssa"), (1, "sisA"), (3, "ge:"), (3, "-)"), (2, "ere"), (2, "t" ))
      val keyValuesRDD = sc.parallelize(keyValues)
      keyValuesRDD.reduceByKey(_ + _)

      val keyValuesDS = keyValues.toDS()
      keyValuesDS.groupByKey(p => p._1).mapGroups((k, vs) => (k, vs.foldLeft("")((acc, p) => acc + p._2)))
        .sort($"_1").show()
      //+---+----------+
      //| _1|       _2 |
      //+---+----------+
      //| 1 |  ThisIsA |
      //| 2 |    Secret|
      //| 3 |Message:-)|
      //+---+----------+
      // The only issue with this approach is this disclaimer in the API docs for mapGroups:
      //This function not support partial aggregation, and as a result requires shuffling
      //all the data in the Dataset. If an application intends to perform an aggregation
      //over each key, it is best to use the reduce function or an org.apache.spark.sql.expressions#Aggregator

      /**
        * Aggregators ==>> class Aggregator[-IN, BUF, OUT] org.apache.spark.sql.expressions.Aggregator
        * - IN: is the input type to the aggregator. When using an aggregator after groupByKey, this is the
        *       type that represents the value in the key/value pair.
        * - BUF: is the intermediate type during aggregation.
        * - OUT: is the type of the output of the aggregation.
        *
        * This is how implement our own Aggregator
        *    val myAgg = new Aggregator[IN, BUF, OUT]{
        *      def zero: BUF = ...                      //The initial value.
        *      def reduce(b: BUF, a: IN): BUF = ...     //Add an element to the running total
        *      def merge(b1: BUF, b2: BUF): BUF = ...   //Merge intermediate values.
        *      def finish(b: BUF): OUT = ...            //Return the final result.
        *    }.toColumn
        *
        * ENCODERS
        * - Encoders are what convert your data between JVM objects and Spark SQL's specialized
        *   internal(tabular) representation. They're required by all Datasets!
        * - Encoders are highly specialized, optimized code generators that generate custom
        *   bytecode for serialization and deserialization of your data, improving memory utilization.
        * - Uses significantly less memory than Kryo/Java serialization
        * - >10x faster than Kryo serialization(Java serialization orders of magnitude slower)
        *
        * Two ways to introduce encoders:
        * 1. Automatically(generally the case) via implicits from a SpakSession import spark.implicits._
        * 2. Explicitly via org.apache.spark.sql.Encoders which contains a large selection of methods
        *    for creating Encoders from Scala primitive types and Products
        *
        * - creation methods in Encoders
        *   INT/LONG/STRING etc, for nullable primitives.
        *   scalaInt/scalaLong/scalaByte etc, for Scala's primitives.
        *   product/tuple for Scala's Product and tuple types.
        * - Examples:
        *   Encoders.scalaInt //Encoder[Int]
        *   Encoders.STRING //Encoder[String]
        *   Encoders.product[Person] //Encoder[Person], where Person extends Product/is a case class.
        *
        * Emulating reduceByKey with an Aggregator
        */
      val strConcat = new Aggregator[(Int, String), String, String]{
        def zero: String = ""
        def reduce(b: String, a: (Int, String)): String = b + a._2
        def merge(b1: String, b2: String): String = b1 + b2
        def finish(r: String): String = r
        override def bufferEncoder: Encoder[String] = Encoders.STRING   //Tell Spark which Encoders you need.
        override def outputEncoder: Encoder[String] = Encoders.STRING
      }.toColumn

      //pass it to your aggregator
      keyValuesDS.groupByKey(pair => pair._1).agg(strConcat.as[String])
        .sort($"value")
        .show()
      //+-----+--------------------+
      //|value|anon$1(scala.Tuple2)|
      //+-----+--------------------+
      //|    1|            ThisIsA |
      //|    2|              Secret|
      //|    3|          Message:-)|
      //+-----+--------------------+

      /**
        * Common Dataset Actions
        * - collect(): Array[T] ==>> returns an array  that contains all of Rows in this Dataset.
        * - count(): Long ==>> Returns the number of rows in the Dataset.
        * - first(): T/head(): T ==>> returns the first row in this Dataset.
        * - foreach(f: T => Unit): Unit ==>> Applies a function f to all rows.
        * - reduce(f: (T, T) => T): T ==>> reduces the elements of this Dataset using the specified binary function.
        * - show(): Unit ==>> Displays the top 20 rows of Dataset in a tabular form.
        * - take(n: Int): Array[T] ==>> returns the first n rows in the Dataset.
        */


      /**
        * When to use Datasets vs Data Frames vs RDDs?
        * 1. Use Datasets when:
        *    - you have structured/semi-structured data
        *    - you want typesafety
        *    - you need to work with functional APIs
        *    - you need good performance, but it doesn't have to be the best.
        * 2. Use DataFrames when:
        *    - you have structured/semi-structured data
        *    - you want the best possible performance, automatically optimized for you
        * 3. Use RDDs when:
        *    - you have unstructured data
        *    - you need to fine-tune and manage low-level details of RDD computations
        *    - you have complex data types that cannot be serialized with Encoders
        */

      /**
        * Limitations of Datasets
        * - Catalyst optimizes this case.
        *   Relational filter operation: ds.filter($"city".as[String] === "Boston")
        *   Performs best because you're explicitly telling Spark which columns/atributes
        *   and conditions are required in your filter operation. Avoids data moving over the network
        *
        * - Catalyst cannot optimize this case.
        *   Functional filter operation: ds.filter(p => p.city == "Boston")
        *   Same filter written with a function literal is opaque to Spark - it's
        *   impossible for Spark to introspect the lambda function.
        */

      /**
        * Limitations of Datasets
        * Catalyst Can't Optimize All Operations
        * - When using Datasets with higher-order functions like map, you miss
        *   out on many Catalyst optimizations.
        * - When using Datasets with relational operations like select, you get
        *   all of Catalyst's optimizations.
        * - Though not all operations on Datasets benefit from Catalyst's optimizations,
        *   Tungsten is still always running under the hood of Datasets, storing and
        *   organizing data in a highly optimized way, which can result in large
        *   speedups over RDDs.
        * - Limited Data Types
        *   If your data can't be expressed by case classes/Products and standard
        *   Spark SQL data types, it may be difficult to ensure that a Tungsten
        *   encoder exists for your data type.
        * - Requires Semi-Structured/Structured Data
        *   If your unstructured data cannont be reformulated to adhere to some kind
        *   of schema, it would be better to use RDDs.
        */
      // To have the opportunity to view the web console of Spark: http://localhost:4040/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("SparkContext stopped.")
      spark.stop()
      println("SparkSession stopped.")
    }
	}
}