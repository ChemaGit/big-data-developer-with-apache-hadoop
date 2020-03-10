package spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DataFrames {
  
  val sconf = new SparkConf
  val sc = new SparkContext(sconf) 
  
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("My App")
      .config("spark.master", "local")
      .getOrCreate()  
  
  // For implicit conversions like converting RDDs to DataFrames    
  import spark.implicits._       
  
  /**
   * DataFrames have their own APls
   * DataFrames are: A relational API over Spark's RDDs
   * Able to be automatically aggressively optimized
   * DataFrames are Untyped!
   */
  
  /**
   * DataFrames data types
   * To enable optimization opportunities, Spark SQL's DataFrames operate on
   * a restricted set of data types.
   * 
   * Basic Spark SQL Data Types: 
   * ByteType,ShortType,IntegerType,LongType,DecimalType,FloatType
   * DoubleType,BinaryType,BooleanType,BooleanType,TimestampType,DateType,StringType
   * 
   * Complex Spark SQL Data Types:
   * ArrayType(elementType, containsNull)
   * MapType(keyType, valueType, valueContainsNull)
   * case class StructType(List[StructFields])
   */
  
  /**
   * ArrayType
   * Array of only one type of element (elementType}. containsNull is set to true if the
   * elements in ArrayType value can have null values.
   * ArrayType(StringType, true)
   */
  
  /**
   * MapType
   * Map of key /value pairs with two types of elements. value containsNull is set to true if
   * the elements in MapType value can have null values.
   * MapType(IntegerType, StringType, true)
   */
  
  /**
   * case class StructType(List[StructFields])
   * Struct type with list of possible fields of different types. containsNull is set to true if the
   * elements in StructFields can have null values.
   * StructType(List(StructField("name", StringType, true),StructField("age", IntegerType, true)))
   * 
   * It's possible to arbitrarily nest complex data types! For example:
   * 
   * to access any of these data types, we must first import Spark SQL types!
   * import org.apache.spark.sql.types._
   */
  case class Account(balance: Double,employees:Array[Employee])
  case class Employe(id: Int,name: String,jobTitle: String)
  case class Project(title: String,team: Array[Employee],acct: Account)
  case class Employee(id: Int, fname: String, lname: String, age: Int, city: String)
  case class Listing(street: String, zip: Int, price: Int)
  case class Post(authorID: Int, subforum: String, likes: Int, date: String)
  case class Abo(id: Int, v: (String, String))
  case class Loc(id: Int, v: String)
  case class Finances(id: Int,
                      hasDebt: Boolean,
                      hasFinancialDependents: Boolean,
                      hasStudentsLoans: Boolean,
                      income: Int)
  case class Demographic(id: Int,
                         age: Int,
                         codingBootcamp: Boolean,
                         country: String,
                         gender: String,
                         isEthnicMinority: Boolean,
                         servedInMilitary: Boolean)
  /*
  StructType(
		StructField(title,StringType,true),
		StructField(
			team,
			ArrayType(
				StructType(StructField(id,IntegerType,true),
				StructField(name,StringType,true),
				StructField(jobTitle,StringType,true)),
				true),
			true),
			StructField(
				acct,
				StructType(
					StructField(balance,DoubleType,true),
					StructField(
						employees,
						ArrayType(
							StructType(StructField(id,IntegerType,true),
							StructField(name,StringType,true),
							StructField(jobTitle,StringType,true)),
							true),
						true)
					),
				true)
			)
  */
  
  /**
   * DataFrames API: Similar-looking to SQL. Example methods include:
   * select, where, limit, orderBy, groupBy, join
   * 
   * show() pretty-prints DataFrame in tabular form. Shows first 20 elements.
   * printSchema() prints the schema of your DataFrame in a tree format.
   */
  case class Person(id: Int, name: String, country: String, city: String)
  
	def main(args: Array[String]) {

    try {
      val tuplePerson = sc.textFile("dir").map(line => line.split(",")).map(f => new Person(f(0).toInt, f(1), f(2), f(3)))
      val personDF = tuplePerson.toDF //infer the attributes from the case class's fields.
      personDF.printSchema()
      // root
      // 1-- id: integer (nullable = true)
      // 1-- name: string (nullable = true)
      // 1-- country: string (nullable = true)
      // 1-- city: string (nullable = true)

      /**
        * Common DataFrame Transformations
        * Like on RDDs, transformations on DataFrames are (1) operations which return a
        * DataFrame as a result, and (2) are lazily evaluated.
        *
        * selects a set of named columns and returns a new DataFrame with these columns as a result.
        * def select(col: String, cols: String*): DataFrame
        *
        * performs aggregations on a series of columns and returns a new DataFrame with the calculated output.
        * def agg(expr: Column, exprs: Column*): DataFrame
        *
        * groups the DataFrame using the specified columns. Intended to be used before an aggregation.
        * def groupBy(col1: String, cols: String*): DataFrame
        *
        * inner join with another DataFrame
        * def join(right: DataFrame): DataFrame
        *
        * Other transformations include: filter, limit, orderBy, where, as, sort, union, drop, amongst others.
        */

      /**
        * Most methods on DataFrames tend to some well-understood, pre-defined operation on a column of the data set
        * You can select and work with columns in three ways:
        * 1. Using $-notation: $-notation requires: import spark.implicits._
        * df.filter($"age" > 18)
        * 2. Referring to the Dataframe
        * df.filter(df("age") > 18))
        * 3. Using SQL query string
        * df.filter("age > 18")
        */

      /**
        * EXAMPLE
        * We'd like to obtain just the IDs and last names of employees working in a
        * specific city, say, Sydney, Australia. Let's sort our result in order of in creasing employee ID.
        * Rather than using SQL syntax, let's convert our example to use the DataFrame API.
        */

      //Let's assume we have a DataFrame representing a data set of employees:
      //DataFrame with schema defined in Emplyee case class
      val employeeDF = sc.textFile("dir").map(line => line.split(",")).map(f => new Employee(f(0).toInt, f(1), f(2), f(3).toInt,f(4)))
        .toDF
      val sydneyEmployeesDF = employeeDF.select("id", "lname")
        .where("city == 'Sydney'")
        .orderBy("id")
      // employeeDF:
      //+---+-----+-------+---+--------+
      // id |lfnamel lname|age | city  |
      //+---+-----+-------+---+--------+
      //| 121 Joel Smithl 381New York |
      //| 563ISallyl Owensl 481New York|
      // l645ISlatelMarkhaml 281 Sydney|
      // 1221 | David| Walker|21 Sydney|
      //+---+-----+-------+---+--------+

      //sydneyEmployeesDF:
      //+---+-------+
      //|id | lname|
      //+---+-------+
      //1221 | Walker|
      //l6451 |Markham|
      //+---+-------+

      /**
        * The DataFrame API makes two methods available for filtering:
        * filter and where (from SQL). They are equivalent!
        */
      val over30 = employeeDF.filter("age > 30").show()
      val over30W = employeeDF.where("age > 30").show()

      val filterComplex = employeeDF.filter(($"age" > 25) && ($"city" === "Sydney")).show()
      employeeDF.filter("age > 25 && city == 'Sydney'").show()

      /**
        * Grouping and Aggregating on Data Frames
        * One of the most common tasks on tables is to (1) group data by a certain
        * attribute, and then (2) do some kind of aggregation on it like a count.
        *
        * For grouping & aggregating, SparkSQL provides:
        * - a groupBy function which returns a RelationalGroupedDataset,
        *   which has several standard aggregation functions defined on ti like count, sum, min, max, and avg.
        *
        * - How to group and aggregate?
        *   Just call groupBy on specific attribute/column(s) of a DataFrame,
        *   followed by a call to a method on RelationalGroupedDataset like count, max,
        *   or agg (for agg, also specify which attribute/column(s) subsequent
        *   spark.sql.functions like count, sum, max, etc, should be called upon.)
        */
      employeeDF.groupBy($"id").agg(sum($"age"))
      employeeDF.groupBy($"id").count()

      /**
        * Example:
        * Let's assume that we have a dataset of homes currently for sale in an
        * entire US state. Let's calculate the most expensive, and least expensive
        * homes for sale per zip code.
        */
      val listingsDF = sc.textFile("dir").map(line => line.split(",")).map(f => new Listing(f(0), f(1).toInt, f(2).toInt))
        .toDF
      val mostExpensive = listingsDF.groupBy($"zip").max("price")
      val lessExpensive = listingsDF.groupBy($"zip").min("price")

      /**
        * Example:
        * Let's assume we have the following data set representing all of the posts in a
        * busy open source community's Discourse forum.
        * Let's say we would like to tally up each authors' posts per subforum, and then
        * rank the authors with the most posts per subforum.
        */
      val postsDF = sc.textFile("dir").map(line => line.split(",")).map(f => new Post(f(0).toInt, f(1), f(2).toInt, f(3)))
        .toDF
      val rankedDF = postsDF.groupBy($"authorID", $"subforum")
        .agg(count($"authorID")) // new DF with columns authorID, subforum, count(authorID)
        .orderBy($"subforum", $"count(authorID)".desc)

      /**
        * After callinggroupBy, methods on RelationalGroupedDataset:
        * To see a list of all operations you can call following a groupBy, see
        * the API docs for RelationalGroupedDataset.
        * http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.RelationalGroupedDataset
        */

      /**
        * Methods within agg:
        * Examples include: min, max, sum, mean, stddev, cout, avg, first, last. To see a list
        * of all operations you can call within an agg, see the API docs for org.apache.spark.sql.functions.
        * http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$
        */

      /**
        * Cleaning data with DataFrames
        * Sometimes you may have a data set with null or NaN values.
        * We can do one of the following:
        * - drop rows/records with unwanted values like null or NaN
        * - replace certain values with a constant.
        * 1. Dropping records with unwanted values:
        * - drop() ==>> drops rows that contain null or NaN values in all
        *   columns and returns a new DataFrame.
        * - drop("all") ==>> drops rows that contain null or NaN values in
        *   all columns and returns a new DataFrame
        * - drop(Array("id", "name")) ==>> drops rows that contain null or
        *   Nan values in the specified columns and returns a new DataFrame.
        * 2. Reclacing unwanted values:
        * - fill(0) ==>> replaces all occurrences of null or NaN in numeric columns
        *   with specified value and returns a new DataFrame.
        * - fill(Map("minBalance" -> 0)) ==>> replaces all occurrences of null or NaN
        *   in specified column with spcified value and returns a new DataFrame.
        * - replace(Array(id), Map(1234 -> 8923)) replaces specified value (1234) in
        *   specified column (id) with specified replacement value (8923) and returns
        *   a new DataFrame.
        */

      /**
        * Common Actions on DataFrames
        * - collect(): Array[Row] ==>> returns an array that contains all
        *   of Rows in this DataFrame
        * - count(): Long ==>> returns the number of rows in the DataFrame
        * - first(): Row/head(): Row ==> returns the first row in the DataFrame
        * - show(): Unit ==>> displays the top 20 rows of DataFrame in a tabular form.
        * - take(n: Int): Array[Row] ==>> returns the first n rows in the DataFrame.
        */

      /**
        * Joins on DataFrames
        * Several types of joins are available:
        * inner, outer, left_outer, right_outer, leftsemi.
        * Performing joins:
        * - inner join ==>> df1.join(df2, $"df1.id" === $"df2.id")
        * It's possible to change the join type by passing an additional string
        * parameter to join specifying which type of join to perform
        * - df1.join(df2, $"df1.id" === $"df2.id", "right_outer")
        */
      val as = List(Abo(101, ("Ruetli", "AG")),Abo(102, ("Brelaz", "DemiTarif")),Abo(103, ("Gress", "DemiTarifVisa")),Abo(104, ("Schatten", "DemiTarif")))
      val abosDF = sc.parallelize(as).toDF

      val ls = List(Loc(101, "Bern"),Loc(101, "Thun"),Loc(102, "Lausanne"),Loc(102, "Geneve"),
        Loc(102, "Nyon"),Loc(103, "Zurich"),Loc(103, "St-Gallen"),Loc(103, "Chur"))
      val locationsDF = sc.parallelize(ls).toDF

      locationsDF.show()
      abosDF.show()

      /**
        * How do we combine only customers that have a subscription and
        * where there is location info?
        */
      val trackedCustomerDF = abosDF.join(locationsDF, abosDF("id") === locationsDF("id"))

      /**
        * the CFF wants to know for which subscribers the CFF has
        * managed to collect location information. It's possible that
        * someone has demi-tarif, but doesn't use the CFF app and only
        * pays cash for tickets.
        */
      val abosWithOptionalLocationsDF = abosDF.join(locationsDF, abosDF("id") === locationsDF("id"), "left-outer")

      /**
        * CodeAward is offering scholarships to programmers who have
        * overcome adversity.
        * We have the following two datasets.
        * Let's count
        * Swiss students who have debt & financial dependents.
        */
      val demographicsDF = sc.textFile("dir").map(line => line.split(","))
        .map(d => (new Demographic(d(0).toInt,d(1).toInt,d(2).toBoolean,d(3),d(4),d(5).toBoolean,d(6).toBoolean)) )
        .toDF

      val financesDF = sc.textFile("dir").map(line => line.split(","))
        .map(f => (new Finances(f(0).toInt,f(0).toBoolean,f(0).toBoolean,f(0).toBoolean,f(0).toInt)))
        .toDF
      val filtered = financesDF.filter($"hasDebt" && $"hasFinancialDependents")
      val joined = demographicsDF.filter($"country" === "Switzerland").join(filtered, demographicsDF("id") === filtered("id"), "inner")
        .count()
      /**
        * Comparing performance between hadwritten
        * RDD-based solutions and DataFrame solution:
        * it's almost the same than RDD-based solution
        * with filter first.
        * DataFrame 1.24 seconds
        * RDD-filtered 1.35 seconds
        * It's possible becausse Spark SQL comes with two
        * specialized backend components:
        * - Catalyst, query optimizer
        * - Tungsten, off-heap serializer.
        */

      /**
        * Limitations of DataFrames: Untyped!
        * listingsDF.filter($"state" === "CA")
        * //org.apache.spark.sql.AnalysisException:
        * //cannot resolve "state" given input columns: [street, zip, price];;
        * Your code compiles, but you get runtime exceptions when you attempt to
        * run a query on a column that doesn't exist.
        * Would be nice if this was caught at compile time like we're used to in Scala!
        *
        * Limited Data Types
        * If your data can't be expressed by case classes/Products and standard
        * Spark SQL data types, it may be difficult to ensure that a Tungsten encoder
        * exists for your data type.
        *
        * Requires Semi-Structured/Structured Data
        * If your unstructured data cannot be reformulated to adhere to
        * some kind of schema, IT WOULD BE BETTER TO USE RDDs.
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