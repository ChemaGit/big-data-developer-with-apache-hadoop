package spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}

object StructureAndOptimization {

  val spark = SparkSession
    .builder()
    .appName("StructureAndOptimization")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "StructureAndOptimization")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/files/"
  
  case class Demographic(id: Int,
                         age: Int,
                         codingBootcamp: Boolean,
                         country: String,
                         gender: String,
                         isEthnicMinority: Boolean,
                         servedInMilitary: Boolean)
  
  case class Finances(id: Int,
                      hasDebt: Boolean,
                      hasFinancialDependents: Boolean,
                      hasStudentsLoans: Boolean,
                      income: Int)

	def main(args: Array[String]) {

    Logger.getRootLogger.setLevel(Level.ERROR)

    val demographics = sc.textFile(s"${path}demog.csv").map(line => line.split(","))
      .map(d => (d(0).toInt, new Demographic(d(0).toInt,d(1).toInt,d(2).toBoolean,d(3),d(4),d(5).toBoolean,d(6).toBoolean)) )

    val finances = sc.textFile(s"${path}finances.csv").map(line => line.split(","))
      .map(f => (f(0).toInt, new Finances(f(0).toInt,f(0).toBoolean,f(0).toBoolean,f(0).toBoolean,f(0).toInt)))

    /**
      * As an example, Let's count:
      * - Swiss students
      * - Who have debt & finalcial dependents
      */
    //Possibility 1
    //1. Inner join first
    //2. Filter to select people in Switzerland
    //3. Filter to select people with debt & financial dependents
    demographics.join(finances).filter({case(k,(d, f)) => d.country == "Switzerland" && f.hasFinancialDependents && f.hasDebt }).count

    //Possibility 2
    //1. Filter down the dataset first(look at only people with debt & financial dependents)
    //2. Filter to select people in Switzerland(look at only people in Switzerland)
    //3. Inner join on smaller, filtered down dataset
    val filtered = finances.filter({case(k, v) => v.hasFinancialDependents && v.hasDebt})
    demographics.filter({case(k, v) => v.country == "Switzerland"}).join(filtered).count

    //Possibility 3
    //1. Cartesian product on both datasets
    //2. Filter to select resulting of cartesian with same IDs
    //3. Filter to select people in Switzerland who have debt and financial dependents
    val cartesian = demographics.cartesian(finances)
    cartesian.filter({case(p1, p2) => p1._1 == p2._1})
      .filter({case(p1, p2) => p1._2.country == "Switzerland" && p2._2.hasFinancialDependents && p2._2.hasDebt }).count

    /**
      * The three solutions are right and the end result is the same.
      * But the time it takes to execute the job is vastly different.
      * For 150.000 people
      * Possibility 1: takes 4.97 seconds
      * Possibility 2: takes 1.35 seconds 3x faster(filtering data first) than 1
      * Possibility 3: takes 4 mins 177x slower!!!!
      */

    /**
      * Structured vs Unstructured Data
      * Given a bit of extra structural information, Spark can do many optimizations.
      *
      * Unstructured: Log files, Images ==>> Spark + regular RDDs don't know anything about the schema of the data it's dealing with.
      * Not much structure. Difficult to aggressively optimize.
      * RDDs operate on unstructured data.
      * We have to do all the optimization work ourselves!
      *
      * Semi-Structured: JSON, XML
      * Structured: Database tables.
      * Lots of structure.
      * Lots of optimization opportunities!
      * Spark SQL makes this possible!!!
      */
    
		sc.stop()
    spark.stop()
	}
}