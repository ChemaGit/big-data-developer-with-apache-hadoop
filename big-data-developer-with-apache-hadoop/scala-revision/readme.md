# Quick revision of programming language – Scala 2.11
	- As part of this session we will see quick revision of Scala as programming languages with emphasis on collections and map reduce APIs.
   		- Scala REPL, IDE and sbt
   		-  Variables – val vs. var
   		-  Pre-defined functions
       			- Operators
       			- String Manipulation
       			- Type Conversion
   		- User-defined functions
    		- Object Oriented Constructs
    		- Collections and Map Reduce APIs
    		- Usage of Tuples
    		- Development life cycle using Scala

# Scala REPL, IDE and sbt
	- As any other programming language, Scala also have multiple graphical interfaces to explore, validate and implement the code.	
    		- Scala REPL (Read, Evaluate, Print and Loop) is nothing but CLI to validate simple code snippets before adding to the modules associated with a project.
     		- IDE (Integrated Development Environment) – we should be comfortable with IDE and should use IDEs to build applications using any programming language. Following are some IDEs which are popular with respect to Scala.
        	- Eclipse with Scala plugin
        	- IntelliJ with Scala plugin (we will be using this for demo)
    		- Once we develop the projects or modules using IDE, we have to compile them into jar files so that they can be deployed. 
		- Maven is quite popular in Java world. 
		- With respect to Scala we can either use Maven or SBT. SBT is more popular to build Scala based applications over maven.

# Variables
	- Let us understand some important concepts around Scala Variables.
    		- Every variable is object
    		- It is not mandatory to explicitly specify data type (i = 0 and i: Int = 0 are same)
    		- If data type is not specified, it will be inherited based up on the value used for initialization of the corresponding variable.
    		- Once the object is created you cannot change the type of the object (strongly as well as statically typed)
    		- Primitive types are called as value classes, 
		- scala compiler will make sure we are exposed to all the functions on primitive types as well as, 
		- run efficiently with out significant overhead of a typical object unless the functions are used.
     		- You need to specify whether the variable is mutable or not mutable using var or val. 
		- If the object is defined as val we cannot assign different values to it.

# Pre-defined Functions
	- It is very important for us to understand the broad spectrum of pre-defined functions to leverage them as part of data processing.
		- Operator related functions
     		- All arithmetic operators are functions in Scala.
    		- We do not need to use . to invoke function and () to pass arguments. 
		- There are some limitations with respect to arguments.
	- String Manipulation functions
    		- Extracting information from fixed length fields – substring
    		- Extracting information from variable length fields – split and then use index as part of []
    		- Case conversion functions
    		- Trimming characters
    		- Padding characters
    		- and more
	- Type conversion functions
    		- As part of processing the data if we extract information from a string, 
		- the output will be returned as string even if original type is of non string type (such as int, float etc)
    		- It is important to convert type of our data to its original data type
    		- It can be achieved by functions of format toDATATYPE (eg: toInt, toFloat etc)
	- Collections related functions
    		- Scala gives us rich APIs to manipulate collections.
    		- Most of these APIs take anonymous functions as arguments
    		- filter, map, reduce, sum, avg, groupBy etc are example functions on type of collections.

```scala
val o = "1,2013-07-25 00:00:00.0,1185,CLOSED"

o.length #length of the string
o.split(',')(3) #extract status
o.split(',')(1).substring(0, 4).toInt #get year as number using split, substring and int
(o.split(',')(0).toInt, o.split(',')(1)) #convert o to tuple with order id as int and date
```

# User-defined Functions

	- Now let us see how we can develop user defined functions.
    		- Function starts with key word def
    		- We need to define arguments with data types
    		- Return type and return statement are optional
    		- If return statement is used, then return type is mandatory
    		- We can use varrying number of arguments while defining functions
    		- Scala also supports anonymous or lambda functions

````scala
def sumOfIntegers(lb: Int, ub: Int) = {
  var total = 0
  for (i <- (lb to ub))
    total += i
  total
}

print(sumOfIntegers(5, 10))

def sumOfIntegers(i: Int*) = {
  i.toList.sum
}

sumOfIntegers(1, 2, 3)

def sum(lb: Int, ub: Int, f: Int => Int) = {
  var total = 0
  for (i <- (lb to ub))
    total += f(i)
  total
}

print(sum(5, 10, i => i))
print(sum(5, 10, i => i * i))
print(sum(5, 10, i => if(i%2 == 0) i else 0))
````
# Object Oriented Constructs

	- Scala is known as pure object oriented programming languages. It supports different types of classes.

    	- class
        	- By default variables passed while defining class are constructor arguments
        	- If you define variables as val, they will be considered as class members which are immutable. You will get boiler plate code for getters.
        	- If you define variables as var, they will be considered as class members which are mutable. You will get boiler plate code for both getters and setters
    	- case class
        	- By default variables passed to case class are considered as class members which are immutable. You will get boiler plate code for getters.
        	- If you define variables as var, they will be considered as class members which are mutable. You will get boiler plate code for both getters and setters
        	- case class implements Serializable
        	- case class implements Product and hence we get boiler plate code for functions such as productArity, productElement and productIterator
    	- object
        	- It is singleton class
        	- We need to have object to define main method

# Collections and Map Reduce APIs

	- Scala Collections have support for many type of Collection objects and have APIs to process different types of Collections. 
	- Collections contain group of homogeneous elements.
    		- In the collection hierarchy you have Traversable at the higher level
    		- Then we have Iterable. Traversable and Iterable are traits which provide functionality for many APIs such as foreach, map, filter etc.
    		- All classes for collections implement Traversable and Iterable
     		- At a higher level it support Array (inherited from Java), List, Set and Map
    		- List is group of elements with length and index. Index starts with 0.
    		- Set is group of unique elements
    		- Map is group of key value pairs. Keys are unique.
    		- We can use APIs such as map, filter, reduce, groupBy to process data in collections.

# Usage of Tuples

	- While collection is like a table with homogeneous elements, tuple is like a row with columns of different type.
    		- Tuple contains group of heterogeneous elements
    		- It is nothing but object with attributes with no names
    		- As there are no names, we can access elements in tuple by position
    		- We use _1 to access first element, _2 to access second element.
    		- Tuples are represented in ()
    		- Tuples implement Product and hence we have APIs such as productArity, productIterator, productElement etc

# Development life cycle

	- Let us define problem statement and understand how we can develop the logic using Scala as Programming Language. 
	- This will be quick revision about all important Scala programming constructs.
    		- Data Set: order_items
    		- Table Structure:
        		- order_item_id (primary key)
        		- order_item_order_id (foreign key to orders.order_id)
        		- order_item_product_id (foreign key to products.product_id)
        		- order_item_quantity
        		- order_item_subtotal
        		- order_item_product_price
    	- Task 1: Get revenue for given order_item_order_id
        	- Define function getOrderRevenue with 2 arguments order_items and order_id
        	- Use map reduce APIs to filter order items for given order id, to extract order_item_subtotal and add it to get revenue
        	- Return order revenue
    	- Task 2: Get revenue for each order_item_order_id
        	- Define function getRevenuePerOrder with 1 argument order_items
        	- Use map reduce APIs to get order_item_order_id and order_item_subtotal, then group by order_item_order_id and then process the values for each order_item_order_id
        	- Return a collection which contain order_item_order_id and revenue_per_order_id

````scala
import scala.io.Source

object GetRevenueForOrderId {
  def getOrderRevenue(orderItems: List[String], orderId: Int) = {
    val orderRevenue = orderItems.
      filter(orderItem => orderItem.split(",")(1).toInt == orderId).
      map(orderItem => orderItem.split(",")(4).toFloat).
      reduce((t, v) => t + v)
    orderRevenue
  }

  def main(args: Array[String]): Unit = {
    val orderItems = Source.fromFile(args(0)).getLines.toList
    val orderRevenue = getOrderRevenue(orderItems, args(1).toInt)
    print(orderRevenue)
  }
}
````

````scala
import scala.io.Source


object GetRevenuePerOrder {
  def getRevenuePerOrder(orderItems: List[String]) = {
    val revenuePerOrder = orderItems.
      map(orderItem => (orderItem.split(",")(1).toInt, orderItem.split(",")(4).toFloat)).
      groupBy(_._1).
      map(e => (e._1, e._2.map(r => r._2).sum))
    revenuePerOrder
  }

  def main(args: Array[String]) = {
    val orderItems = Source.fromFile(args(0)).getLines.toList
    val revenuePerOrder = getRevenuePerOrder(orderItems)
    revenuePerOrder.take(10).foreach(println)
  }
}
````


	
 
 
