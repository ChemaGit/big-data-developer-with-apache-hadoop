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


 



	
 
 
