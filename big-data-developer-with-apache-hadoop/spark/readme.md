# SPARK
````
    - Apache Spark is a fast and general engine for large-scale data processing written in Scala
    - Spark-shell: interactive for learning or data exploration(Python, Scala)     
        - Python shell: pyspark
        - Scala shell: spark-shell
        
    - Every Spark application requires a Spark context
    - The Spark shell provides a preconfigured Spark context called sc
    
    - RDDs (Resilient Distributed Datasets)
         - Resilient: If data in memory is lost, it can be recreated
         - Distributed: Processed across the cluster
         - Dataset: Initial data can come from a source such as a file, or it can be created programmatically
    - RDDs are the fundamental unit of data in Spark
    - Most Spark programming consists of performing operations on RDDs
    - Three ways to create an RDD
         1.From a file or set of files
         2.From data in memory
         3.From another RDD
         
    - Two broad types of RDD operations
         -Actions return values
         -Transformations define a new RDD based on the current one(s)
         
    - Some common actions
         - count() returns the number of elements
         - take(n) returns an array of the first n elements
         - collect() returns an array of all elements
         - saveAsTextFile(dir) saves to text file(s)
         
    - Transformations create a new RDD from an existing one
    - RDDs are immutable
    - Two common transformations
         - map(function) creates a new RDD by performing a function on each record in the base RDD
         - filter(function) creates a new RDD by including or excluding each recor in the base RDD according to a Boolean function
         
    - Lazy evaluation. 
        - Transformations are lazy. Data in RDDs is not processed until an action is performed.
        
    - Actions are eager. 
        - Actions kick-off the computations and give a result.
        
    - Transformations may be chained together.
    - RDD Lineage and toDebugString
    - Spark maintains each RDD's lineage - the previous RDDs on which it depends
        - Use toDebugString to view the lineage an RDD
    - When possible, Spark will perform sequences of transformations by element so no data is stored
    
    - Functional programming in Spark
        - Spark depends heavily on the concepts of functional programming
        
    - Key concepts
         - Functions are the fundamental unit of programming
         - Functions have input and output only: No state or side effects
         - Passing functions as input to other functions
         - Anonymous functions
         - Many RDD operations take functions as parameters
         
    - RDDs can hold any serializable type of element
    
    - Some RDDs are specialized and have additional functionality
        - Pair RDDs: RDDs consisting of key-value pairs: each element must be a key-value pair (key, value1)...
        - Use with map-reduce algorithms
        - Many additional functions are available for common data processing needs:
            sorting, joining, grouping ......
        - Commonly used functions to create pair RDDs: map, flatMap/flatMapValues, keyBy
        - Pair RDD Operations specific to pair RDDs: countByKey, groupByKey, sortByKey, join....
        - Some other pair operations: keys, values, lookup(key), leftOuterJoin, rightOuterJoin, fullOuterJoin, mapValues, flatMapValues
        -Double RDDs: RDDs consisting of numerical data
        
    A common programming pattern
         1. Map separates datasets into Key-value pairRdds
         2. Join by Key
         3. Map joined data into the desired format
         4. Save, display or continue processing
```` 

