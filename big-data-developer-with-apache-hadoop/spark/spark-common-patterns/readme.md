# Common Spark Use Cases(1)
````text
	-Spark is especially useful when working with any combination of:
		-Large amounts of data: Distributed storage
		-Intensive computations: Distributed computing
		-Iterative algorithms: In-memory processing and pipelining	
````

# Common Spark Use Cases(2)
````text
	-Risk analisys 
		-How likely is the borrower to pay back a loan?
	-Recommendations
		-Which products will this customer enjoy?
	-Predictions
		-How can we prevent service outages instead of simply reacting to them?
	-Classification
		-How can we tell which mail is spam and which is legitimate?
````

# Spark Examples
````text
	-Spark includes many example programs that demonstrate some common Spark programming patterns and algorithms
		-k-means
		-Logistic regression
		-Calculating pi
		-Alternating least squares(ALS)
		-Querying Apache web logs
		-Processing Twitter feeds
	-Examples
		-SPARK_HOME/lib ==> spark-examples.jar
		-SPARK_HOME is usually /usr/lib/spark
````
		
# Iterative Algorithms in Apache Spark. Example: PageRank
````text
	-PageRank gives web pages a ranking score based on links from other pages
		-Higher scores given for more links, and links from other high ranking pages
	-PageRank is a classic example of big data analysis(like word count)
		-Lots of data: Needs an algorithm that is distributable and scalable
		-Iterative: The more iterations, the better than answer
	-PageRank Algorithm(1)
		1. Start each page with a rank of 1.0
		2. On each iteration:
			a. Each page contributes to its neighbors its own rank divided by the number of its neighbors: contrib' = rank' / neighbors'
			b. Set each page's new rank based on the sum of its neighbors contribution: new_rank = sum(contrib - .85 + .15)
		3. Each iteration incrementally improves the page ranking
	-PageRank in Spark: Neighbor Contribution Function
		file:
		page1 page3
		page2 page1
		page4 page1
		page3 page1
		page4 page2
		page3 page4
````

````scala
		def computeContribs(neighbors: List[Page], rank: Double): List[(Page,Double)] = {
			for(neighbor <- neighbors)yield(neighbor, rank / neighbors.length)
		}						
		
		val links = sc.textFile(file).map(line => line.split(' '))
		                             .map(pages => (pages(0), pages(1)))
		                             .distinct()	
		                             .groupByKey()
		                             .persist()
	
//		links
//		(page4, (page2, page1))
//		(page2, (page1))
//		(page3, (page1, page4))
//		(page1, (page3))            
		
		val ranks = links.map({case(page, neighbors) => (page, 1.0)})
		
//		ranks
//		(page4, 1.0)
//		(page2, 1.0)
//		(page3, 1.0)
//		(page1, 1.0)          
		
		for(x <- 1 to 10) {
			val contribs = links.join(ranks)
			                    .flatMap({case( (page,(neighbors, rank)) ) => computeContribs(neighbors, rank)})
//			contribs
//			(page2, 0.5)
//			(page1, 0.5)
//			(page1, 1.0)
//			(page1, 0.5)
//			(page4, 0.5)
//			(page3, 1.0)    
			
			val ranks = contribs.reduceByKey({ case(v1,v2) => v1 + v2 })         
			                    .map({case(page, contrib) => (page, contrib - 0.85 + 0.15)}) 
			
//			ranks
//			(page4, 0.58)
//			(page2, 0.58)
//			(page3, 1.0)
//			(page1, 1.85)       
			
//			second iteration
//			ranks
//			(page4, 0.57)
//			(page2, 0.21)
//			(page3, 1.0)
//			(page1, 0.77)			                   
		}       		
````

# Checkpointing(1)
````text
	-Maintaining RDD lineage provides resilience bun can also cause problems when the lineage gets very long.
		-For example: iterative algorithms, streaming
	-Recovery can be very expensive
	-Potential stack overflow
````

# Checkpointing(2)
````text
	-Checkpointing saves the data to HDFS
		-Provides fault-tolerant storage across nodes
	-Lineage is not saved
	-Must be checkpointed before any actions on the RDD
		> sc.setCheckpointDir(directory)
````
		
# Machine Learning
````text
	-First consider how a typical programs works
		-Hardcoded conditional logic
		-Predefined reactions when those conditions are met
	-The programmer must consider all possibilities at design time
	-An alternative technique is to have computers learn what to do
````

# What is Machine Learning?
````text
	-Machine learning is a field within artificial intelligence(AI)
		-AI: "The science and engineering of making intelligent machines"
	-Machine learning focuses on automated knowledge acquisition
		-Primarily through the design and implementation of algorithms
		-These algorithms require empirical data as input
	-Machine learning algorithms "learn" from data and often produce a predictive model as their output
		-Model can then be used to make predictions as new data arrives
	-For example, consider a predictive model based on credit card customers
		-Build a model with data about customers who did/did not default on debt
		-Model can then be used to predict whether new customers will default
````

# Types of Machine Learning
````text
	-Three established categories of machine learning techniques:
		-Collaborative filtering(recommendations)
		-Clustering
		-Classification
````

````text
# What is Collaborative Filtering?
	-Collaborative filtering is a technique for making recommendations
	-Helps users find items of relevance
		-Among a potentially vast number of choices
		-Based on comparison of preferences between users
		-Preferences can be either explicit(stated) or implicit(observed)
````

# Applications Involving Collaborative Filtering(CF)
````text
	-Collaborative filtering is domain agnostic
	-Can use the same algorithm to recommend practically anything
		-Movies(Netflix, Amazon Instan Video)
		-Television(TiVO Suggestions)
		-Music(several popular music download and streaming services)		
	-Amazon uses CF to recommend a variety of products
````

# What is Clustering?
````text
	-Clustering algorithms discover structure in collections of data
		-Where no formal structure previously existed
	-They discover which clusters("groupings") naturally occur in data
		-By examining various properties of the input data
	-Clustering if often used for exploratory analysis
		-Divide huge amount of data into smaller groups
		-Can then tune analysis for each group
````

# Unsupervised Learning
````text
	-Clustering is an example of unsupervised learning
		-Begin with a data set that has no apparent label
		-Use an algorithm to discover structure in the data
			DATA ==> Algorithm ==> Model
	-Once the model has been created, you can use it to assign groups
			DATA ==> Model ==> DATA => Group
````

# Applications Involving Clustering
````text
	-Market segmentation
		-Group similar customers in order to target them effectively
	-Finding related news articles
		-Google News
	-Epidemiological studies
		-Identifying a "cancer cluster" and finding a root cause
	-Computer vision (groups of pixels that chhere into objects)
		-Related pixels clustered to recognize faces or license plates
````

# What is Classification?
````text
	-Classification is a form of supervised learning
		-This requires training with data that has known labels
		-A classifier can then label new data based on what it learned in training
````
# Supervised Learning
````text
	-Classification is an example of supervised learning
		-Begin with a data set that includes the value to be predicted(the label)
		-Use an algorithm to train a predictive model using the data-label pairs
			DATA ==> Label ==> Algorithm ==> Model
	-Once the model has been trained, you can make predictions
		-This will take new(previously unseen) data as input
		-The new data will not have labels
			New Data ==> Model ==> Predictions
````

# Applications Involving Classification
````text
	-Spam filtering
		-Train using a set of spam and non-spam messages
		-System will eventually learn to detect unwanted email
	-Oncology
		-Train using images of benign and malignant tumors
		-System will eventually learn to identify cancer
	-Risk Analysis
		-Train using financial records of customers who do/don't default
		-System will eventually learn to identify risk customers
````

# Relationship of Algorithms and Data Volume
````text
	-There are many algorithms for each type of machine learning
		-There's no overall "best" algorithm
		-Each algorithm has advantages and limitations
	-Algorithm choice is often related to data volume		
		-Some scale better than others
	-Most algorithms offer better results as volume increases
		-Best approach = simple algorithm + lots of data
	-Spark is an excellent platform for machine learning over large data sets
		-Resilient, iterative, parallel computations over distributed data sets
-  It's not who has the best algorithms that wins. It's who has the most data.
````

# Machine Learning Challenges
````text
	-Highly computation-intensive and iterative
	-Many traditional numerical processing systems do not scal to very large datasets
		-For example, MATLAB
````

# Spark MLlib and Spark ML
````text
	-Spark MLlib is a Spark machine learning library
		-Makes practical machine learning scalable and easy
		-Includes many common machine learning algorithms
		-Includes base data types for efficient calculations at scale
		-Supports scalable statistics and data transformations
	-Spark ML is a new higher-level API for machine learning pipelines
		-Built on top of Spark's DataFrames API
		-Simple and clean interface for running a series of complex tasks
		-Supports most functionality included in Spark MLlib
	-Spark MLlib and ML support a variety of machine learning algorithms
		-Such as ALS(alternating least squares), k-means, linear regression, logistic regression, gradient descent
````

# K-Means Clustering
````text
	-K-Means Clustering
		-A common iterative algorithm used in graph analysis and machine learning
	-Clustering
		-Goal: Find "clusters" of data points	
	-Example: K-Means Clustering(1)
		1. Choose k random points as starting centers
		2. Find all points closest to each center
		3. Find the center(mean) of each cluster
		4. If the centers changed, iterate again
			2. Find all points closest to each center
			3. Find the center(mean) of each cluster
			4. If the centers changed, iterate again
		5. Done!
		
		Alternative
		4. If the centers changed by more than c, iterate again
		5. Close enough!	
````