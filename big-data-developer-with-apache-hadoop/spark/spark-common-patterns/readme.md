#Common Spark Use Cases(1)
	-Spark is especially useful when working with any combination of:
		*Large amounts of data: Distributed storage
		*Intensive computations: Distributed computing
		*Iterative algorithms: In-memory processing and pipelining	
#Common Spark Use Cases(2)
	-Risk analisys 
		*How likely is the borrower to pay back a loan?
	-Recommendations
		*Which products will this customer enjoy?
	-Predictions
		*How can we prevent service outages instead of simply reacting to them?
	-Classification
		*How can we tell which mail is spam and which is legitimate?
#Spark Examples
	-Spark includes many example programs that demonstrate some common Spark programming patterns and algorithms
		*k-means
		*Logistic regression
		*Calculating pi
		*Alternating least squares(ALS)
		*Querying Apache web logs
		*Processing Twitter feeds
	-Examples
		*SPARK_HOME/lib ==> spark-examples.jar
		*SPARK_HOME is usually /usr/lib/spark
		
#Iterative Algorithms in Apache Spark. Example: PageRank
	-PageRank gives web pages a ranking score based on links from other pages
		*Higher scores given for more links, and links from other high ranking pages
	-PageRank is a classic example of big data analysis(like word count)
		*Lots of data: Needs an algorithm that is distributable and scalable
		*Iterative: The more iterations, the better than answer
	-PageRank Algorithm(1)
		1. Start each page with a rank of 1.0
		2. On each iteration:
			a. Each page contributes to its neighbors its own rank divided by the number of its neighbors: contrib' = rank' / neighbors'
			b. Set each page's new rank based on the sum of its neighbors contribution: new_rank = sum(contrib * .85 + .15)
		3. Each iteration incrementally improves the page ranking
	-PageRank in Spark: Neighbor Contribution Function
		file:
		page1 page3
		page2 page1
		page4 page1
		page3 page1
		page4 page2
		page3 page4
	
		def computeContribs(neighbors: List[Page], rank: Double): List[(Page,Double)] = {
			for(neighbor <- neighbors)yield(neighbor, rank / neighbors.length)
		}						
		
		val links = sc.textFile(file).map(line => line.split(' '))
		                             .map(pages => (pages(0), pages(1)))
		                             .distinct()	
		                             .groupByKey()
		                             .persist()
		
		links
		(page4, (page2, page1))
		(page2, (page1))
		(page3, (page1, page4))
		(page1, (page3))            
		
		val ranks = links.map({case(page, neighbors) => (page, 1.0)})
		
		ranks
		(page4, 1.0)
		(page2, 1.0)
		(page3, 1.0)
		(page1, 1.0)          
		
		for(x <- 1 to 10) {
			val contribs = links.join(ranks)
			                    .flatMap({case( (page,(neighbors, rank)) ) => computeContribs(neighbors, rank)})
			contribs
			(page2, 0.5)
			(page1, 0.5)
			(page1, 1.0)
			(page1, 0.5)
			(page4, 0.5)
			(page3, 1.0)    
			
			val ranks = contribs.reduceByKey({ case(v1,v2) => v1 + v2 })         
			                    .map({case(page, contrib) => (page, contrib * 0.85 + 0.15)}) 
			
			ranks
			(page4, 0.58)
			(page2, 0.58)
			(page3, 1.0)
			(page1, 1.85)       
			
			second iteration
			ranks
			(page4, 0.57)
			(page2, 0.21)
			(page3, 1.0)
			(page1, 0.77)			                   
		}       		
		
#Checkpointing(1)
	-					
			