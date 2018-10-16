#RDD Lineage
	-Each transformation operation creates a new child RDD.
	-Spark keeps track of the parent RDD for each new RDD.
	-Child RDDs depend on their parents.
	-Action operations execute the parent transformations.
	-Each action re-executes the lineage transformations starting with the base.
		*By default
		
		val mydata = sc.textFile("purplecow.txt")
		val myrdd = mydata.map(s => s.toUpperCase()).filter(s => s.startsWith("I"))
		println(myrdd.count()) ==> 3
		
		println(myrdd.count()) ==> 3 
		
#RDD Persistence	
	-Persisting an RDD saves the data (in memory, by default)
	-Subsequent operations use saved data.	
	
		val mydata = sc.textFile("purplecow.txt")
		val myrdd = mydata.map(s => s.toUpperCase())
		myrdd.persist()
		val myrdd2 = myrdd.filter(s => s.startsWith("I"))		
		println(myrdd2.count()) ==> 3
		
		//Subsequent operations use saved data.
		println(myrdd2.count()) ==> 3 	
		
#Memory Pesistence
	-In-memory persistence is a suggestion to Spark
		*If no enough memory is available, persisted partitions will be cleared from memory
			-Least recently used partitions cleared first.
		*Transformations will be re-executed using the lineage when needed

#Persistence and Fault-Tolerance
	-RDD = Resilient Distributed Dataset
		*Resiliency is a product of tracking lineage
		*RDDs can always be recomputed from their base if needed
		
#Distributed Persistence
	-RDD partitions are distributed across a cluster
	-By default, partitions are persisted in memory in Executor JVMs
	-What happens if a partition persisted in memory becomes unavailable?
	-The driver starts a new task to recompute the partition on a different node
	-Lineage is preserved, data is never lost.
#Persistence Levels
	-By default, the persist method stores data in memory only
	-The persist method offers other options called storage levels
	-Storage levels let you control
		*Storage location(memory or disk)
		*Format in memory
		*Partition replication
#Persistence Levels: Storage Location
	-Storage location - where is the data stored?
		*MEMORY_ONLY: Store data in memory if it fits
		*MEMORY_AND_DISK: Store partitions on disk if they do not fit in memory(called spilling)
		*DISK_ONLY: Store all partitions on disk
			> import org.apache.spark.storage.StorageLevel
			> myrdd.persist(StorageLevel.DISK_ONLY)
#Persistence Levels: Memory Format
	-Serialization - you can choose to serialize the data in memory
		*MEMORY_ONLY_SER and MEMORY_AND_DISK_SER
		*Much more space efficient
		*Less time efficient
			-If using Java or Scala, choose a fast serialization library such as Kryo
#Persistence Levels: Partition Replication
	-Replication - store partitions on two nodes
		*DISK_ONLY_2
		*MEMORY_AND_DISK_2
		*MEMORY_ONLY_2
		*MEMORY_AND_DISK_SER_2
		*MEMORY_ONLY_SER_2
		*You can also define custom storage levels
#Default Persistence Levels
	-The storageLevel parameter for the persist() operation is optional
		*If no storage level is specified, the default value depends on the language
			-Scala default: MEMORY_ONLY
			-Python default: MEMORY_ONLY_SER
	-cache() is a synonym for persist() with no storage level specified
		> myrdd.cache()
		is equivalent to
		> myrdd.persist()
#Disk Persistence
	-Disk-persisted partitions are stored in local files
#Disk Persistence with Replication
	-Persistence replication makes recomputation less likely to be necessary
	-Replicated data on disk will be used to recreate the partition if possible
		*Will be recomputed if the data is unavailable
			-For example, when the node is down
#When and Where to Persist
	-When should you persist a dataset?
		*When a dataset is likely to be re-used
			-Such as in iterative algorithms and machine learning
	-How to choose a persistence level
		*Memory only - choose when possible, best performance
			-Save space by saving as serialized objects in memory if necessary
	-Dik - choose when recomputation is more expensive than disk read
		*Such as with expensive functions or filtering large datasets
	-Replication - choose when recomputation is more expensive than memory
#Changing Persistence Options
	-To stop persisting and remove from memory and disk
		*rdd.unpersist()
	-To change an RDD to a different persistence level
		*Unpersist first																																	