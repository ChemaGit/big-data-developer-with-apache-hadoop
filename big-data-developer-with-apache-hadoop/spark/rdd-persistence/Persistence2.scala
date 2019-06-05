  /** 
   * RDD Persistence 2
   */
  // Count web server log requests by user id
  val userReqs = sc.textFile("/loudacre/weblogs/*2.log").
     map(line => line.split(' ')).
     map(words => (words(2),1)).  
     reduceByKey((v1,v2) => v1 + v2)
   
  // Map account data to (userid,"lastname,firstname") pairs
  val accounts = sc.textFile("/loudacre/accounts/*").
    map(line => line.split(',')).
    map(values => (values(0),values(4) + ',' + values(3)))

  // Join account names with request counts
  val accountHits = accounts.join(userReqs).map(pair => pair._2)

  accountHits.filter(pair => pair._2 > 5).count()

  accountHits.persist()

  accountHits.filter(pair => pair._2 > 5).count()
  accountHits.toDebugString

  accountHits.unpersist()

  import org.apache.spark.storage.StorageLevel._

  accountHits.persist(DISK_ONLY)

  accountHits.filter(pair => pair._2 > 5).count()
  accountHits.toDebugString