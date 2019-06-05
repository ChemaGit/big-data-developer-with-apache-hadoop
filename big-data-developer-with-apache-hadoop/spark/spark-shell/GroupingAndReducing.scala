case class CFFPurchase(customerId: Int, destination: String, price: Double)
/**
 * Assume we have an RDD of the purchases that users of the Swiss train
 * company's, the CFF's, mobile app have made in the past month.
 * Goal: calculate how many trips, and how much money was spent by
 * each individual customer over the course of the month.
 */
//1,Ginebra,12.3
//2,Paris,20.5
//2,Madrid,30.5
//1,Dublin,20.5
//3,Paris,20.5
//2,Paris,20.5
//3,Londres,20.5
//.....
val dir = "/loudacre/purchase.txt"
val purchasesRdd: RDD[CFFPurchase] = sc.textFile(dir)
                                       .map(line => line.split(","))
                                       .map(rec => new CFFPurchase(rec(0).toInt, rec(1), rec(2).toDouble))
                                       
val purchasesPerMonth = purchasesRdd.map(p => (p.customerId, p.price)) //Pair RDD
                                    .groupByKey() // groupByKey returns RDD[K, Iterable[V]]
                                    .map(p => (p._1, (p._2.size, p._2.sum)))
                                    .collect()
                                    //purchasesPerMonth: Array[(Int, (Int, Double))] = Array((4,(2,45.3)), (2,(3,71.5)), (1,(3,60.3)), (3,(2,48.0)))

/**
 * we can red uce before we shuffle. This could greatly reduce the
 * amount of data we have to send over the network.
 * We can use reduceByKey(func: (V, V) =>): RDD[(K, V)]
 */
                                    
//Goal: calculate how many trips, and how much money was spent by each
//individual customer over the course of the month.                                    
val dir = "/loudacre/purchase.txt"
val purchaseRdd: RDD[CFFPurchase] = sc.textFile(dir)
                                      .map(line => line.split(","))
                                      .map(rec => new CFFPurchase(rec(0).toInt, rec(1), rec(2).toDouble))
                                      .map(p => (p.customerId, (1, p.price)))
                                      .reduceByKey( (v, v1) => (v._1 + v1._2, v._2 + v1._2))
val purchasePerMonth = purchaseRdd.collect()                  
//purchasesPerMonth: Array[(Int, (Int, Double))] = Array((4,(2,45.3)), (2,(3,71.5)), (1,(3,60.3)), (3,(2,48.0)))