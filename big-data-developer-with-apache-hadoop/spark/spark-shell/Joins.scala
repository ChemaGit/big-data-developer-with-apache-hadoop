  /**
   * Joins
   * Joins are another sort of transformations on Pair RDDs. 
   * They're used to combine multiple datasets.
   * Two kind of joins:
   * - Inner joins(join)
   * - Outer joins(leftOuterJoin/rightOuterJoin) 
   */
  /**
   * Inner Join
   */
  //Let's assume the following concrete data:
  val as = List((101, ("Ruetli", "AG")), (102, ("Brelaz", "DemiTarif")),( 103, ("Gress", "DemiTarifVisa")), ( 104, ( "Schatten", "DemiTarif")))
  val abos = sc.parallelize(as) // RDD[(Int, (String, String))]
  val ls = List((101, "Bern"), (101, "Thun"), (102, "Lausanne"), (102, "Geneve"),(102, "Nyon"), (103, "Zurich"), (103, "St-Gallen"), (103, "Chur"))
  val locations = sc.parallelize(ls) // RDD[(Int, String)]
  /**
   * One RDD representing customers and their subscriptions ( abos), and another representing
   * customers and cities they frequently travel to (locations).
   * How do we combine only customers that have a subscription and where there is location info?
   */
  val trackedCustomers = abos.join(locations) // trackedCustomers: RDD[(Int, ((String, String) , String) ) ]
  trackedCustomers.collect().foreach(println)
  // (101, ((Ruetli, AG) , Bern) )
  // (101, ((Ruetli, AG) , Thun) )
  // (102, ((Brelaz, DemiTarif) , Nyon) )
  // (102, ((Brelaz, DemiTarif) , Lausanne) )
  // (102, ((Brelaz, DemiTarif) , Geneve) )
  // (103, ((Gress, DemiTarifVisa) , St-Gallen) )
  // (103, ((Gress, DemiTarifVisa) , Chur) )
  // (103, ((Gress, DemiTarifVisa) , Zurich) ) 
  //What happened to customer 104 ?
  
  /**
   * Outer Join
   * Outer joins return a new RDD containing combined pairs whose keys don't have to be present in both input RDDs.
   * Example: Let's assume the CFF wants to know for which subscribers the CFF
   * has managed to collect location information. E.g., it's possible that someone has
   * a demi-tarif, but doesn't use the CFF app and only pays cash for tickets.
   * Which join do we use?
   */
  val abosWithOptionlLocations = abos.leftOuterJoin(locations) // abosWithOptionallocations: RDD[(Int, ((String, String) , Option[String]))]  
  abosWithOptionLocations.collect().foreach(println)
  //(101 , ( (Ruetli, AG), Some (Thun)))
  //(101 , ( (Ruetli, AG), Some (Bern)))
  //(102, ( (Brelaz, DemiTarif), Some (Geneve)))
  //(102, ( (Brelaz, DemiTarif), Some (Nyon)))
  //(102, ( (Brelaz, DemiTarif), Some (Lausanne)))
  //(103, ( (Gress, DemiTarifVisa), Some (Zurich)))
  //(103, ( (Gress, DemiTarifVisa), Some (St-Gallen)))
  //(103, ( (Gress, DemiTarifVisa), Some (Chur)))
  //(104, ( (Schatten, DemiTarif), None))  <=====Since we use a leftOuterJoin, keys are guaranteed to occur in the left source RDD.
 
  /**
   * We can do the converse using a rightOuterJoin
   * The CFF wants to know for which customers (smartphone app users) it has subscriptions for.
   * E.g., it's possible that someone uses the mobileapp, but has no demi-tarif.
   */
  val customersWithLocationDataAndOptionalAbos = abos.rightOuterJoin(locations)// RDD[(Int, (Option[(String, String)], String))]
  //  (101, (Some ( (Ruetli, AG)), Bern))
  //  (101 , (Some ( (Ruetli, AG)), Thun))
  //  (102, (Some ( (Brelaz, DemiTarif)), Lausanne))
  //  (102, (Some ( (Brelaz, DemiTarif)), Geneve))
  //  (102, (Some ( (Brelaz, DemiTarif)), Nyon))
  //  (103, (Some ( (Gress, DemiTarifVisa)), Zurich))
  //  (103, (Some ( (Gress, DemiTarifVisa)), St-Gallen))
  //  (103, (Some ( (Gress, DemiTarifVisa)), Chur))  
  //Note that, here, customer 104 disappears again because that customer doesn't have
  //location info stored with the CFF {the right ROD in the join).  
  
  /**
   * Shuffles Happen
   * Shuffles can be an enormous hit to because it means that Spark must send
   * data from one node to another. Why? Latency!  
   */
