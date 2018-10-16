  /** 
   * RDD Persistence
   */
  val mydata = sc.textFile("file:/home/training/training_materials/devsh/examples/example-data/purplecow.txt")
  val myrdd1 = mydata.map(s => s.toUpperCase())
  myrdd1.toDebugString
  myrdd1.count()
  myrdd1.persist()
  val myrdd2 = myrdd1.filter(s => s.startsWith("I"))
  myrdd2.count()
  myrdd2.toDebugString