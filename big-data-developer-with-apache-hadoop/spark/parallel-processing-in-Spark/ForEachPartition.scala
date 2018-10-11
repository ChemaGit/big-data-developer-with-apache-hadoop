  /** 
   * use foreachPartition to print out the first record of each partition
   */
  def printFirstLine(iter: Iterator[Any]) = {
    println(iter.next)
  }
  val accounts=sc.textFile("/loudacre/accounts/*")
  accounts.foreachPartition(printFirstLine)