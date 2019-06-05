  /** 
   * AverageWordLengthByFirstLetter
   * Example: For each letter, calculate the average length of the words starting with that letter
   */
  val myfile = "file:/home/training/training_materials/data/frostroad.txt"
  // specify 4 partitions to simulate a 4-block file
  val avglens = sc.textFile(myfile,4).
  flatMap(line => line.split("\\W")).
  filter(line => line.length > 0).
  map(word => (word(0),word.length)).
  groupByKey(2).
  map(pair => (pair._1, pair._2.sum/pair._2.size.toDouble))
  // call save to trigger the operations
  avglens.saveAsTextFile("/loudacre/avglen-output")