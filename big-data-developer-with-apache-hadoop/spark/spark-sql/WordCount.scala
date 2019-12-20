val filename = "file:/home/training/training_materials/devsh/examples/example-data/purplecow.txt"

// Typical example of Word Count using RDDs
val countsRDD = sc.textFile(filename).
   flatMap(line => line.split(" ")).
   map(word => (word,1)).
   reduceByKey((v1,v2) => v1+v2)
   
// Word Count using Datasets instead
val countsDS = sqlContext.read.text(filename).as[String].
    flatMap(line => line.split(" ")).
    groupBy(word => word).
    count()