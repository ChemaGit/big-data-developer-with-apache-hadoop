//working with Rdd's
val dir = "/loudacre/weblogs/*"
val pairRdd = sc.textFile(dir)
pairRdd.count()
val jpgs = pairRdd.filter(line => line.contains(".jpg"))
jpgs.take(10).foreach(line => println(line))
jpgs.count()
val lenLines = jpgs.map(line => line.length)
lenLines.take(5).foreach(len => println(len))
val arr = jpgs.map(line => line.split(" "))
arr.take(5).foreach(line => println(line.mkString(",")))
val ipRdd = arr.map(line => line(0))
ipRdd.take(5).foreach(println)
//saving the Rdd as a text file
ipRdd.saveAsTextFile("/loudacre/ipList")

/*************************************/
/**
 * Use RDD transformation to create a dataset of the IP address and
 * corresponding user ID for each request for an HTML file.
 * Display the data in the form ipaddress/userid
*/
val dir = "/loudacre/weblogs/*"
val rdd = sc.textFile(dir)
            .filter(file => file.contains(".html"))
            .map(line => line.split(" "))
            .map(line => (line(0), line(2)))
rdd.collect().foreach(tuple => println(tuple._1 + "/" + tuple._2)) 

rdd.map(line => line._1 + "/" + line._2).repartition(1).saveAsTextFile("/loudacre/ipIdList")