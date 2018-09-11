sc.textFile("purplecow.txt").map(line => line.toUpperCase()).filter(line => line.startsWith("I")).count()

sc.textFile("purplecow.txt").map(line => line.toUpperCase())
                            .filter(line => line.startsWith("I"))
                            .repartition(1)
                            .saveAsTextFile("/user/training/upperFil")