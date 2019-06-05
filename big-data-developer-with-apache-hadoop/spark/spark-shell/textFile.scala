val rddCow = sc.textFile("/user/training/purplecow.txt")
rddCow.collect().foreach(x => println(x))
val rddDir = sc.textFile("/loudacre/kb/")
rddDir.collect().foreach(line => println(line))
val rddDirB = sc.textFile("/loudacre/kb/*.html")
rddDirB.collect().take(20).foreach(line => println(line))

val absoluteUri = sc.textFile("file:/home/training/accounts.java")
absoluteUri.take(20).foreach(line => println(line))
val absoluteUri2 = sc.textFile("hdfs://localhost/user/training/purplecow.txt")
absoluteUri2.collect().foreach(x => println(x))

val whole = sc.wholeTextFiles("/loudacre/kb/")
whole.take(1).foreach(x => println(x))