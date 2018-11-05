/**
 * Problem Scenario 67 : You have been given below code snippet.
 * lines = sc.parallelize(['its fun to have fun,','but you have to know how.'])
 * r1 = lines.map(lambda x: x.replace(',','').replace('.','').lower())
 * r2 = r1.flatMap(lambda x: x.split(' '))
 * r3 = r2.map(lambda x: (x, 1))
 * operation1
 * r5 = r4.map(lambda x:(x[1],x[0]))
 * r6 = r5.sortByKey(ascending=False)
 * r6.take(20)
 * Write a correct code snippet for operationl which will produce desired output, shown below.
 * [(2, 'fun'), (2, 'to'), (2, 'have'), (1, 'its'), (1, 'know'), (1, 'how'), (1, 'you'), (1, 'but')]
 */
//Python
r4 = r3.reduceByKey(lambda v, v1: v + v1)

lines = sc.parallelize(['its fun to have fun,','but you have to know how.'])
r1 = lines.map(lambda x: x.replace(',','').replace('.','').lower())
r2 = r1.flatMap(lambda x: x.split(' '))
r3 = r2.map(lambda x: (x, 1))
r4 = r3.reduceByKey(lambda v, v1: v + v1)
r5 = r4.map(lambda x:(x[1],x[0]))
r6 = r5.sortByKey(ascending=False)
r6.collect()

//Scala
val r4 = r3.reduceByKey( (v, v1) =>  v + v1)

val lines = sc.parallelize(List("its fun to have fun,","but you have to know how."))
val r1 = lines.map(x => x.replace(",","").replace(".","").toLowerCase())
val r2 = r1.flatMap(x => x.split(" "))
val r3 = r2.map(x => (x, 1))
val r4 = r3.reduceByKey( (v, v1) => v + v1)
val r5 = r4.map(x => (x._2,x._1))
val r6 = r5.sortByKey(ascending=False)
r6.collect()