/**
 * Problem Scenario 48 : You have been given below Python code snippet, with intermediate output.
 * We want to take a list of records about people and then we want to sum up their ages and count them.
 * So for this example the type in the RDD will be a Dictionary in the format of {name: NAME, age:AGE, gender:GENDER}.
 * The result type will be a tuple that looks like so (Sum of Ages, Count)
 * people = []
 * people.append({'name':'Amit', 'age':45,'gender':'M'})
 * people.append({'name':'Ganga', 'age':43,'gender':'F'})
 * people.append({'name':'John', 'age':28,'gender':'M'})
 * people.append({'name':'Lolita', 'age':33,'gender':'F'})
 * people.append({'name':'Dont Know', 'age':18,'gender':'T'}) 
 * peopleRdd=sc.parallelize(people) //Create an RDD
 * peopleRdd.aggregate((0,0), seqOp, combOp) //Output of above line : 167, 5)
 * Now define two operation seqOp and combOp , such that
 * seqOp : Sum the age of all people as well count them, in each partition. 
 * combOp : Combine results from all partitions.
 */
//Python
seqOp = (lambda x,y: (x[0] + y['age'],x[1] + 1)) 
combOp = (lambda x,y: (x[0] + y[0], x[1] + y[1]))
//Scala
val people = List(("Amit",45,"M"),("Ganga",43,"F"),("John",28,"M"),("Lolita",33,"F"),("Dont Know",18,"T"))
val peopleRdd = sc.parallelize(people)
def seqOp(z:(Int, Int), e: (String,Int,String)): (Int, Int) = { (z._1 + e._2, z._2 + 1)}
def combOp(v:(Int, Int), z:(Int, Int)): (Int, Int) = {(v._1 + z._1, v._2 + z._2)}
peopleRdd.aggregate(0,0)(seqOp ,combOp)