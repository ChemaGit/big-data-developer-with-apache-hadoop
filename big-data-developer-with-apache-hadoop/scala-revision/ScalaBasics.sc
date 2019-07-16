//every variable is an object
val i = 0 // cannot be reassigned
val p = 1
i + p
var c = 0 //mutable variable
val d = 0 //inmutable variable
val j = 4

/**Predefinided functions*/
i + j
i.+(j)
i * j
i.*(j)

val o = "1,2013-07-25 00:00:00.0,1185,CLOSED"
o.split(",")
o.split(",")(3)
o.length
o
val o1 = "10,2013-07-25 00:00:00.0,1185,CLOSED"
o1.split(",")(1).substring(0,4).toInt
(1,"Hello")
o
(o.split(",")(0),o.split(",")(1))
val (idx,date) = (o.split(",")(0),o.split(",")(1))

val l = List(1,2,3,4,5,6)
l.length
l(0)

val s = Set(1,2,3,4,5,6)
s(5)

val m = Map(1 -> "H", 2 -> "World")
m(1)
m.keys
m.values
l.foreach(println)
m.foreach(println)
s.foreach(println)

l
l.reduce( (v,v1) => v + v1)
l.min
l.max
l.foreach(println)

/**User defined functions **/
def sumOfIntegers(lb: Int,ub: Int): Int = {
	var total = 0
	for(i <- lb to ub) {
		total += i
	}
	total
}
sumOfIntegers(1,10)
sumOfIntegers(5, 20)

//Sum of squares of integers
def sumOfSquares(lb: Int, ub: Int): Int = {
	var total = 0
	for(i <- lb to ub){
		total += i * i
	}
	total
}
sumOfSquares(1,4)

def sum(lb: Int,ub: Int,f:Int => Int): Int = {
	var total = 0
	for(i <- lb to ub){
		println(s"Value of i: $i")
		total += f(i)
	}
	println(s"Value of total: $total")
	total
}

def id(i: Int) = i
def sqr(i: Int) = i * i
def even(i: Int) = if(i % 2 == 0) i else 0

sum(5, 10, id)
sum(5, 10, sqr)
sum(5, 10, even)
sum(5, 10, i => i * i)
sum(5, 10, i => {if(i % 2 == 0) i else 0})
