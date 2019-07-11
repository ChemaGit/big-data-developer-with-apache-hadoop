//every variable is an object
val i = 0 // cannot be reassigned
val j = 1
i + j
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
val (id,date) = (o.split(",")(0),o.split(",")(1))

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
