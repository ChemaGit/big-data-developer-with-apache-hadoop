// Collection -> Group of homogeneous elements
// Tuple -> Group of heteregeneous elements

object DemoCollections {

	def main(args: Array[String]): Unit = {
		val l = List(1, "Hello",4.0)
		val l1 = List(1,4.0)

		val t = (1,"Hello",4)
		println(t._1 + t._2 + t._3)
		t.productArity
	}
}
