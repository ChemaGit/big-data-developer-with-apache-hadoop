//it is a singleton class
object Demo {

	class Order(orderId: Int, orderDate: String, orderCustomerId: Int, orderStatus: String) {
		
	}
	//:javap -p Order

	class Order1(val orderId: Int,val orderDate: String, val orderCustomerId: Int, val orderStatus: String) {
	}
	//:javap -p Order1

	class Order2(val orderId: Int,val orderDate: String,var orderCustomerId: Int, orderStatus: String)
	//:javap -p Order2

	case class Order3(orderId: Int,orderDate: String,orderCustomerId: Int,orderStatus:String)
	//:javap -p Order3
	/**
		* public class $line5.$read$$iw$$iw$Order3 implements scala.Product,scala.Serializable {
		* private final int orderId;
		* private final java.lang.String orderDate;
		* private final int orderCustomerId;
		* private final java.lang.String orderStatus;
		* public int orderId();
		* public java.lang.String orderDate();
		* public int orderCustomerId();
		* public java.lang.String orderStatus();
		* public $line5.$read$$iw$$iw$Order3 copy(int, java.lang.String, int, java.lang.String);
		* public int copy$default$1();
		* public java.lang.String copy$default$2();
		* public int copy$default$3();
		* public java.lang.String copy$default$4();
		* public java.lang.String productPrefix();
		* public int productArity();
		* public java.lang.Object productElement(int);
		* public scala.collection.Iterator<java.lang.Object> productIterator();
		* public boolean canEqual(java.lang.Object);
		* public int hashCode();
		* public java.lang.String toString();
		* public boolean equals(java.lang.Object);
		* public $line5.$read$$iw$$iw$Order3(int, java.lang.String, int, java.lang.String);
		* }
		*/

	def main(args: Array[String]): Unit = {
		val o = new Order(1, "2015-07-25", 100, "CLOSED")
		val o1 = new Order1(1, "2015-07-25", 100, "CLOSED")
		val o2 = new Order2(1, "2015-07-25", 100, "CLOSED")
		val o3 = new Order3(1, "2015-07-25", 100, "CLOSED")

		o.orderId

		println(o2.orderCustomerId)
		o2.orderCustomerId_Seq(200) //Setter method because is a "var variable"
		println(o2.orderCustomerId)
		o2.orderCustomerId = 300 //Setter method because is a "var variable"
		println(o2.orderCustomerId)
		println(o3.orderCustomerId)

		println(o(1))
		println(o1(2))
		println(o2(3))
		println(o3(0))

	}
}
