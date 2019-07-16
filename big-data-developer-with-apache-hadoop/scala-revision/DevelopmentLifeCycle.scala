import scala.io.Source

object GetRevenueForOrderId {
  def getOrderRevenue(orderItems: List[String], orderId: Int) = {
    val orderRevenue = orderItems.
      filter(orderItem => orderItem.split(",")(1).toInt == orderId).
      map(orderItem => orderItem.split(",")(4).toFloat).
      reduce((t, v) => t + v)
    orderRevenue
  }

  def main(args: Array[String]): Unit = {
    val orderItems = Source.fromFile(args(0)).getLines.toList
    val orderRevenue = getOrderRevenue(orderItems, args(1).toInt)
    print(orderRevenue)
  }
}
object GetRevenuePerOrder {
  def getRevenuePerOrder(orderItems: List[String]) = {
    val revenuePerOrder = orderItems.
      map(orderItem => (orderItem.split(",")(1).toInt, orderItem.split(",")(4).toFloat)).
      groupBy(_._1).
      map(e => (e._1, e._2.map(r => r._2).sum))
    revenuePerOrder
  }

  def main(args: Array[String]) = {
    val orderItems = Source.fromFile(args(0)).getLines.toList
    val revenuePerOrder = getRevenuePerOrder(orderItems)
    revenuePerOrder.take(10).foreach(println)
  }
}
