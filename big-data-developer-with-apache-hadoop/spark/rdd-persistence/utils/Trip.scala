package spark.rdd-persistence.utils

import java.text.SimpleDateFormat
import java.sql.Timestamp


case class Trip(
                 id: Int,
                 duration: Int,
                 startDate: Timestamp,
                 startStation: String,
                 startTerminal: Int,
                 endDate: Timestamp,
                 endStation: String,
                 endTerminal: Int,
                 bike: Int,
                 subscriberType: String,
                 zipCode: Option[String]
               )

object Trip {
  def parse(i: Array[String]): Trip = {
    val fmt = new SimpleDateFormat("M/d/yyyy HH:mm")

    val zip = i.length  match { // zip is optional
      case 11 => Some(i(10))
      case _ => None
    }

    Trip(i(0).toInt,i(1).toInt,new Timestamp(fmt.parse(i(2)).getTime),i(3),i(4).toInt,new Timestamp(fmt.parse(i(5)).getTime),i(6),i(7).toInt,i(8).toInt,i(9),zip)

  }
}