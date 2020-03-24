package spark.rdd-persistence.utils

import java.text.SimpleDateFormat
import java.sql.Timestamp

case class Station(
                    id: Int,
                    name: String,
                    lat: Double,
                    lon: Double,
                    docks: Int,
                    landmark: String,
                    installDate: Timestamp
                  )
object Station {
  def parse(i: Array[String]): Station = {
    val fmt = new SimpleDateFormat("M/d/yyyy")

    Station(i(0).toInt,i(1),i(2).toDouble,i(3).toDouble,i(4).toInt,i(5), new Timestamp(fmt.parse(i(6)).getTime))
  }
}