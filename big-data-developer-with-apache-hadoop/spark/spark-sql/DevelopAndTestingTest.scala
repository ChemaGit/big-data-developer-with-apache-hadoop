import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite
import org.scalatest.Matchers

import spark_fundamentals_II.DevelopAndTesting

class DevelopAndTestingTest extends FunSuite with SharedSparkContext with Matchers {
  test("distance calculation") {
    val start = (42.178, -82.556)
    val end = (42.229 , -82.5528)

    val expected = 3.542615227517951

    DevelopAndTesting.distanceOf(start._1,end._1,start._2,end._2) should be(expected)
  }

  test("join trips and stations and calculate distance") {
  }
}