package timeusage

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import TimeUsage._

import org.scalatest.funsuite.AnyFunSuite

class TimeUsageSuite extends AnyFunSuite {

  test("row: empty list parsed correctly") {
    assert(row(List[String]()) == Row())
  }

  test("row: non-empty list parsed correctly") {
    assert(row(List[String]("string", "0", "1.0")) == Row("string", 0d, 1d))
  }

  test("classifiedColumns: columns correctly classified") {
    val (primary, working, other) = classifiedColumns(
      List(
        "t010",
        "t030",
        "t110",
        "t18010",
        "t18030",
        "t050",
        "t18050",
        "t020",
        "t040",
        "t060",
        "t070",
        "t080",
        "t090",
        "t100",
        "t120",
        "t130",
        "t140",
        "t150",
        "t160",
        "t180"
      )
    )
    assert(primary == List(col("t010"), col("t030"), col("t110"), col("t18010"), col("t18030")))
    assert(working == List(col("t050"), col("t18050")))
    assert(
      other == List(
        col("t020"),
        col("t040"),
        col("t060"),
        col("t070"),
        col("t080"),
        col("t090"),
        col("t100"),
        col("t120"),
        col("t130"),
        col("t140"),
        col("t150"),
        col("t160"),
        col("t180")
      )
    )
  }
}
