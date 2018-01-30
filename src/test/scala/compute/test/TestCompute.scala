package compute.test

import compute._
import org.scalatest.FunSuite

class TestCompute extends FunSuite with SparkSessionTestWrapper {
  import spark.implicits._
  val df = Seq(("a","p_500315456",5,1476686549844L),
    ("b","p_504922691",3,1476686550002L),
    ("c","p_500905401",1,1476686550033L),
    ("d","p_500416355",2,1476686550097L),
    ("d","p_500416355",2,1476686550131L)).toDF("userid",
    "itemid",
    "rating",
    "timestamp")

  test("full chain aggregation test"){
    val lookupP = Main.Compute(spark, df)
    lookupP._1.show()
    lookupP._2.show()
    lookupP._3.show()

    //my bad! asserts are missing, i was not sure about my choice for the best DataFrameComparison library..
  }
}
