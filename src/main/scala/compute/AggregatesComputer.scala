package compute

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

class AggregatesComputer(val session: SparkSession) {
  final val penalityFactor: Double = 0.95

  def compute(lp:DataFrame,lu:DataFrame,rawData:DataFrame) : DataFrame = {
      val maxTs = computeMaxTs(rawData)

      lp.createTempView("lookup_product")
      lu.createTempView("lookup_user")
      rawData.createTempView("rawData")

      val df = session.sql(s"select lookup_product.itemIdAsInteger, lookup_user.userIdAsInteger,(rawData.rating * pow($penalityFactor, $maxTs-timestamp)) as penalizedRating" +
        "                   FROM rawData INNER JOIN lookup_user ON lookup_user.userid=rawData.userid" +
        "                   INNER JOIN lookup_product ON lookup_product.itemid=rawData.itemid")

      import session.implicits._
      import org.apache.spark.sql.functions.sum
      df.filter($"penalizedRating" > 0.01).groupBy($"userIdAsInteger", $"itemIdAsInteger").agg(sum($"penalizedRating").as("ratingSum"))
    }

  def computeMaxTs(df:DataFrame) : Long = {
    import session.implicits._
    import org.apache.spark.sql.functions.max
    df.agg(max($"timestamp")).collect()(0).getLong(0)
   }
}


