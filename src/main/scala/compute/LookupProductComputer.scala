package compute
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.{DataFrame, SparkSession}

class LookupProductComputer(val session: SparkSession) {
  def compute(df:DataFrame) : DataFrame = {
    import session.implicits._
    val w = Window.orderBy($"itemId")
    df.select($"itemId").distinct().select($"itemId", (row_number.over(w)-1).alias("itemIdAsInteger"))
  }
}

