package compute
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

class LookupUserComputer(val session: SparkSession) {
  def compute(df:DataFrame) : DataFrame = {
    import session.implicits._
    val w = Window.orderBy($"userid")
    df.select($"userid").distinct().select($"userid", (row_number.over(w) - 1).alias("userIdAsInteger"))
  }
}