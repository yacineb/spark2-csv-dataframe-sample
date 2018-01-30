package schemas
import org.apache.spark.sql.types.StructType

package object csv {
  def InputData: StructType = {
    import org.apache.spark.sql.types._
    val customSchema = StructType(Array(
      StructField("userid",    StringType, false),
      StructField("itemid",    StringType, false),
      StructField("rating",    DoubleType, false),
      StructField("timestamp", LongType, false)))
    customSchema
  }
}
