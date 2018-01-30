package io

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

class CsvLoader(val session: SparkSession) {
  def loadWithSchema(filePath: String, schema: StructType): DataFrame = {
    val loadedDF = session.read.format("csv")
                               .option("header", "false")
                               .option("delimiter", ",")
                               .schema(schema)
                               .load(filePath)
    loadedDF
  }
}