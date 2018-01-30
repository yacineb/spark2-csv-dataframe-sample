package compute

import io._
import org.apache.spark.sql._

object Main {
  val filePath = "data/test.csv"

  def main(args: Array[String]): Unit = {
    val session: SparkSession  = buildSession

    FileUtils.download("https://storage.googleapis.com/eb-dumps/xag.csv", filePath)
    val df = new CsvLoader(session).loadWithSchema(filePath, schemas.csv.InputData)

    val (lookupProduct: DataFrame, lookupUser: DataFrame, aggregate: DataFrame) = Compute(session, df)

    FileUtils.dumpCsv(lookupProduct,"data/results/lookup_product.csv")
    FileUtils.dumpCsv(lookupUser, "data/results/lookupuser.csv")
    FileUtils.dumpCsv(aggregate, "data/results/aggratings.csv")
  }

  def Compute(session: SparkSession, df: DataFrame): (DataFrame, DataFrame, DataFrame) = {
    val lookupProduct = new LookupProductComputer(session).compute(df)
    val lookupUser = new LookupUserComputer(session).compute(df)
    val aggregate = new AggregatesComputer(session).compute(lookupProduct, lookupUser, df)
    (lookupProduct, lookupUser, aggregate)
  }

  def buildSession: SparkSession = {
    //System.setProperty("hadoop.home.dir", "d:\\hadoop");

    val sparkSession = SparkSession.builder
                                   .master("local[*]")
                                   .appName("csvProcess")
                                   .config("spark.sql.warehouse.dir", "data")
                                   .getOrCreate()
    sparkSession
  }
}