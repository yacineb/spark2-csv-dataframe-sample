package io

import org.apache.spark.sql.DataFrame

object FileUtils {
  def download(url: String, filename: String): Unit = {
    import sys.process._
    import java.net.URL
    import java.io.File
    new URL(url) #> new File(filename) !!
  }

  def dumpCsv(df : DataFrame, path: String):Unit={
    df.coalesce(1).write.csv(path)
  }
}
