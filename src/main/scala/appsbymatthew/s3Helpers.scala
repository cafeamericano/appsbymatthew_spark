package appsbymatthew

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object s3Helpers {

  def writeToS3(df: DataFrame, saveMode: SaveMode): Unit = {
    df.repartition(5).write.mode(saveMode).parquet("s3n://mfarmer5102-spark/AppsByMatthew/")
  }

  def readFromS3(spark: SparkSession): DataFrame = {
    val df = spark.read.parquet("s3n://mfarmer5102-spark/AppsByMatthew/")
    df
  }

}