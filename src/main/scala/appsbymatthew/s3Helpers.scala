package appsbymatthew

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object s3Helpers {

  def writeToS3(df: DataFrame, saveMode: SaveMode): Unit = {
    df.repartition(config.s3PartitionCount).write.mode(saveMode).parquet(config.s3Path)
  }

  def readFromS3(spark: SparkSession): DataFrame = {
    val df = spark.read.parquet(config.s3Path)
    df
  }

}