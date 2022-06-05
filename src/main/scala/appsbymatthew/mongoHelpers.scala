package appsbymatthew

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, SaveMode, SparkSession}
import sparkManager.spark
import config._

object mongoHelpers {

  def writeToMongo(df: DataFrame, databaseName: String, collectionName: String, saveMode: SaveMode): Unit = {
    val writer: DataFrameWriter[org.apache.spark.sql.Row] = df.repartition(2).write.mode(saveMode)
    val writeConfig = WriteConfig(Map(
      "uri" -> mongoDbUri,
      "database" -> databaseName,
      "collection" -> collectionName
    ))
    MongoSpark.save(writer, writeConfig)
  }

  def readFromMongo(databaseName: String, collectionName: String): DataFrame = {
    val readConfig = ReadConfig(Map(
      "uri" -> mongoDbUri,
      "database" -> databaseName,
      "collection" -> collectionName
    ))
    val df = MongoSpark.load(spark.sparkContext, readConfig).toDF()
    df
  }

}