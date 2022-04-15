package appsbymatthew

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import sparkManager.{spark}

object mongoHelpers {

  val readConfigTraffic = ReadConfig(Map(
    "collection" -> config.inputCollection,
    "readPreference.type" -> "secondaryPreferred"
  ), Some(ReadConfig(spark.sparkContext)))

  def writeToMongo(df: DataFrame): Unit = {
    MongoSpark.save(df)
  }

  def readFromMongo(): DataFrame = {
    val df = MongoSpark.load(spark.sparkContext, readConfigTraffic).toDF()
    df
  }

}