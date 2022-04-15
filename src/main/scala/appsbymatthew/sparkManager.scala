package appsbymatthew

import org.apache.spark.sql.{SparkSession}

object sparkManager {

  val spark = sparkManager.launchSpark()

  def launchSpark(): SparkSession = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("TrafficMongoSpark")
      .config("spark.mongodb.input.uri", config.inputUri)
      .config("spark.mongodb.output.uri", config.outputUri)
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", scala.util.Properties.envOrElse("AWS_ACCESS_KEY_ID", ""))
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", scala.util.Properties.envOrElse("AWS_SECRET_ACCESS_KEY", ""))
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.endpoint", "s3.amazonaws.com")
    spark
  }

}