package appsbymatthew

import org.apache.spark.sql.SparkSession
import config.environment

object sparkManager {

  val spark = sparkManager.launchSpark()

  def launchSpark(): SparkSession = {
    val sparkBuilder = SparkSession.builder().appName("AppsByMatthew_Spark")
    if (environment == "local") {
      sparkBuilder.master("local")
      sparkBuilder.config("spark.driver.bindAddress", "localhost")
    }
    sparkBuilder.config("spark.sql.shuffle.partitions", "100")
    sparkBuilder.config("spark.default.parallelism", "100")
    val spark = sparkBuilder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    authenticateForS3(spark)
    spark
  }

  def authenticateForS3(spark: SparkSession): Unit = {
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", scala.util.Properties.envOrElse("AWS_ACCESS_KEY_ID", ""))
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", scala.util.Properties.envOrElse("AWS_SECRET_ACCESS_KEY", ""))
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.endpoint", "s3.amazonaws.com")
  }

}