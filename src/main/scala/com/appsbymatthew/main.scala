package com.appsbymatthew

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object main extends App {

  val inputUri = scala.util.Properties.envOrElse("INPUT_DB_URI", "mongodb://localhost/appsbymatthew_dev.useractions")
  val outputUri = scala.util.Properties.envOrElse("OUTPUT_DB_URI", "mongodb://localhost/appsbymatthew_dev.traffic_reports")
  val inputCollection = scala.util.Properties.envOrElse("INPUT_COLLECTION", "useractions")

  val spark = SparkSession.builder()
    .master("local")
    .appName("TrafficMongoSpark")
    .config("spark.mongodb.input.uri", inputUri)
    .config("spark.mongodb.output.uri", outputUri)
    .getOrCreate()
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", scala.util.Properties.envOrElse("AWS_ACCESS_KEY_ID", ""))
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", scala.util.Properties.envOrElse("AWS_SECRET_ACCESS_KEY", ""))
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.endpoint", "s3.amazonaws.com")

  import spark.implicits._

  val readConfigTraffic = ReadConfig(Map("collection" -> inputCollection, "readPreference.type" -> "secondaryPreferred"), Some(ReadConfig(spark.sparkContext)))
  val trafficRdd = MongoSpark.load(spark.sparkContext, readConfigTraffic)
  val trafficDfProto = trafficRdd.toDF()
  trafficDfProto.show()
  val trafficDf = trafficRdd.toDF()
    .filter("timestamp > DATE(NOW() - INTERVAL 7 DAY)")
    .groupBy($"ip_address", $"browser", $"sublocation", $"description", $"operation")
    .count()
    .agg(
      sum("count") as "all_interactions_count",
      collect_set("ip_address") as "unique_users",
      collect_set("browser") as "unique_browsers",
      collect_set("sublocation") as "unique_sublocations",
      collect_set("description") as "unique_descriptions",
      collect_set("operation") as "unique_operations",
      collect_list("ip_address") as "users",
      collect_list("browser") as "browsers"
    )
    .withColumn("unique_visitor_count", size($"unique_users"))
    .withColumn("unique_visitor_count", size($"unique_users"))
    .withColumn("run_date", current_timestamp())
    .withColumn("report_start_date", date_add(current_timestamp(), -7))
    .withColumn("report_end_date", current_timestamp())

  trafficDf.show()
  trafficDf.write.parquet("s3n://mfarmer5102-spark/AppsByMatthew/")
  MongoSpark.save(trafficDf)

  spark.stop()

}
