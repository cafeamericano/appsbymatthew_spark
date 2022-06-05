package appsbymatthew

object config {

  // Env vars
  val environment = scala.util.Properties.envOrElse("ENVIRONMENT", "local")
  val mongoDbUri = scala.util.Properties.envOrElse("MONGO_URI", "mongodb://192.168.86.41:27017")
  val abmDatabase = scala.util.Properties.envOrElse("ABM_DATABASE", "AppsByMatthew")
  val s3Path = scala.util.Properties.envOrElse("S3_PATH", "/Users/matthewfarmer/Desktop/localParquet")

  // Fixed variables
  val daysToCaptureBeforeCurrent = 30
  val s3PartitionCount = 5
  val userActionsCollection = "useractions"
  val trafficReportsCollection = "traffic_reports"

}