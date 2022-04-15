package appsbymatthew

object config {
  val inputUri = scala.util.Properties.envOrElse("INPUT_DB_URI", "")
  val outputUri = scala.util.Properties.envOrElse("OUTPUT_DB_URI", "")
  val inputCollection = scala.util.Properties.envOrElse("INPUT_COLLECTION", "")
  val s3Path = scala.util.Properties.envOrElse("S3_PATH", "")
  val s3PartitionCount = 5
}