package appsbymatthew

import org.apache.spark.sql.{SaveMode}
import sparkManager.{spark}
import utils.{logItem}
import config._

object app {

  def main(args: Array[String]): Unit = {

    logItem("Loading traffic data from Mongo.")
    val trafficDf = mongoHelpers.readFromMongo(abmDatabase, userActionsCollection)

    logItem("Restructuring traffic data into report.")
    val restructuredTrafficDf = utils.restructureTrafficDf(trafficDf)

    logItem("Writing data to S3.")
    s3Helpers.writeToS3(restructuredTrafficDf, SaveMode.Overwrite)

    logItem("Reading data from parquet files in S3.")
    val reReadDf = s3Helpers.readFromS3(spark)

    logItem("Saving aggregated data to Mongo.")
    mongoHelpers.writeToMongo(reReadDf, abmDatabase, trafficReportsCollection, SaveMode.Overwrite)

    logItem("Job complete.")
    spark.stop()
  }

}