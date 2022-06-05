package appsbymatthew

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import config.daysToCaptureBeforeCurrent

object utils {

  def logItem(text: String): Unit = {
    println(text)
  }

  def restructureTrafficDf(df: DataFrame): DataFrame = {
    df.show()
    val trafficDf = df
      .filter("timestamp > DATE(NOW() - INTERVAL " + daysToCaptureBeforeCurrent.toString() + " DAY)")
      .groupBy("ip_address", "browser", "sublocation", "description", "operation")
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
      .withColumn("unique_visitor_count", size(col("unique_users")))
      .withColumn("unique_visitor_count", size(col("unique_users")))
      .withColumn("run_date", current_timestamp())
      .withColumn("report_start_date", date_add(current_timestamp(), daysToCaptureBeforeCurrent * -1))
      .withColumn("report_end_date", current_timestamp())
    trafficDf
  }

}