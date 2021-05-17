//import com.mongodb.client.{MongoClient, MongoClients}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.sql.{DataFrameWriter, SparkSession}

object Main extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("AppsByMatthewSpark")
    .config("spark.mongodb.input.uri", "mongodb://localhost/abm_spark.user_actions_input")
    .config("spark.mongodb.output.uri", "mongodb://localhost/abm_spark.user_actions_output")
    .getOrCreate()

  import spark.implicits._

  val readConfig = ReadConfig(Map("collection" -> "user_actions_input", "readPreference.type" -> "secondaryPreferred"), Some(ReadConfig(spark.sparkContext)))
  val customRdd = MongoSpark.load(spark.sparkContext, readConfig)

  MongoSpark.save(customRdd)

  spark.stop()
}