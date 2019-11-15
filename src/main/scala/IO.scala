import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object IO {

      case class Record(time: Long, bid_volumes: List[Long], ask_volumes: List[Long])

  def jsonToDataFrame(spark: SparkSession, path: String): DataFrame = {
    @transient lazy val logger = Logger.getLogger(getClass.getName)
    logger.info("Reading JSON File..")
    import spark.implicits._

    val timeFilterWindow = Window.partitionBy("time").orderBy(desc("record_size"))

    val df = spark.read.json(path)
    val dataDF = df
        .withColumn("record_size", size($"Bid.volume") + size($"Ask.volume"))
        .withColumn("completeness_rank", rank().over(timeFilterWindow))
        .filter($"completeness_rank"===1)
        .select($"time", $"Bid.volume".as("bid_volumes"), $"Ask.volume".as("ask_volumes"))
        .dropDuplicates("time")
        .filter($"bid_volumes".isNotNull && $"ask_volumes".isNotNull)
    dataDF
  }
}
