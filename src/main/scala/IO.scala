import org.apache.log4j.Logger
import org.apache.spark
import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object IO {

      case class Record(time: Long, bid_volumes: List[Long], ask_volumes: List[Long])


      def jsonToDataFrame(spark: SparkSession, path: String): DataFrame = {
        @transient lazy val logger = Logger.getLogger(getClass.getName)
        logger.info("Reading JSON File..")
        import spark.implicits._

        val df = spark.read.json(path)
        val dataDF = df.select($"time",
          $"Bid.volume".getItem(0).as("bid_v1"),
          $"Bid.volume".getItem(1).as("bid_v2"),
          $"Bid.volume".getItem(2).as("bid_v3"),
          $"Bid.volume".getItem(3).as("bid_v4"),
          $"Ask.volume".getItem(0).as("ask_v1"),
          $"Ask.volume".getItem(1).as("ask_v2"),
          $"Ask.volume".getItem(2).as("ask_v3"),
          $"Ask.volume".getItem(3).as("ask_v4")
        )
         .dropDuplicates(Seq("time"))
         .filter($"bid_v1".isNotNull)
        dataDF
      }

    def jsonToDataFrame2(spark: SparkSession, path: String): Dataset[Record] = {
      @transient lazy val logger = Logger.getLogger(getClass.getName)
      logger.info("Reading JSON File..")
      import spark.implicits._

      val timeFilterWindow = Window.partitionBy("time").orderBy("time")

      val df = spark.read.json(path)
      val dataDF = df
        .withColumn("ts_rank", row_number().over(timeFilterWindow))
        .where($"ts_rank" === 1)
        .drop("ts_rank")

        .select($"time", $"Bid.volume".as("bid_volumes"), $"Ask.volume".as("ask_volumes"))
        .filter($"bid_volumes".getItem(0).isNotNull)
      .as[Record]
      dataDF
    }

  case class Record2(time: Long, volumes: Array[Column])

  def jsonToDataFrame3(spark: SparkSession, path: String): DataFrame = {
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
        .orderBy("time")
        .filter($"bid_volumes".isNotNull && $"ask_volumes".isNotNull)

    dataDF
  }
}
