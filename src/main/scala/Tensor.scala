import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._

class Tensor(resetTicks: List[Int], resetTimes: List[Double]) extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def say_hello(arg: String): Unit = {
    logger.info("Hello World!")
    logger.info("Hello " + arg)
  }

  def resetSum: UserDefinedFunction = udf(
    (tick: Long, tick_post: Long, dt: Double, dt_prev: Double, dt_post: Double) => {
    (tick, dt, tick_post) match {
      case (x, _, _) if resetTicks.contains(x) => "reset_tick"
      case (_, y, _) if resetTimes.exists(rt => (y >= rt) && (dt_prev < rt)) => "reset_time"
      case (_, _, z) if resetTicks.contains(z) => "report_tick_reset"
      case (_, y, _) if resetTimes.exists(rt => (y < rt) && (dt_post > rt)) => "report_time_reset"
      case (x, _, _) if x == 1.0 => ""
      case (_,_, _) => ""
    }
  })
}

object Tensor extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("Simple Application")
      .getOrCreate()
    val sc = spark.sparkContext

    val path = "src/main/resources/challenge.json"
    val data = IO.jsonToDataFrame(spark, path)

    val hls = Array(1e6, 1e9, 1e12)
    val resetTicks = List(1000, 1000000)
    val resetTimes = List(1, 60, 3600).map(_ * 1e9) // seconds to ns

    val Row(minTime: Long) = data.agg(min("time")).head

    import spark.implicits._
    val w = Window.orderBy("time")
    val w_res = Window.partitionBy("half_life").orderBy("time")

    val decayedSum = new CalculateDecayedSum
    val tensor = new Tensor(resetTicks, resetTimes)

    val results = data
      .withColumn("tick", row_number().over(w))
      .withColumn("time_prev", lag("time", 1, minTime).over(w))
      .withColumn("time_delta_ns", (col("time") - col("time_prev")))
      .withColumn("time_elapsed_ns", (col("time") - minTime))
      .withColumn("time_elapsed_ns_prev", lag("time_elapsed_ns", 1).over(w))
      .withColumn("time_elapsed_ns_post", lead("time_elapsed_ns", 1).over(w))
      .withColumn("tick_post", lead("tick", 1).over(w))
      .withColumn("status", tensor.resetSum($"tick", $"tick_post", $"time_elapsed_ns", $"time_elapsed_ns_prev", $"time_elapsed_ns_post"))
      .withColumn("half_life", explode(lit(hls)))
      .withColumn("result", decayedSum($"time_delta_ns", $"bid_volumes", $"ask_volumes", $"half_life", $"status").over(w_res))
      .select($"time", $"tick", $"half_life", $"bid_volumes", $"ask_volumes", $"time_elapsed_ns",
              $"result.bid_decayed_sums", $"result.ask_decayed_sums", $"status")
      .filter($"status".contains("report"))
      .orderBy($"time", $"half_life")

    results.show(100, false)
    sc.stop()
  }
}