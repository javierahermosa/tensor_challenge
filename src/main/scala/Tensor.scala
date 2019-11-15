import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._

class Tensor(resetTicks: List[Int], resetTimes: List[Int]) extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def say_hello(arg: String): Unit = {
    logger.info("Hello World!")
    logger.info("Hello " + arg)
  }

  def resetSum: UserDefinedFunction = udf(
    (tick: Long, tick_post: Long, dt: Double, dt_prev: Double, dt_post: Double) => {
    (tick, dt) match {
      case (x, _) if resetTicks.contains(x) => "reset_tick"
      case (_, y) if resetTimes.exists(rt => (y >= rt) && (dt_prev < rt)) => "reset_time"
      case (_, y) if resetTicks.contains(tick_post) |  resetTimes.exists(rt => (y < rt) && (dt_post > rt)) => "report"
      case (x, _) if x == 1.0 => ""
      case (_,_) => ""
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
    val data2 = IO.jsonToDataFrame3(spark, path)

    val hls = Array(1e6/1e9, 1e9/1e9, 1e12/1e9)
    val resetTicks = List(1000, 1000000)
    val resetTimes = List(1, 60, 3600)

    val Row(minTime: Long) = data.agg(min("time")).head

    import spark.implicits._
    val w = Window.orderBy("time")
    val w_res = Window.partitionBy("half_life").orderBy("time")

    val calculateDecayedSum = new CalculateDecayedSum(hls)
    val calculateDecayedSum2 = new CalculateDecayedSum2
    val tensor = new Tensor(resetTicks, resetTimes)

    val dataPrep = data
      .withColumn("tick", row_number().over(w))
      .withColumn("time_prev", lag("time", 1, minTime).over(w))
      .withColumn("time_delta_seconds", (col("time") - col("time_prev"))/1e9)
      //.withColumn("time_elapsed_seconds", sum("time_delta_seconds").over(w))
      .withColumn("time_elapsed_seconds", (col("time") - minTime)/1e9)
      .withColumn("time_elapsed_seconds_prev", lag("time_elapsed_seconds", 1).over(w))
      .withColumn("time_elapsed_seconds_post", lead("time_elapsed_seconds", 1).over(w))
      .withColumn("tick_post", lead("tick", 1).over(w))
      .withColumn("status", tensor.resetSum($"tick", $"tick_post", $"time_elapsed_seconds", $"time_elapsed_seconds_prev", $"time_elapsed_seconds_post"))
      //.select($"time_elapsed_seconds", $"bid_volumes", $"ask_volumes", $"status")
      .withColumn("v1_sum", calculateDecayedSum($"time_delta_seconds", $"bid_v1", $"status").over(w))
      .select("v1_sum")
      .orderBy($"time_elapsed_seconds")
      .filter($"status"==="report")
      //.withColumn("volume1_prev", lag("bid_v1", 1).over(w))

    val dataPrep2 = data2
      .withColumn("tick", row_number().over(w))
      .withColumn("time_prev", lag("time", 1, minTime).over(w))
      .withColumn("time_delta_seconds", (col("time") - col("time_prev"))/1e9)
      //.withColumn("time_elapsed_seconds", sum("time_delta_seconds").over(w))
      .withColumn("time_elapsed_seconds", (col("time") - minTime)/1e9)
      .withColumn("time_elapsed_seconds_prev", lag("time_elapsed_seconds", 1).over(w))
      .withColumn("time_elapsed_seconds_post", lead("time_elapsed_seconds", 1).over(w))
      .withColumn("tick_post", lead("tick", 1).over(w))
      .withColumn("status", tensor.resetSum($"tick", $"tick_post", $"time_elapsed_seconds", $"time_elapsed_seconds_prev", $"time_elapsed_seconds_post"))
      .withColumn("hls", (lit(hls)))
      .withColumn("half_life", explode($"hls"))
      .withColumn("result", calculateDecayedSum2($"time_delta_seconds", $"bid_volumes", $"ask_volumes", $"half_life", $"status").over(w_res))
      .select($"time", $"tick", $"half_life", $"bid_volumes", $"ask_volumes", $"time_elapsed_seconds", $"result.bid_decayed_sums", $"result.ask_decayed_sums")
    dataPrep2.show(100, false)
    //dataPrep.show(100, false)
    //data.orderBy($"time").show(100, false)


    //val d3 = IO.jsonToDataFrame3(spark, path)
    //d3.show(100, false)
    //println(d3.count)
    sc.stop()
  }
}