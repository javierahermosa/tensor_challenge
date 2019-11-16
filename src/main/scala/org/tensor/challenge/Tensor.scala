package org.tensor.challenge

import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

class Tensor(resetTicks: List[Int], resetTimes: List[Double]) extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def resetSumUDF: UserDefinedFunction = udf(
    (tick: Long, time: Double, time_prev: Double, time_post: Double) => {
      val tick_post = tick + 1L
      (tick, time, tick_post) match {
        case (x, _, _) if resetTicks.contains(x) => "reset_tick"
        case (_, y, _) if resetTimes.exists(rt => (y >= rt) && (time_prev < rt)) => "reset_time"
        case (_, _, z) if resetTicks.contains(z) => "report_tick_reset"
        case (_, y, _) if resetTimes.exists(rt => (y < rt) && (time_post > rt)) => "report_time_reset"
        case (_,_, _) => ""
      }
  })
}

object Tensor extends Serializable {

  val startTime: Long = System.nanoTime
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("tensor_challenge")
      .getOrCreate()

    val path = "src/main/resources/challenge.json"
    val data = IO.jsonToDataFrame(spark, path)

    val hls = Array(1e6, 1e9, 1e12)
    val resetTicks = List(1000, 1000000)
    val resetTimes = List(1, 60, 3600).map(_ * 1e9) // seconds to ns

    import spark.implicits._
    val decayedSum = new CalculateDecayedSum
    val tensor = new Tensor(resetTicks, resetTimes)

    val Row(minTime: Long) = data.agg(min("time")).head
    val w = Window.orderBy("time")
    val w_res = Window.partitionBy("half_life").orderBy("time")

    val results = data
      .withColumn("tick", row_number().over(w))
      .withColumn("time_prev", lag("time", 1, minTime).over(w))
      .withColumn("time_delta_ns", (col("time") - col("time_prev")))
      .withColumn("time_elapsed_ns", (col("time") - minTime))
      .withColumn("time_elapsed_ns_prev", lag("time_elapsed_ns", 1).over(w))
      .withColumn("time_elapsed_ns_post", lead("time_elapsed_ns", 1).over(w))
      .withColumn("status", tensor.resetSumUDF($"tick", $"time_elapsed_ns", $"time_elapsed_ns_prev", $"time_elapsed_ns_post"))
      .withColumn("half_life", explode(lit(hls)))
      .withColumn("result", decayedSum($"time_delta_ns", $"bid_volumes", $"ask_volumes", $"half_life", $"status").over(w_res))
      .select($"time", $"tick", $"half_life", $"bid_volumes", $"ask_volumes", $"time_elapsed_ns",
              $"result.bid_decayed_sums", $"result.ask_decayed_sums", $"status")
      .filter($"status".contains("report"))

    results.show(10, false)

    val endTime = (System.nanoTime - startTime)/1e9
    logger.info("Code ran in %1.5f seconds.".format(endTime))

    spark.stop()
  }
}