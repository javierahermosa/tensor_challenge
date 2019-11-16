package org.tensor.tests

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.scalatest.FunSpec
import org.tensor.challenge.{CalculateDecayedSum, Tensor}

class TensorTest
  extends FunSpec
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  private val resetTicks = List(1000, 1000000)
  private val resetTimes = List(1, 60, 3600).map(_ * 1e9)
  val tensor = new Tensor(resetTicks, resetTimes)
  val decayedSum = new CalculateDecayedSum

  describe("Should compute the reset and record rows") {

    it("appends the column status to the provided Dataframe") {

      val resetSumTestData = Seq(
        Row(100L, 0.9e7, 1e7, 1e8),
        Row(1000L, 0.8e9, 0.9e9, 1e9),
        Row(5000L, 59e9, 60e9, 61e9),
        Row(1000000L, 70e9, 80e9, 90e9),
        Row(1000001L, 59e9, 3600e9, 3700e9)
      )

      val resetSumTestSchema = List(
        StructField("tick", LongType),
        StructField("time", DoubleType),
        StructField("time_prev", DoubleType),
        StructField("time_post", DoubleType)
      )

      val resetSumTestDF = spark.createDataFrame(
        spark.sparkContext.parallelize(resetSumTestData),
        StructType(resetSumTestSchema))

      val expectedSchema = List(
        StructField("tick", LongType),
        StructField("time", DoubleType),
        StructField("time_prev", DoubleType),
        StructField("time_post", DoubleType),
        StructField("status", StringType)
      )

      val expectedData = Seq(
        Row(100L, 0.9e7, 1e7, 1e8, ""),
        Row(1000L, 0.8e9, 0.9e9, 1e9, "reset_tick"),
        Row(5000L, 59e9, 60e9, 61e9, "report_time_reset"),
        Row(1000000L, 70e9, 80e9, 90e9, "reset_tick"),
        Row(1000001L, 59e9, 3600e9, 3700e9, "report_time_reset")
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )
      val isReset = resetSumTestDF.withColumn("status", tensor.resetSumUDF($"tick", $"time", $"time_prev", $"time_post"))
      assertSmallDatasetEquality(isReset, expectedDF)
    }
  }

  describe("Should compute the decayed sum for a list of volumes") {

    it("appends decayed sums to dataframe") {
      val testVolumeData = Seq(
        Row(1522588865646358382L, 1e9, List(1, 10, 20, 30, 50), List(), 1e6, ""),
        Row(1522588865646359382L, 2e7, List(10, 10, 20, 60, 80), List(1, 20, 20, 30, 70), 1e6, "report_time_reset"),
        Row(1522597526724857397L, 3e9, List(1, 5, 20, 30, 70), List(10, 10, 20, 5, 50), 1e6, "reset_time")
      )

      val computeSumTestSchema = List(
        StructField("time", LongType),
        StructField("time_delta_ns", DoubleType),
        StructField("bid_volumes", ArrayType(IntegerType)),
        StructField("ask_volumes", ArrayType(IntegerType)),
        StructField("half_life", DoubleType),
        StructField("status", StringType)
      )

      val resetSumTestDF = spark.createDataFrame(
        spark.sparkContext.parallelize(testVolumeData),
        StructType(computeSumTestSchema))

      val expectedSchema = List(
        StructField("time", LongType),
        StructField("time_delta_ns", DoubleType),
        StructField("bid_volumes", ArrayType(IntegerType)),
        StructField("ask_volumes", ArrayType(IntegerType)),
        StructField("half_life", DoubleType),
        StructField("status", StringType),
        StructField("bid_decayed_sums", ArrayType(DoubleType)),
        StructField("ask_decayed_sums", ArrayType(DoubleType))
      )

      val expectedSumData = Seq(
        Row(1522588865646358382L, 1e9, List(1, 10, 20, 30, 50), List(), 1e6, "", List(1.0, 10.0, 20.0, 30.0, 50.0), List(0.0, 0.0, 0.0, 0.0, 0.0)),
        Row(1522588865646359382L, 2e7, List(10, 10, 20, 60, 80), List(1, 20, 20, 30, 70), 1e6, "report_time_reset", List(10.000000953674316, 10.000009536743164, 20.000019073486328, 60.00002861022949, 80.00004768371582), List(1.0, 20.0, 20.0, 30.0, 70.0)),
        Row(1522597526724857397L, 3e9, List(1, 5, 20, 30, 70), List(10, 10, 20, 5, 50), 1e6, "reset_time", List(0.0, 0.0, 0.0, 0.0, 0.0), List(0.0, 0.0, 0.0, 0.0, 0.0))
      )

      val expecteSumdDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedSumData),
        StructType(expectedSchema)
      )

      val w_res = Window.partitionBy("half_life").orderBy("time")

      val isSum = resetSumTestDF
        .withColumn("result", decayedSum($"time_delta_ns", $"bid_volumes", $"ask_volumes", $"half_life", $"status").over(w_res))
        .select($"time", $"time_delta_ns", $"bid_volumes", $"ask_volumes",  $"half_life",$"status",
          $"result.bid_decayed_sums", $"result.ask_decayed_sums")
      assertSmallDatasetEquality(isSum, expecteSumdDF)
    }
  }
}