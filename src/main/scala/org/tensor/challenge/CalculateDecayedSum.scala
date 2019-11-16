package org.tensor.challenge

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class CalculateDecayedSum extends UserDefinedAggregateFunction{

  // Input schema for this UserDefinedAggregateFunction
  override def inputSchema: StructType =
    StructType(
      StructField("time_delta", DoubleType) ::
        StructField("bid_volumes", ArrayType(IntegerType)) ::
        StructField("ask_volumes", ArrayType(IntegerType)) ::
        StructField("half_life", DoubleType) ::
        StructField("status", StringType) :: Nil)

  // Schema for the parameters that will be used internally to buffer temporary values
  override def bufferSchema: StructType = StructType(
    StructField("bid_decayed_sums", ArrayType(DoubleType)) ::
      StructField("ask_decayed_sums", ArrayType(DoubleType)):: Nil
  )

  // The data type returned by this UserDefinedAggregateFunction.
  override def dataType: DataType = StructType(
    Seq(StructField("bid_decayed_sums", ArrayType(DoubleType)),
        StructField("ask_decayed_sums", ArrayType(DoubleType))))

  // Whether this UDAF is deterministic or not. In this case, it is
  override def deterministic: Boolean = true

  // Initial values for the temporary values declared in buffer schema
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Seq(0.0, 0.0, 0.0, 0.0, 0.0)
    buffer(1) = Seq(0.0, 0.0, 0.0, 0.0, 0.0)
  }

  // Function to calculate the decayed sum
  private def decayedSum(etds_prev: Seq[Double], timeDelta: Double, volumes:Seq[Int], hl: Double, status: String): Seq[Double] = {
    val factor = math.pow(2, -1.0 * timeDelta / hl)
    volumes.zip(etds_prev).map{ case (volume, etds_prev) =>
      if(status == "reset_time" | status == "reset_tick") {
        0.0
      } else {
        etds_prev * factor + volume
      }
    }
  }

  // In this function, the values associated to the buffer schema are updated
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    val timeDelta = input.getDouble(0)
    val bid_volumes = input.getAs[Seq[Int]](1)
    val ask_volumes = if (input.getAs[Seq[Int]](2).isEmpty) Seq(0, 0, 0, 0, 0) else input.getAs[Seq[Int]](2)
    val hl = input.getDouble(3)
    val status = input.getString(4)
    val etds_bid_prev = buffer.getAs[Seq[Double]](0)
    val etds_ask_prev = buffer.getAs[Seq[Double]](1)

    buffer(0) = decayedSum(etds_bid_prev, timeDelta, bid_volumes, hl, status)
    buffer(1) = decayedSum(etds_ask_prev, timeDelta, ask_volumes, hl, status)
  }

  // Function used to merge two objects. In this case, it is not necessary to define this method since
  // the whole logic has been implemented in update
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {}

  // Function returns this
  override def evaluate(buffer: Row): Any = {
    (buffer.getList(0), buffer.getList(1))
  }
}
