import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


class CalculateDecayedSum(hl: Array[Double]) extends UserDefinedAggregateFunction{

  // Input schema for this UserDefinedAggregateFunction
  override def inputSchema: StructType =
    StructType(
        StructField("time_delta", DoubleType) ::
        StructField("volume", IntegerType) ::
        StructField("status", StringType) :: Nil)

  // Schema for the parameters that will be used internally to buffer temporary values
  override def bufferSchema: StructType = StructType(
    StructField("decayed_sum_hl_1", DoubleType)::
      StructField("decayed_sum_hl_2", DoubleType)::
      StructField("decayed_sum_hl_3", DoubleType) :: Nil
  )

  // The data type returned by this UserDefinedAggregateFunction.
  override def dataType: DataType = StructType(
    Seq(StructField("decayed_sum_hl_1", DoubleType),
        StructField("decayed_sum_hl_2", DoubleType),
        StructField("decayed_sum_hl_3", DoubleType)))

  // Whether this UDAF is deterministic or not. In this case, it is
  override def deterministic: Boolean = true

  // Initial values for the temporary values declared above
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
    buffer(1) = 0.0
    buffer(2) = 0.0
  }

  // Function to calculate the decayed sum
  private def decayedSum(etds_prev: Double, timeDelta: Double, volume:Int, hl: Double, status: String): Double = {
    val factor = math.pow(2, -1.0 * timeDelta / hl)
    if(status == "reset_time" | status == "reset_tick") {
      0.0
    } else {
      etds_prev * factor + volume
    }
  }

  // In this function, the values associated to the buffer schema are updated
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    val timeDelta = input.getDouble(0)
    val volume = if(input.isNullAt(1)) 0 else input.getInt(1)
    val status = input.getString(2)

    buffer(0) = decayedSum(buffer.getDouble(0), timeDelta, volume, hl(0) , status)
    buffer(1) = decayedSum(buffer.getDouble(1), timeDelta, volume, hl(1) , status)
    buffer(2) = decayedSum(buffer.getDouble(2), timeDelta, volume, hl(2) , status)
  }

  // Function used to merge two objects. In this case, it is not necessary to define this method since
  // the whole logic has been implemented in update
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {}

  // Function returns this
  override def evaluate(buffer: Row): Any = {
    (buffer.getDouble(0), buffer.getDouble(1), buffer.getDouble(2))
  }

}
