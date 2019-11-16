package org.tensor.challenge

import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, rank, size}
import org.apache.spark.sql.{DataFrame, SparkSession}

object IO {

  def jsonToDataFrame(spark: SparkSession, path: String): DataFrame = {
    @transient lazy val logger = Logger.getLogger(getClass.getName)
    logger.info("Reading JSON File..")
    import spark.implicits._

    val timeFilterWindow = Window.partitionBy("time").orderBy(desc("record_size"))

    val df = spark.read.json(path)

    // Clean up data to keep most complete record per timestamp
    val dataDF = df
        .withColumn("record_size", size($"Bid.volume") + size($"Ask.volume"))
        .withColumn("completeness_rank", rank().over(timeFilterWindow))
        .filter($"completeness_rank"===1)
        .select($"time", $"Bid.volume".as("bid_volumes"), $"Ask.volume".as("ask_volumes"))
        .dropDuplicates("time")
        .filter($"bid_volumes".isNotNull && $"ask_volumes".isNotNull)
    dataDF.persist()
  }
}
