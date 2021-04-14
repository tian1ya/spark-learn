package com.bigData.spark.structuredStreaming

import com.bigData.spark.structuredStreaming.WordCount.{lines, spark}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class DeviceData(device: String, deviceType: String, signal: Double, time: Long)

object APIs extends App {


  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("appName")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._

  val source: DataFrame = spark
    .readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999).load()

  val device: Dataset[DeviceData] = source.as[String]
    .map(line => {
      val linesData = line.split(" ")
      DeviceData(linesData(0), linesData(1), linesData(2).toDouble, linesData(3).toLong)
    })

  val df: DataFrame = device.filter(a => a.signal > 10).groupBy("deviceType").count()

  val query: StreamingQuery = df
      .writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()

  query.awaitTermination()

}
