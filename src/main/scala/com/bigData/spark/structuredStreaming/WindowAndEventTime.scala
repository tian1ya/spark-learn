package com.bigData.spark.structuredStreaming

import org.apache.commons.lang.time.FastDateFormat
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{current_timestamp, explode, window}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

import java.sql.Timestamp


case class DeviceData1(device: String, deviceType: String, signal: Double, timestamp: Timestamp)

case class DeviceData2(windowStartTimeStr: String, windowEndTimeStr: String, deviceType: String, count: Long)

/*
    b bbb 233 1547718100
    b bbb 233 1547718101
    b bbb 233 1547718102
    b bbb 233 1547718104
    b bbb 233 1547718105

    b bbb 233 1547718106
    b bbb 233 1547718103
    b bbb 233 1547718108

 */

object WindowAndEventTime extends App {

  val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

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


  val device: Dataset[DeviceData1] = source.as[String]
    .map(line => {
      val linesData = line.split(" ")
      DeviceData1(linesData(0), linesData(1), linesData(2).toDouble, new Timestamp(1000L * linesData(3).toLong))
    })

  /*
      device.printSchema()
      root
       |-- device: string (nullable = true)
       |-- deviceType: string (nullable = true)
       |-- signal: double (nullable = false)
       |-- timestamp: timestamp (nullable = true)
   */

  val df: DataFrame = device
    .withWatermark("timestamp", "2 second")
    .groupBy(
      window($"timestamp", "5 second"), // 滚动窗口 5s 时间
      $"deviceType"
    ).count()

  /*
      df.printSchema()
      root
       |-- window: struct (nullable = false)
       |    |-- start: timestamp (nullable = true)
       |    |-- end: timestamp (nullable = true)
       |-- deviceType: string (nullable = true)
       |-- count: long (nullable = false)
   */

  val dff: DataFrame = df.select(
    $"deviceType",
    $"count",
    $"window".getField("start").as("windowStartTime"),
    $"window".getField("end").as("windowEndTime")
  ).map(row => DeviceData2(
    dateFormat.format(row.get(2)),
    dateFormat.format(row.get(3)),
    row.getAs[String](0), row.getAs[Long](1)
  )).toDF()

  dff.printSchema()
  /*
      root
     |-- deviceType: string (nullable = true)
     |-- count: long (nullable = false)
     |-- windowStartTime: timestamp (nullable = true)
     |-- windowEndTime: timestamp (nullable = true)
   */

  val query = dff
    .writeStream
    .outputMode(OutputMode.Complete())
    // 切换 结果输出方式  Append 或者 Update 模式，看看不一致情况
    .format("console")
    .start()

  query.awaitTermination()

}

