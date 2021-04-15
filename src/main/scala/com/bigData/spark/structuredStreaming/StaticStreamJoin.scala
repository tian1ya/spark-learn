package com.bigData.spark.structuredStreaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class DeviceData34(device: String, deviceType: String, signal: Double, time: Long)

case class DeviceData42(device: String, deviceType: String, age: Long)

object StaticStreamJoin extends App {

  /*
      b bbb 3
      b bbb 8
      b aaa 6
      b ccc 5
   */
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


  private val deviceStatic: Dataset[DeviceData3] = sc
    .parallelize(Seq(("b", "bbb", 233, 1547718101),
      ("b", "aaa", 233, 1547718100),
      ("b", "ccc", 233, 1547718102),
      ("b", "ddd", 233, 1547718104))
    ).map(linesData => {
    DeviceData3(linesData._1, linesData._2, linesData._3, linesData._4.toLong)
  }).toDS()


  val deviceStream: Dataset[DeviceData4] = source.as[String]
    .map(line => {
      val linesData = line.split(" ")
      DeviceData4(linesData(0), linesData(1), linesData(2).toLong)
    })


  //
  private val StreamingStaticDFjoin: DataFrame = deviceStream.join(deviceStatic, Seq("deviceType"), "leftouter")


  val query: StreamingQuery = StreamingStaticDFjoin
    .writeStream
    .outputMode(OutputMode.Append())
    .format("console")
    .start()
  /*
      Complete 不支持
      Append join 到谁，输出谁, 重复 key join 保持更新旧值
      Update：join 到谁，输出谁，重复 key join 保持更新旧值
   */
  query.awaitTermination()

}
