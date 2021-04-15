package com.bigData.spark.structuredStreaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class DeviceData3(device: String, deviceType: String, signal: Double, time: Long)

case class DeviceData4(device: String, deviceType: String, age: Long)

object StreamStreamJoin extends App {

  /*
      去重，和 df api 接口是一样的，只不过在去重的时候如何保持过去数据，然后在保持的过去数据中去重
      1. 使用 .withWatermark("eventTime", "10 seconds")， 指定了一个延迟时间，表明重复的数据会在10s后到达，那么持有现在10s 的数据
         等重复数据来了，就去重，那么会在延迟时间这个范围内的df 进行去重，如 当前时间是 3分40秒，那么现在的 watermark 就是 3分30秒，
         而state 中持有的数据就是 最近10s 的数据组成一个 bound 数据
         然后在这个数据中去重

      2. 没有 watermark 配合使用，
          streamingDf.dropDuplicates("guid")， 会在一个 unbound 数据中去重，也就意味着会保存所有的过去的数据

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
