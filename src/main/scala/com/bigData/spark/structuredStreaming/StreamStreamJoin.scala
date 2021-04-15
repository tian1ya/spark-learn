package com.bigData.spark.structuredStreaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class DeviceData3www(device: String, deviceType: String, signal: Double, time: Long)

case class DeviceData4dd(device: String, deviceType: String, age: Long)

object StreamStreamJoidddn extends App {

  /*
      for both the input streams, we buffer past input as streaming state,
      so that we can match every future input with past input and accordingly generate joined results

      但是这里带来的新问题就是，数据的状态会一直一直的增加，显然是不合理的(unbound state)
      所以在 join 的时候需要制定一个另外的条件，这个条件表示当前 join 的范围的数据，而在范围外的数据是不可以参与join 的，也就是这部分数据的 state 不需要
      保持了，你可以
      1. 设置 watermark 来接受一定范围内迟到的数据
      2. 基于 eventTime 设置条件 join，这样选出一定范围时间的state 数据才会去 join
        Time range join conditions (e.g. ...JOIN ON leftTime BETWEEN rightTime AND rightTime + INTERVAL 1 HOUR),
        Join on event-time windows (e.g. ...JOIN ON leftTimeWindow = rightTimeWindow).

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
