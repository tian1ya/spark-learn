package com.bigData.spark.stream

import java.sql.Timestamp
import com.bigData.spark.Commons
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamReader, OutputMode, StreamingQuery}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamingWordCount {
  def main(args: Array[String]): Unit = {
//    val spark = Commons.sparkSession

    val spark = SparkSession
      .builder()
      .master("local[2]")
      // 这里的 core 至少需要有2个，因为一个被 receiver 占了，剩下的被计算逻辑占用
      //
      .getOrCreate()

    val sc = spark.sparkContext
    // sc 用来创建 RDD
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc, Seconds(2)) // 一个小批次产生的时间间隔
    ssc.sparkContext.setLogLevel("WARN")
    // ssc 创建实时计算抽象的数据集 DStream

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // 实时的 wordcount
    val value: DStream[String] = lines.flatMap(_.split(" "))
    val value1: DStream[(String, Int)] = value.map(a => (a, 1))

    val value2: DStream[(String, Int)] = value1.reduceByKey((a, b) => a + b)

    value2.print()

    // 开启
    ssc.start()

    // 让程序一直运行, 将Driver 挂起
    ssc.awaitTermination()


//    import spark.implicits._
//
//    val streamReader: DataStreamReader = spark.readStream
//
//    val lines: DataFrame = streamReader
//      .format("socket")
//      .option("host", "localhost")
//      .option("port", 7777)
//      .option("includeTimestamp", value = true) //输出内容包括时间戳
//      .load()
//
//
//    val words: DataFrame = lines.as[(String, Timestamp)]
//      .flatMap(line => line._1.split(", ").map(word => (word, line._2)))
//      .toDF("word", "timestamp")
//
//
//    val windowSize = 10
//    val slideSize = 5
//    val windowDuration = s"$windowSize seconds"
//    val slideDuration = s"$slideSize seconds"
//
//
//    val wordCount: DataFrame = words
//      .withWatermark("timestamp", "10 minutes")// 定义 wm
//      .groupBy(
//        window($"timestamp", windowDuration, slideDuration)
//        // 分别指明了事件事件列，窗口size，slide
//      ).count()
//
//    val orderedWordCount = wordCount.orderBy("window")
//
//    /*
//      由于采用聚合操作，所以需要指定"complete"输出形式。
//      非聚合操作只能用appen模式
//     */
//
//
//    val query: StreamingQuery = orderedWordCount.writeStream
//      .outputMode(OutputMode.Complete().toString)
//      .format("console")
//      .start()
//
//    query.awaitTermination()

  }
}
