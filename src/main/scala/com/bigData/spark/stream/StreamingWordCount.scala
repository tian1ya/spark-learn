package com.bigData.spark.stream

import java.sql.Timestamp

import com.bigData.spark.Commons
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamReader, OutputMode, StreamingQuery}


object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    val spark = Commons.sparkSession

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    import spark.implicits._

    val streamReader: DataStreamReader = spark.readStream

    val lines: DataFrame = streamReader
      .format("socket")
      .option("host", "localhost")
      .option("port", 7777)
      .option("includeTimestamp", value = true) //输出内容包括时间戳
      .load()


    val words: DataFrame = lines.as[(String, Timestamp)]
      .flatMap(line => line._1.split(", ").map(word => (word, line._2)))
      .toDF("word", "timestamp")


    val windowSize = 10
    val slideSize = 5
    val windowDuration = s"$windowSize seconds"
    val slideDuration = s"$slideSize seconds"


    val wordCount: DataFrame = words
      .withWatermark("timestamp", "10 minutes")// 定义 wm
      .groupBy(
        window($"timestamp", windowDuration, slideDuration)
        // 分别指明了事件事件列，窗口size，slide
      ).count()

    val orderedWordCount = wordCount.orderBy("window")

    /*
      由于采用聚合操作，所以需要指定"complete"输出形式。
      非聚合操作只能用appen模式
     */


    val query: StreamingQuery = orderedWordCount.writeStream
      .outputMode(OutputMode.Complete().toString)
      .format("console")
      .start()

    query.awaitTermination()

  }
}
