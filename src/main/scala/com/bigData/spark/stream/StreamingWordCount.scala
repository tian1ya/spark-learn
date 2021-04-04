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
      .getOrCreate()

    val sc = spark.sparkContext
    // sc 用来创建 RDD
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc, Seconds(2)) // 一个小批次产生的时间间隔
    ssc.sparkContext.setLogLevel("WARN")
    // ssc 创建实时计算抽象的数据集 DStream

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // 实时的 wordcount
    // flatMap operation is applied on each RDD in the lines DStream to generate the RDDs of the words DStream.
    val value: DStream[String] = lines.flatMap(_.split(" "))
    val value1: DStream[(String, Int)] = value.map(a => (a, 1))

    val value2: DStream[(String, Int)] = value1.reduceByKey((a, b) => a + b)

    value2.print()

    // 开启
    ssc.start()

    // 让程序一直运行, 将Driver 挂起
    ssc.awaitTermination()

    ssc.stop()
  }
}
