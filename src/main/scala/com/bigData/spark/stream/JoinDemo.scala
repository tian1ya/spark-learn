package com.bigData.spark.stream

import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object JoinDemo {
  def main(args: Array[String]): Unit = {
//    val spark = Commons.sparkSession

    val spark = SparkSession
      .builder()
      .master("local[*]")
      // 这里的 core 至少需要有2个，因为一个被 receiver 占了，剩下的被计算逻辑占用
      .getOrCreate()

    val sc = spark.sparkContext
    // sc 用来创建 RDD
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc, Seconds(3)) // 一个小批次产生的时间间隔
    ssc.sparkContext.setLogLevel("WARN")
    // ssc 创建实时计算抽象的数据集 DStream

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val lines1: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9998)

    lines.print()
    lines1.print()

    val value: DStream[(String, Int)] = lines.map(a => (a, 9))
    val value1: DStream[(String, Int)] = lines1.map(a => (a, 8))

    value.print()
    value1.print()
    // 其就是 2个 rdd 的join
    val value2: DStream[(String, (Int, Int))] = value.join(value1)

    value2.print()

    // 开启
    ssc.start()

    // 让程序一直运行, 将Driver 挂起
    ssc.awaitTermination()

    ssc.stop()
  }
}
