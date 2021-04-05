package com.bigData.spark.stream

import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object WindowDemo2 {
  def main(args: Array[String]): Unit = {
//    val spark = Commons.sparkSession

    /*

     */
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

    ssc.checkpoint("cp")

    // ssc 创建实时计算抽象的数据集 DStream

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val value: DStream[(String, Int)] = lines.map(a => (a, 1))


    // 是属于有状态计算
    val value1: DStream[(String, Int)] = value.reduceByKeyAndWindow(
      (x: Int, y:Int) => x+y,
      (x: Int, y: Int) => x-y,
      Seconds(6),Seconds(6)
    )
//    val value1: DStream[(String, Int)] = value.window(Seconds(9), Seconds(3))
    val value2: DStream[(String, Int)] = value1.reduceByKey(_ + _)


    value2.print()

    // 开启
    ssc.start()

    // 让程序一直运行, 将Driver 挂起，阻塞线程，后面的代码执行不到。
    ssc.awaitTermination()

    // 优雅关闭
    // stop(stopSparkContext: Boolean, stopGracefully: Boolean)
    // 也就是 executor 先将当前的数据处理完毕，之后再去关闭，而不是直接关闭
    // 不能再同一个线程中执行，需要在另外一个线程中执行，并且通过外部的一个按钮能够控制这个线程，
    // ssc.stop(true, true)
  }
}
