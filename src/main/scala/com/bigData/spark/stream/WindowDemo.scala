package com.bigData.spark.stream

import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object WindowDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[2]")
      // 这里的 core 至少需要有2个，因为一个被 receiver 占了，剩下的被计算逻辑占用
      .getOrCreate()

    val sc = spark.sparkContext
    // sc 用来创建 RDD
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc, Seconds(3)) // 一个小批次产生的时间间隔
    ssc.sparkContext.setLogLevel("WARN")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val value: DStream[(String, Int)] = lines.map(a => (a, 1))

    // 这里的周期的设置是和 批次产生的时间间隔是相关的
    // 窗口的范围应该是采集周期的整数倍，一个窗口中收集若干个 microBatch
    // 如果只给一个时间参数，那么就是一个滚动参数，步长就是一个批次时间间隔
    // 还可以传入第二个参数，这个参数就是步长，这个时候就是一个滑动窗口
    // 注意这里是会产生一个同一个数据出现在多个窗口中，出现重复统计
    // 滑动步长: 隔多久触发一次计算
    val value1: DStream[(String, Int)] = value.window(Seconds(9))
//    val value1: DStream[(String, Int)] = value.window(Seconds(9), Seconds(3))
    val value2: DStream[(String, Int)] = value1.reduceByKey(_ + _)


    value2.print()

    // 开启
    ssc.start()

    // 让程序一直运行, 将Driver 挂起
    ssc.awaitTermination()

    ssc.stop()
  }
}
