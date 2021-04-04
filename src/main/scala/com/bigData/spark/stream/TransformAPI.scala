package com.bigData.spark.stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object TransformAPI {

  def main(args: Array[String]): Unit = {
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

    // 获取底层的 rdd 获取，然后在返回这些 rdd
    // 该 api 的功能
    // 由于在 DStream 中有时候也需要很多的 api，但是呢，这些 api 在rdd 中已经实现了
    // 所以在 DStream 在实现一遍就有些冗余了，所以如果在 DStream 中能够获得底层的 rdd
    // 并且可以操作，实现一些功能，那么就加强了 DStream 的功能
    val value: DStream[String] = lines.transform(rdd => rdd)

    // 开启
    ssc.start()

    // 让程序一直运行, 将Driver 挂起
    ssc.awaitTermination()

    ssc.stop()
  }

}
