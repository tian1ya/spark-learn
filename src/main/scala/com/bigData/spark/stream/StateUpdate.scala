package com.bigData.spark.stream

import com.bigData.spark.stream.MyReceiver.MyReceiverCls
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object StateUpdate {

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
    // ssc 创建实时计算抽象的数据集 DStream

    // checkpoint 存储路径，用于缓冲有状态操作的时候产生的状态。
    ssc.checkpoint("cp")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // 无状态数据操作，只对当前的采集周几数据进行处理
    val value: DStream[String] = lines.flatMap(_.split(" "))

    val value1: DStream[(String, Int)] = value.map(a => (a, 1))

//    val value2: DStream[(String, Int)] = value1.reduceByKey((a, b) => a + b)

    // 根据 key 对数据的状态进行更新
    // 传递的参数中，
    // seq 相同 key 的 value 数据
    // buffer 是缓冲区中相同 key 的已经缓冲了的数据
    // 此外还需要设置 checkpoint 的存储地方，用于存储这些 状态

    val value2 = value1.updateStateByKey((seq: Seq[Int], buffer: Option[Int]) => {
      val existValue = buffer.getOrElse(0) + seq.sum
      Option(existValue)
    })

    value2.print()

    // 开启
    ssc.start()

    // 让程序一直运行, 将Driver 挂起
    ssc.awaitTermination()

    ssc.stop()
  }

}
