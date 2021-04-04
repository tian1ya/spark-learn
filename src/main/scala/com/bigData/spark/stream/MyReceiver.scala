package com.bigData.spark.stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object MyReceiver {

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

    val msgDs: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiverCls)

    msgDs.print()

    ssc.start()
    ssc.awaitTermination()

    ssc.stop()
  }

  /*
    自定义数据采集器
   */

  class MyReceiverCls extends Receiver[String](StorageLevel.MEMORY_ONLY) {

    private var flag: Boolean = true;

    override def onStart(): Unit = {
      // 启动的时候
      new Thread(() => {
        while (flag) {

          val msg = "采集的数据： " + new Random().nextInt(10).toString
          store(msg)
          Thread.sleep(500)
        }
      }).start()
    }

    override def onStop(): Unit = {
      flag = false;
    }
  }

}
