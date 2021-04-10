package com.bigData.spark.stream.kafkaPractice

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaControllerCommitOffset {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      // 这里的 core 至少需要有2个，因为一个被 receiver 占了，剩下的被计算逻辑占用
      .getOrCreate()

    val sc = spark.sparkContext
    // sc 用来创建 RDD
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc, Seconds(5)) // 一个小批次产生的时间间隔
    ssc.sparkContext.setLogLevel("WARN")
    // ssc 创建实时计算抽象的数据集 DStream


    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.3.39:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "sparkstreamingId02", // 还是消费同一个topic，修改组id，那么就会从topic 下从头开始读消息
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean) // 不自动提交偏移量
    )

    // key 可以有，可以没有，key 可以在分区器中使用，没有key 采用轮询的方式将 value 写到分区中
    val kafkaDs: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent, // 位置策略
      ConsumerStrategies.Subscribe[String, String](Set("sparkstreaming"), kafkaParams) // 指定 topic 和 kafka 相关参数
    )

    // 偏移量只有在和 kafka 读取到的第一个 RDD 中可以拿到 offset
    kafkaDs.foreachRDD(rdd => {
      // 这段代码是在 Driver 端执行的
      if (!rdd.isEmpty()) {
        // 如果有数据才去执行， isEmpty 是一个 action 的算子
        // 先将 rdd 强转为 HasOffsetRanges，这是一个 kafka 的一个 rdd KafkaRDD，实现继承了 spark 的rdd
        // KafkaRDD 中的 compute 方法中获取 kafka 的消息
        // 如果是一个普通的 rdd 那么无法转换的，
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 保存的信息
        /*
            final class OffsetRange private(
            val topic: String,
            val partition: Int,
            val fromOffset: Long,
            val untilOffset: Long)
         */

        for (elem <- offsetRanges) {
          println(s"topic: ${elem.topic} - partition: ${elem.partition} - fromOffset:${elem.fromOffset} - untilOffset: ${elem.untilOffset}")
        }

        // 上面的操作都是在Driver 端操作的

        // 下面对于 rdd 的操作均是在集群上完成的
        rdd.map(_.value()).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
          .foreach(println)

        // 在 Driver 端异步的更新偏移量，实现这个接口的 是 DirectKafkaInputDStream
        kafkaDs.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })

    // 消费者自动提交消息 偏移
    //    val value: DStream[(String, Int)] = kafkaDs
    //      .map(_.value())
    //      .flatMap(_.split(" "))
    //      .map((_, 1))
    //      .reduceByKey(_ + _)
    //
    //    value.print()

    // 开启
    ssc.start()

    // 让程序一直运行, 将Driver 挂起
    ssc.awaitTermination()

    ssc.stop()
  }
}
