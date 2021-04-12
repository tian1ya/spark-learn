package com.bigData.spark.stream.kafkaPractice

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}

object AggOperatorExactlyOnce {
  /*
      聚合最终结果写到 mysql 等支持事务的数据库中
      聚合后的结果使用 Executor 写入到结果数据库
        不推荐，因为一个Executor 持有一个连接，多个Executor 中的写不能在同一个事务中，出现数据问题

      因为聚合结果只会的结果比较少的数据，是基本可以拿到 Driver 端的，所以由 Driver 去写数据，数据收集到Driver 然后在去写

      包括偏移信息，也是在 Driver 端的
      所以这个时候 偏移量和聚合结果均在一个事务中，这样就保证数据的 Exactly Once 的消费

      如果失败了写结果数据库，那么会重新执行聚合操作，这个时候会从上次执行成功开始在读kafka 中的数据
      这个时候由于有失败的一次，所以会出现这次和上次失败的任务会重新读取数据，对于 kafka 出现重读的问题
      但是对于计算结果是没有问题的，计算结果不会出现问题

      官方文档也说明，偏移量可以是在 spark checkpoint 中保存。交给 kafka 保存
      也可以是自己写到外部数据库保存

      自己写程序写到外部数据库，会更加的精确
   */

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


    // 开启
    ssc.start()

    // 让程序一直运行, 将Driver 挂起
    ssc.awaitTermination()

    ssc.stop()
  }

}
