package com.bigData.spark.stream.kafkaPractice

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountJoinKafkaHbaseManagerOffset {

  def main(args: Array[String]): Unit = {
    val appname = "ssc-kafka-hbase"
    val group = "group1"
    val topics = ""

    val spark = SparkSession
      .builder().appName(appname)
      .master("local[*]")
      // 这里的 core 至少需要有2个，因为一个被 receiver 占了，剩下的被计算逻辑占用
      .getOrCreate()

    val sc = spark.sparkContext
    // sc 用来创建 RDD
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc, Seconds(2)) // 一个小批次产生的时间间隔
    ssc.sparkContext.setLogLevel("WARN")
    // ssc 创建实时计算抽象的数据集 DStream

    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "dream1:9092,dream2:9092,dream3:9092", // kafka地址
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer", // 设置反序列化组件
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> group, // 消费者组
      "auto.offset.reset" -> "earliest", // 指定消费者从哪开始消费[latest,earliest]
      "enable.auto.commit" -> "false" // 是否自动提交偏移量，默认是true
    )
    val lastoffset = OffsetUtils.selectOffsetFromHbase(appname, group)
    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, lastoffset)
    )

    dstream.foreachRDD((kafkaRdd: RDD[ConsumerRecord[String, String]]) => {
      println("------------------------------------" + System.currentTimeMillis() + "------------------------------------")
      if (!kafkaRdd.isEmpty()) {
        // 每条数据都是一个json，第一步解析数据
        val ranges: Array[OffsetRange] = kafkaRdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val lines: RDD[String] = kafkaRdd.map(_.value())
        val ordersAndErro: RDD[MyOrder] = lines.mapPartitions((pt: Iterator[String]) => {
          val gson = new Gson()
          var order: MyOrder = null
          pt.map(line => {
            try {
              order = gson.fromJson(line, classOf[MyOrder])
            } catch {
              case _: Exception =>
                logger.error(line + "-----------解析异常")
            }
            order
          })
        })
        // 过滤异常数据
        val orders: RDD[MyOrder] = ordersAndErro.filter(_ != null)

        orders.foreachPartition(pt1 => {
          if (pt1.nonEmpty) {
            // ****************** 根据分区id获取，offset传入executor端 ******************
            val range: OffsetRange = ranges(TaskContext.get.partitionId())
            var conn: Connection = null
            var table: Table = null
            try {
              // 获取Hbase连接
              conn = HbaseUtils.getConnection()
              table = conn.getTable(TableName.valueOf("STREAMING"))
              val puts: util.ArrayList[Put] = new util.ArrayList[Put]()
              pt1.foreach(order => {
                val put = new Put(Bytes.toBytes(order.gid))
                put.addColumn(Bytes.toBytes("DATA"), Bytes.toBytes("ORDER"), Bytes.toBytes(order.money))
                // create view STREAMING (pk varchar primary key,"OFFSET"."OFFSET" varchar,"OFFSET"."topic_partition" varchar);
                // 如果是最后一条数据将偏移量写入和数据一块写入 hbase，我们利用单条数据的一致性（要成功都成功要失败都失败）包子ExactlyOnce
                if (!pt1.hasNext) {
                  put.addColumn(Bytes.toBytes("OFFSET"), Bytes.toBytes("APP_GID"), Bytes.toBytes(appname + "_" + group))
                  put.addColumn(Bytes.toBytes("OFFSET"), Bytes.toBytes("TOPIC_PARTITION"), Bytes.toBytes(range.topic + "_" + range.partition))
                  put.addColumn(Bytes.toBytes("OFFSET"), Bytes.toBytes("OFFSET"), Bytes.toBytes(range.untilOffset))
                }
                puts.add(put)
                if (puts.size() % 10 == 0) {
                  table.put(puts)
                  puts.clear()
                }
              })
              table.put(puts)
            } catch {
              case e: Exception =>
                e.printStackTrace()
            } finally {
              if (table != null) {
                table.close()
              }
              if (conn != null) {
                conn.close()
              }
            }
          }
        })
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
