//package com.bigData.spark.stream.kafkaPractice
//
//import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.TableName
//import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
//import org.apache.kafka.common.TopicPartition
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka010._
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
//import scala.collection.mutable
//
///*
//    会有这样的场景
//      处理数据有几亿条用户id数据，然后我们需要将用户id的信息关联上省市区等其他信息，然后在写会去，输入输出数据量是一样的
//      事实表关联维度表数据，然后在写会到事实表，存储中。例如写到 hbase
//
//   而 HBASE 是只支持行级的事务，所以需要将 数据的偏移和数据写到hnase 的一行中
//   保证数据和offset 是始终是一致的。
//      主义整个队数据的操作过程是不能shuffle 的，否则 offset 就乱了
//
//
//
// */
//
//object KafkaWordCountResultAndOffsetToHbase {
//  def main(args: Array[String]): Unit = {
//    val appName = "appName" // 这个应该是从程序外部传进来的
//    val groupId = "sparkstreamingId" // 这个应该是从程序外部传进来的
//
//    val spark = SparkSession
//      .builder().appName(appName)
//      .master("local[*]")
//      // 这里的 core 至少需要有2个，因为一个被 receiver 占了，剩下的被计算逻辑占用
//      .getOrCreate()
//
//    val sc = spark.sparkContext
//    // sc 用来创建 RDD
//    sc.setLogLevel("ERROR")
//
//    val ssc = new StreamingContext(sc, Seconds(2)) // 一个小批次产生的时间间隔
//    ssc.sparkContext.setLogLevel("WARN")
//    // ssc 创建实时计算抽象的数据集 DStream
//
//
//    val kafkaParams = Map[String, Object](
//      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "10.205.20.6:9092",
//      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
//      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
//      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
//      "auto.offset.reset" -> "latest",
//      "enable.auto.commit" -> (false: java.lang.Boolean) // 不自动提交偏移量
//    )
//
//    // key 可以有，可以没有，key 可以在分区器中使用，没有key 采用轮询的方式将 value 写到分区中
//
//    val offset = new mutable.HashMap[TopicPartition, Long]()
//    var connection11: Connection = null
//    var statement: PreparedStatement = null
//    try {
//      connection11 = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "123456")
//      statement = connection11.prepareStatement("select  topic_partition, `offset` from t_kafka_offset where appId_groupID=?")
//      statement.setString(1, appName + groupId)
//
//      val resultSet: ResultSet = statement.executeQuery()
//      while (resultSet.next()) {
//        val topicPartition = resultSet.getString(1)
//        val topicAndPartition: Array[String] = topicPartition.split("_")
//        val offsetNumber = resultSet.getInt(2)
//
//        val topicPartition1: TopicPartition = new TopicPartition(topicAndPartition(0), topicAndPartition(1).toInt)
//        offset(topicPartition1) = offsetNumber
//      }
//    } catch {
//      case e: Exception => {
//        e.printStackTrace()
//      }
//    } finally {
//      if (statement != null) statement.close()
//      if (connection11 != null) connection11.close()
//    }
//
//
//    val kafkaDs: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      LocationStrategies.PreferConsistent, // 位置策略
//      ConsumerStrategies.Subscribe[String, String](Set("sparkstreaming"),
//        kafkaParams, // 指定 topic 和 kafka 相关参数
//        offset,
//      )
//    )
//
//    // 创建的时候，如果给了偏移量，那么从偏移量开始读，否则从kafka 中的 __consumer_offset 开始读
//
//    kafkaDs.foreachRDD(rdd => {
//      if (!rdd.isEmpty()) {
//        val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//
//        // 实现聚合
//        val lines = rdd.map(_.value())
//        lines.foreachPartition(iter => {
//          if (!iter.isEmpty) {
//            val hbaseConnection:Connection = null
//
//            iter.foreach(l => {
//                hbaseConnection.getTable(TableName.)
//            })
//          }
//
//
//        })
//      }
//    })
//    // 开启
//    ssc.start()
//
//    // 让程序一直运行, 将Driver 挂起
//    ssc.awaitTermination()
//
//    ssc.stop()
//  }
//}
