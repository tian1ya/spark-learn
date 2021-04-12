package com.bigData.spark.stream.kafkaPractice

import com.bigData.spark.stream.kafkaPractice.JdbcUtils.getConnection
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.collection.mutable

object KafkaWordCountResultAndOffsetToMysql {
  def main(args: Array[String]): Unit = {
    val appName = "appName" // 这个应该是从程序外部传进来的
    val groupId = "sparkstreamingId" // 这个应该是从程序外部传进来的

    val spark = SparkSession
      .builder().appName(appName)
      .master("local[*]")
      // 这里的 core 至少需要有2个，因为一个被 receiver 占了，剩下的被计算逻辑占用
      .getOrCreate()

    val sc = spark.sparkContext
    // sc 用来创建 RDD
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc, Seconds(2)) // 一个小批次产生的时间间隔
    ssc.sparkContext.setLogLevel("WARN")
    // ssc 创建实时计算抽象的数据集 DStream


    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "10.205.20.6:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean) // 不自动提交偏移量
    )

    // key 可以有，可以没有，key 可以在分区器中使用，没有key 采用轮询的方式将 value 写到分区中

    val offset = new mutable.HashMap[TopicPartition, Long]()
    var connection11: Connection = null
    var statement: PreparedStatement = null
    try {
      connection11 = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "123456")
      statement = connection11.prepareStatement("select  topic_partition, `offset` from t_kafka_offset where appId_groupID=?")
      statement.setString(1, appName + groupId)

      val resultSet: ResultSet = statement.executeQuery()
      while (resultSet.next()) {
        val topicPartition = resultSet.getString(1)
        val topicAndPartition: Array[String] = topicPartition.split("_")
        val offsetNumber = resultSet.getInt(2)

        val topicPartition1: TopicPartition = new TopicPartition(topicAndPartition(0), topicAndPartition(1).toInt)
        offset(topicPartition1) = offsetNumber
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      if (statement != null) statement.close()
      if (connection11 != null) connection11.close()
    }


    val kafkaDs: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent, // 位置策略
      ConsumerStrategies.Subscribe[String, String](Set("sparkstreaming"),
        kafkaParams, // 指定 topic 和 kafka 相关参数
        offset,
      )
    )

    // 创建的时候，如果给了偏移量，那么从偏移量开始读，否则从kafka 中的 __consumer_offset 开始读

    kafkaDs.foreachRDD(foreachFunc = rdd => {
      if (!rdd.isEmpty()) {
        val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        // 实现聚合
        val lines = rdd.map(_.value())
        val result: RDD[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

        // 将聚合后的数据收集到 Driver 端
        val driverResult: Array[(String, Int)] = result.collect()

        println(driverResult.mkString(", "))

        var connection: Connection = null
        var psWordcountTable: PreparedStatement = null;
        var psOffsetTable: PreparedStatement = null;
        try {
          connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "123456")
          connection.setAutoCommit(false)

          /*
              计算结果写 数据库 t_word_count(word:String, count:Int)
              CREATE TABLE t_word_count (
                  word VARCHAR(255),
                  count Int,
                  PRIMARY KEY(word)
              )
           */

          /*
            upsert 插入，没有就插入，有就更新
              将数据偏移量写到 数据库
               t_kafka_offset(topic partition groupID, 结束偏移量)

               CREATE TABLE t_kafka_offset(
                topic_partition varchar(255),
                appId_groupID varchar(255),
                offset bigint,
                primary key(topic_partition, appId_groupID)
               )

               topic_partition + appId_groupID 不能重复，所以应该讲他们2个设置为主键

           */

          psWordcountTable = connection.prepareStatement("INSERT INTO t_word_count (word,`count`) VALUE (?,?) ON DUPLICATE KEY UPDATE `count`=`count`+?")
          /*
              INSERT INTO t_word_count (word,count) VALUE (?,?)
              这里 word 是主键，不能这些写，upsert 插入(没有插入，有则更新)
              当心插入的 word 中存在，那么就会报错，使用

              INSERT INTO t_word_count (word,count) VALUE (?,?) ON DUPLICATE KEY UPDATE count=count+1
           */

          // 写结果
          for (elem <- driverResult) {
            psWordcountTable.setString(1, elem._1)
            psWordcountTable.setInt(2, elem._2)
            psWordcountTable.setInt(3, elem._2)

            psWordcountTable.executeUpdate()
            //            psWordcountTable.addBatch()
          }
          //          psWordcountTable.executeBatch()

          // 写偏移量
          for (elem <- ranges) {
            val topic = elem.topic
            val partition = elem.partition

            val endOffset: Long = elem.untilOffset

            psOffsetTable = connection.prepareStatement("INSERT INTO t_kafka_offset (topic_partition,appId_groupID,offset)" +
              " VALUE (?,?,?) ON DUPLICATE KEY UPDATE offset=?")

            psOffsetTable.setString(1, topic + "_" + partition)
            psOffsetTable.setString(2, appName + "_" + groupId)
            psOffsetTable.setInt(3, endOffset.toInt)
            psOffsetTable.setInt(4, endOffset.toInt) // 如果存在则更新

            psOffsetTable.executeUpdate()

          }

          // 都没有出现异常 提交事务
          connection.commit()


        } catch {
          case e: Exception => {
            e.printStackTrace()
            connection.rollback()

            // 停止 spark 程序
            ssc.stop(true)
          }

        } finally {
          if (psOffsetTable != null) psOffsetTable.close()
          if (psWordcountTable != null) psWordcountTable.close()
          if (connection != null) connection.close()
        }

      }
    })

    // 开启
    ssc.start()

    // 让程序一直运行, 将Driver 挂起
    ssc.awaitTermination()

    ssc.stop()
  }
}
