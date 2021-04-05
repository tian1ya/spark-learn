package com.bigData.spark.stream.practice

import org.apache.commons.lang.time.FastDateFormat
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.ResultSet
import scala.collection.mutable.ListBuffer


/*
   实时统计动态黑名单，将每天对某个广告点击超过 100 次的用户拉黑

   黑名单存储在 mysql

   1， 判断已经是黑名单，是那么不操作
   2. 不在黑名单中那么判断点击是否超过阈值，如果超过存储到数据库中， 否则不操作

   3. 检查点会产生大量的小文件，那么最好是将结果放到 mysql 中。然后更新每天用户的点击数量
   4. 获取最新的点击数量，超过阈值，那么拉入到黑名单中
 */

case class AdClink(ts: String, area: String, city: String, user: String, ad: String)

object KafkaBlackList {

  val df = new FastDateFormat("yyyy-MM-dd")

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


    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.3.39:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "sparkstreamingId",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDs: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("sparkstreaming"), kafkaParams)
    )

    val clickData: DStream[AdClink] = kafkaDs.map(data => {
      val record = data.value()
      val data1 = record.split(" ")

      AdClink(data1(0), data1(1), data1(2), data1(3), data1(4))
    })

    // 周期获取黑名单数据
    /*
        CREATE TABLE user_ad_count(
          dt varchar(255),
          userid char(1),
          adid char(1),
          count BIGINT,
          PRIMARY KEY (dt,userid,adid)
        );

        CREATE TABLE black_list(userid char(1));
        CREATE TABLE black_list(userid char(1), PRIMARY KEY (userid));
     */
    // 判断点击用户是否在黑名单中

    // 如果用户不在很名单汇总，那么进行统计数量(每个采集周期)

    // 如果统计数量超过点击阈值，那么直接拉入黑名单
    val userAdClickDStream: DStream[((String, String, String), Int)] = clickData.transform(rdd => {

      val userInBlackList = ListBuffer[String]()
      val connection = JdbcUtils.getConnection
      val psState = connection.prepareStatement("select userid from black_list")

      val rs: ResultSet = psState.executeQuery()
      while (rs.next()) {
        userInBlackList.append(rs.getString(1))
      }


      rs.close()
      psState.close()
      connection.close()

      val rddWithUserNotInBlackList = rdd.filter(data => !userInBlackList.contains(data.user))

      rddWithUserNotInBlackList.map(data => {
        val dateStr = df.format(data.ts.toLong)
        val user = data.user
        val area = data.area
        val city = data.city
        val ad = data.ad
        ((dateStr, user, ad), 1)
      })
        .reduceByKey(_ + _)
    })

    // 如果没有超过阈值，那么需要将当前的广告点击数量进行更新

    userAdClickDStream.foreachRDD(rdd => {
      // stream 本身就是没一个时间间隔提交一个 job
      // rdd 的 foreach 会将数据全部拿到 Driver
      // 课程中使用了 foreach，
      rdd.foreach {
        case ((day, user, ad), count) => {
          if (count > 30) {
            // 统计时间片段中的点击次数是否超过 100
            val connection = JdbcUtils.getConnection
            val psState = connection.prepareStatement(
              """
                |INSERT INTO black_list (userid) VALUE (?)
                |ON DUPLICATE KEY
                |UPDATE userId=?
                |""".stripMargin)

            psState.setString(1, user)
            psState.setString(2, user)
            psState.executeUpdate()

            psState.close()
            connection.close()
          } else {
            // 如果没有超过，那么将当天的统计数量进行更新
            // 查询统计表数据，吐过存在那么更新，还需要判断更新后是否超过阈值，否则新增
            val connection = JdbcUtils.getConnection
            val psState = connection.prepareStatement(
              """
                |SELECT * FROM user_ad_count where dt=? and adid=? and userid=?
                |""".stripMargin)

            psState.setString(1, day)
            psState.setString(2, user)
            psState.setString(2, ad)
            val rs = psState.executeQuery()

            if (rs.next()) {
              // 如果存在数据，那么更新
              val psState = connection.prepareStatement(
                """
                  |update user_ad_count set count = count + ?
                  |where dt=? and adid=? and userid=?
                  |""".stripMargin)

              psState.setInt(1, count)
              psState.setString(2, day)
              psState.setString(3, user)
              psState.setString(4, ad)

              rs.close()
              psState.close()
              connection.close()

              // 更新之后再去判断是否超过阈值
              val psState1 = connection.prepareStatement(
                """
                  |select *
                  |where dt=? and adid=? and userid=? and count >= 30
                  |""".stripMargin)

              psState1.setString(1, day)
              psState1.setString(2, ad)
              psState1.setString(3, user)

              val rs2 = psState1.executeQuery()

              if (rs2.next()) {
                val psState = connection.prepareStatement(
                  """
                    |INSERT INTO black_list (userid) VALUE (?)
                    |ON DUPLICATE KEY
                    |UPDATE userId=?
                    |""".stripMargin)

                psState.setString(1, user);
                psState.executeUpdate()

                psState.close()
              }

              rs2.close()


            } else {
              // 如果不存在，那么新增
              val psState = connection.prepareStatement(
                """
                  |INSERT INTO user_ad_count (dt,adid,userid,count) VALUE (?,?,?,)
                  |""".stripMargin)

              psState.setString(1, day)
              psState.setString(2, user)
              psState.setString(3, ad)
              psState.setInt(4, count)


              rs.close()
              psState.close()
              connection.close()
            }

            rs.close()
            psState.close()
            connection.close()

          }
        }
      }
    })

    // 判断更新后的点击数量是否超过阈值，如果超过那么将用户拉入到黑名单

    //

    // 开启
    ssc.start()

    // 让程序一直运行, 将Driver 挂起
    ssc.awaitTermination()

    ssc.stop()
  }
}
