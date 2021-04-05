package com.bigData.spark.stream.practice

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.util.Random

object KafkaProducer {

  def main(args: Array[String]): Unit = {
    /*
        时间戳，省份 城市 用户 广告
     */

    // application => kakfa => sparkStreaming => Analysis

    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.3.39:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val kafkaProducer = new KafkaProducer[String, String](prop)

    while (true) {

      mockData().foreach(data => {
        // kafka 中生成数据
//        println(data)
        val record = new ProducerRecord[String, String]( "sparkstreaming", data)
        kafkaProducer.send(record)
      })
    }

  }

  def mockData(): ListBuffer[String] = {
    val buffer = ListBuffer[String]()
    val areas = ListBuffer[String]("华北", "华东", "华南")
    val citys = ListBuffer[String]("北京", "上海", "深圳")

    for (i <- 1 to 30) {

      val area = areas(new Random().nextInt(3))
      val city = citys(new Random().nextInt(3))

      val userId = new Random().nextInt(6) + 1
      val adId = new Random().nextInt(6) + 1

      buffer.append(s"${System.currentTimeMillis()} $area $city $userId $adId")
    }
    buffer
  }

}
