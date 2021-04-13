package com.bigData.spark.stream.kafkaPractice

import java.sql.{Connection, DriverManager}
import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable

object OffsetUtils {
  def selectOffsetFromHbase(appname: String, group: String): Map[TopicPartition, Long] = {
    val offsets = new mutable.HashMap[TopicPartition, Long]()
    val conn = DriverManager.getConnection("jdbc:phoenix:dream1,dream2,dream3:2181")
    // 必须加引号才行
    val ps = conn.prepareStatement("select TOPIC_PARTITION,max(\"OFFSET\") as \"OFFSET\" from streaming WHERE APP_GID =?  GROUP BY TOPIC_PARTITION")
    ps.setString(1,appname+"_"+group)
    val resultSet = ps.executeQuery()
    while (resultSet.next()) {
      val TOPIC_PARTITION = resultSet.getString("TOPIC_PARTITION").split("_")
      val OFFSET = resultSet.getInt("OFFSET")
      offsets(new TopicPartition(TOPIC_PARTITION(0),TOPIC_PARTITION(1).toInt)) = OFFSET.toLong
    }
    offsets.toMap
  }
}
