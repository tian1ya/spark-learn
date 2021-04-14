//package com.bigData.spark.stream.kafkaPractice
//
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.{HBaseConfiguration}
//import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
//
//object HbaseUtils {
//  def getConnection(): Connection = synchronized {
//    val conf: Configuration = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.quorum", "dream1:2181,dream2:2181,dream3:2181")
//    ConnectionFactory.createConnection(conf)
//  }
//}
