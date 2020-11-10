package com.bigData.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Commons {
  Logger.getLogger("dd").setLevel(Level.ERROR)
  val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  val dataPath = "/Users/xuxliu/Ifoods/scala/spark/myDevEnv/src/main/data/"
  val basicPath = "/Users/xuxliu/Ifoods/scala/spark/myDevEnv/src/main/data/Input/"
  val basicModelSavePath = "//Users/xuxliu/Ifoods/scala/spark/myDevEnv/src/main/data/model/"
  val fromSrcPath = "/Users/xuxliu/Ifoods/scala/spark/myDevEnv/src/main/data/fromSrc/resources/"
}
