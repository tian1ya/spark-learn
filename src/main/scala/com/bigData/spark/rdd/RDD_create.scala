package com.bigData.spark.rdd

import com.bigData.spark.Commons
import org.apache.spark.rdd.RDD

object RDD_create {
  def main(args: Array[String]): Unit = {
    /*
      RDD 的创建时可以从3部分得到
      1. 从集合中创建
      2. 从外部数据存储中创建
      3. 从已有的RDD转换得到
     */

    val spark = Commons.sparkSession
    val sc = spark.sparkContext

    /*
      1. 从集合中得到
     */

    var numList = List(1, 2, 3, 4)

    val numRDD: RDD[Int] = sc.parallelize(numList)
    val numMakeRDD: RDD[Int] = sc.makeRDD(numList)
    println(numRDD.collect())


    /*
      从外部文件中创建
        spark 读取文件默认使用 hadoop 的文件读取规则，读取字节从偏移量开始计算的，读取文件的单位是行
        分区规则是以文件为单位
        
      支持路径中使用通配符，
        textFile("/myPaht/Hell*.txt)
        textFile("/myPaht/c*.gz)
     */

    val fileRDD: RDD[String] = sc.textFile("file:///Users/xuxliu/Ifoods/scala/spark/myDevEnv/src/main/data/wordCount.txt")
    println("fileRDD")
    fileRDD.foreach(data => println(data))


    /*
        创建文件时候使用的 并行度=分区数，默认情况下，
        先会去取配置文件 conf 中的并行度，如果没有会去计算机器的 totalCores
        scheduler.conf.getInt("spark.default.parallelism", totalCores)
     */

    /*
      如果不给第二个并行度，那么执行下面的代码，生成的文件会有12个part，因为当前我的机器是12核的

      val numRDD2: RDD[Int] = sc.makeRDD(numList)
      numRDD2.saveAsTextFile("file:///Users/xuxliu/Ifoods/scala/spark/myDevEnv/src/main/data/wordCount1")
    */


    /*
        如果我给并行度参数为2
        那么在路径下只有有 2 个 part 的文件

        val numRDD2: RDD[Int] = sc.makeRDD(numList,2)
        numRDD2.saveAsTextFile("file:///Users/xuxliu/Ifoods/scala/spark/myDevEnv/src/main/data/wordCount1")
     */



    /*
        那么读数据的时候的分区是如何指定的呢？

        在读数据的时候也是可以执行并行度的，默认情况下，并行度的设置
        math.min(defaultParallelism, 2)
     */

    val numRDD2: RDD[String] = sc.textFile("/Users/xuxliu/Ifoods/scala/spark/myDevEnv/src/main/data/wordCount.txt")
    println(numRDD2.getNumPartitions) // 2, 然后后面在写数据的时候就会有2个数据的 part
    numRDD2.saveAsTextFile("file:///Users/xuxliu/Ifoods/scala/spark/myDevEnv/src/main/data/wordCount2")

    spark.close()
  }
}
