package com.bigData.spark.core

import com.bigData.spark.Commons
import org.apache.spark.rdd.RDD

object GlomOperator {

  /*

   */
  def main(args: Array[String]): Unit = {

    val spark = Commons.sparkSession
    val sc = spark.sparkContext


    var numList = List(1, 2, 3, 4,4,5,6,7,8,9,0)

    val numMakeRDD: RDD[Int] = sc.makeRDD(numList, 2)

    /*
        将一个分区的数据的数据封装为一个数组进行处理，一个分区处理为一个Array
     */
    val glomRDD: RDD[Array[Int]] = numMakeRDD.glom()

    val mapRDD: RDD[Int] = glomRDD.map(_.max)
    println(mapRDD.collect().mkString(","))

  }
}
