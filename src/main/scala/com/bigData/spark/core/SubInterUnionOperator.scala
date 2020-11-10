package com.bigData.spark.core

import com.bigData.spark.Commons
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

object SubInterUnionOperator {

  /*

   */
  def main(args: Array[String]): Unit = {

    Logger.getLogger("dd").setLevel(Level.ERROR)

    val spark = Commons.sparkSession
    val sc = spark.sparkContext


    var numList = List(1, 2, 3, 4)
    var numList2 = List(4,5,6,7)

    val numMakeRDD: RDD[Int] = sc.makeRDD(numList, 2)
    val numMakeRDD2: RDD[Int] = sc.makeRDD(numList2, 1)

    /*
        计算差集，从原来RDD 中减去和该RDD中共同的部分
     */

    val resultRDD1 = numMakeRDD.subtract(numMakeRDD2)
    println("subtract" + resultRDD1.collect().mkString(","))

    /*
      并集
     */
    val resultRDD2 = numMakeRDD.union(numMakeRDD2)
    println("union" + resultRDD2.collect().mkString(","))

    /*
      交集
     */
    val resultRDD3 = numMakeRDD.intersection(numMakeRDD2)
    println("intersection" + resultRDD3.collect().mkString(","))

    /*
      笛卡尔
     */
    val resultRDD4 = numMakeRDD.cartesian(numMakeRDD2)
    println("cartesian" + resultRDD4.collect().mkString(","))

    /*
      ZIP, 两个 RDD 中的元素个数需要相同，否则会出错，分区不同也会出错
     */
    val resultRDD5 = numMakeRDD.zip(numMakeRDD2)
    println("ZIP" + resultRDD5.collect().mkString(","))
  }
}
