package com.bigData.spark.rdd

import com.bigData.spark.Commons
import org.apache.spark.rdd.RDD

object FilterOperator {

  /*

   */
  def main(args: Array[String]): Unit = {

    val spark = Commons.sparkSession
    val sc = spark.sparkContext


    var numList = List(1, 2, 3, 4,4,5,6,7,8,9,0)

    val numMakeRDD: RDD[Int] = sc.makeRDD(numList, 2)

    /*
        filter 参数：T => Boolean
     */
    val filterRDD: RDD[Int] = numMakeRDD.filter(num => true)

    println(filterRDD.collect().mkString(","))

  }
}
