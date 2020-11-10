package com.bigData.spark.core

import com.bigData.spark.Commons
import org.apache.spark.rdd.RDD

object SortByOperator {

  /*

   */
  def main(args: Array[String]): Unit = {

    val spark = Commons.sparkSession
    val sc = spark.sparkContext


    var numList = List(1, 2, 3, 4,4,5,6,7,8,9,0)

    val numMakeRDD: RDD[Int] = sc.makeRDD(numList, 2)

    /*
        Return this RDD sorted by the given key function.

        sortBy[K](
          f: (T) => K,
          ascending: Boolean = true,
          numPartitions: Int = this.partitions.length)

         会有shuffle 过程，且能够指定shuffle 过程之后的分区个数，默认和之前保持一致

     */
    val sortResult = numMakeRDD.sortBy(num => num)
    println(sortResult.collect().mkString(","))

  }
}
