package com.bigData.spark.rdd

import com.bigData.spark.Commons
import org.apache.spark.rdd.RDD

object RePartitionOperator {

  /*

   */
  def main(args: Array[String]): Unit = {

    val spark = Commons.sparkSession
    val sc = spark.sparkContext


    var numList = List(1, 2, 3, 4,4,5,6,7,8,9,0)

    val numMakeRDD: RDD[Int] = sc.makeRDD(numList, 4)

    /*
        函数签名
        coalesce(
            numPartitions: Int,
            shuffle: Boolean = false,
            partitionCoalescer: Option[PartitionCoalescer] = Option.empty)

      coalesce 默认只能缩小分区，不能扩大，如果给的新的分区个数大于原来的分区个数，那么分区个数还会保持原来的分区个数

      因为将数据分区个数从小到大一定会有 shuffle 过程，而将分区大到小，却不一定发生shuffle 过程，

      所以 coalesce 就会有第二参数指定是否发生 shuffle, 默认情况下不shuffle 过程

      所以当要扩大分区，那么必须将 shuffle 打开

      numMakeRDD.coalesce(8, true)

      一般情况下使用 coalesce 就是缩小分区

      有点疑惑啊，所以spark 还有一个算子 .repartition(numPartitions: Int)
      既可以缩小，又能减少分区

      二者的区别：
        repartition(numPartitions: Int) = coalesce(numPartitions, shuffle = true)
        repartition 必须要 shuffle。
        所以当减少分区数的时候使用 .coalesce 不需要发送shuffle
        而 repartition 是会发生 shuffle 的

     如果使用 coalesce 分区减少之后不shuffle 那么可能会发生数据倾斜，所以 shuffle 有时候是必要的
     */
    val mapRDD = numMakeRDD.coalesce(2)

    val ssss = numMakeRDD.repartition(10)
    println(mapRDD.collect().mkString(","))

  }
}
