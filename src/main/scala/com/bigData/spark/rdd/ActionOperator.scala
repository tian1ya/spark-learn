package com.bigData.spark.rdd

import com.bigData.spark.Commons
import org.apache.spark.rdd.RDD

object ActionOperator {

  /*

   */
  def main(args: Array[String]): Unit = {

    val spark = Commons.sparkSession
    val sc = spark.sparkContext


    var numList = List(1, 2, 3, 4)

    val numMakeRDD: RDD[Int] = sc.makeRDD(numList, 2)

    /*
      count()
      take(n)
      foreach

      aggregate
     */

    val sum: Int = numMakeRDD.aggregate(10)(_+_,_+_)
    /*
        注意这里初始值的使用，这里会在2个分区中分别使用初始值10
        所以结果会是 10 + 1 + 2 + 10 + 3 + 4 = 40

        注意和 aggregateByKey 的区别
     */

    /*
        当分区内计算和分区间计算逻辑相同的时候可以使用 fold
     */
    val sum2: Int = numMakeRDD.fold(10)(_+_)
    println(sum2)

    /*
        countByKey
     */

    val pairRDD2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 20), ("a", 4), ("b", 22), ("b", 2), ("c", 3)), 2)
    val stringToLong: collection.Map[String, Long] = pairRDD2.countByKey()

    /*
        foreach(func)
        它是和collect 是不一样的，foreach 函数均是在Executor 上执行的，不是在Driver 端执行的
     */

    spark.close()
  }
}
