package com.bigData.spark.core

import com.bigData.spark.Commons
import org.apache.spark.rdd.RDD

object SimpleOperator {

  /*

   */
  def main(args: Array[String]): Unit = {

    val spark = Commons.sparkSession
    val sc = spark.sparkContext


    var numList = List(1, 2, 3, 4,4,5,6,7,8,9,0)

    val numMakeRDD: RDD[Int] = sc.makeRDD(numList, 2)
    /*
        .sample 有三个参数

           withReplacement: Boolean: false 不放回，true 有放回
          fraction: Double, 采用比例
          seed: Long = Utils.random.nextLong：


   *  without replacement: probability that each element is chosen; fraction must be [0, 1]
   *  with replacement: expected number of times each element is chosen; fraction must be greater
   *  than or equal to 0

   无放回，fraction 必须是[0,1] 范围
   有放回，fraction 必须大于0 的，可以大于1，表示一个数可以被采样的次数

   种子可以不传，会根据系统时间，自动生成一个种子

     */


    /*
        sample 的一个使用: 判断是否数据倾斜
          如采样100条数据，其中90条数据都是相同的，那么就可以判断数据大部分都是倾斜到了这个相同的key。
     */
    val sampleRDD: RDD[Int] = numMakeRDD.sample(true, 0.9, 42)
//    println(sampleRDD.collect().mkString(","))


    /*
        .distinct： 去重
          底层代码是使用其他操作完成的
          map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)

          所以会出现shuffle 过程
          还有一个方法：
          distinct(numPartitions: Int)
          做完聚合去重之后，分区之后的数据会发生变少，所以这里可以进行重分区

          说道这里就会提到 filter 也会缩小分区中的数据的

          所以还会有一些专门重分区的算子
     */

    val distRDD = numMakeRDD.distinct()

    print(distRDD.collect().mkString(","))
  }
}
