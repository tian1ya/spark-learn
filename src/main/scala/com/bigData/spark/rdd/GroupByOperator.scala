package com.bigData.spark.rdd

import com.bigData.spark.Commons
import org.apache.spark.rdd.RDD

object GroupByOperator {

  /*

   */
  def main(args: Array[String]): Unit = {

    val spark = Commons.sparkSession
    val sc = spark.sparkContext


    var numList = sc.makeRDD(List(1, 2, 3, 4,4,5,6,7,8,9,0))

    /*
      奇偶区分
     */
    val groupRDD: RDD[(Int, Iterable[Int])] = numList.groupBy(num => num % 2)

    /*
       注意 groupBy 和 groupByKey 使用的RDD类型不同

       group 操作会引起 shuffle 过程，shuffle 过程会有读写磁盘(落盘)的过程，因为shuffle 需要等待所有分区数据都准备就绪
       而这个等待过程已经写好的数据需要写磁盘，当所有数据就绪之后，然后才开始后续的操作，这个时候读磁盘。


     */
    print(groupRDD.collect().mkString(","))

  }
}
