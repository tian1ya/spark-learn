package com.bigData.spark.core

import com.bigData.spark.Commons
import org.apache.spark.rdd.RDD

object mapAndOperatorAboutMap {

  /*
      map 是对每一个元素进行的操作，每一个元素都会进行进栈和出栈，所以性对而言性能会有丁丁问题
          但是这里也会有一个好处，就是处理完一个数据之后内存就可以立马释放掉，节省内存。

      mapPartitions: 将一个分区级别做运算，所以进出栈的时候是以分区的位单位的，性能会比较高
          但是这里会有一个不好的地方，那就是内存会长期得不到释放，直到处理完一个分区之后，才能释放

      消费指定分区的数据

      所以当内存比较大的时候可以使用mapPartitions， 但是在一般的场景下是不建议使用的，因为spark本身就是基于内存计算的
      所以会比较容易出现OOM的情况
   */
  def main(args: Array[String]): Unit = {
    val spark = Commons.sparkSession
    val sc = spark.sparkContext


    var numList = List(1, 2, 3, 4)

    val numMakeRDD: RDD[Int] = sc.makeRDD(numList, 3)

    val mapRDD: RDD[Int] = numMakeRDD.mapPartitions(data => data.map(_ * 2))


    /*
         mapPartitionsWithIndex 消费的时候带着分区号
     */
    val mapWithIndexRDD: RDD[Int] = numMakeRDD.mapPartitionsWithIndex {
      case (partitionIndex, datas) => {
        partitionIndex match {
          case 1 => datas.map(_ * 3)
          case _ => datas
        }
      }
    }

//    mapWithIndexRDD.foreach(data => println(data))

    /*
        flatMap: 只扁平化最外层
     */

    val flatMapRDD: RDD[List[Int]] = sc.makeRDD(List(List(1,2,3), List(2,3,4,5)))
    val flatMapResultRDD: RDD[Int] = flatMapRDD.flatMap(data => data)

    /*
      这里就扁平化不成一个 list[Int]
      val flatMapRDD2: RDD[List[List[Int]]] = sc.makeRDD(List(List(List(1,2,3), List(2,3,4,5)), List(List(1,2,3), List(2,3,4,5))))
      val flatMapResultRDD2: RDD[List[Int]] = flatMapRDD2.flatMap(data => data)
    */

    println(flatMapResultRDD.collect().mkString(","))
  }
}
