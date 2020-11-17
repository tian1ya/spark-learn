package com.bigData.spark.rdd

import com.bigData.spark.Commons
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD

object DefinePartitioner {

  /*

   */
  def main(args: Array[String]): Unit = {

    val spark = Commons.sparkSession
    val sc = spark.sparkContext


    var numList = List(1, 2, 3, 4,4,5,6,7,8,9,0)

    val numMakeRDD: RDD[Int] = sc.makeRDD(numList, 2)

    val pairRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)),2)

    val partitionedByKeyRDD: RDD[(String, Int)] = pairRDD.partitionBy(new MyPartitioner(3))

    partitionedByKeyRDD.mapPartitionsWithIndex((index, data) => {
      println(s"partition-index $index" + data)
      data
    }).collect()


    spark.close()
  }
}


class MyPartitioner(numPartition: Int) extends Partitioner {
  // 获取分区个数
  override def numPartitions: Int = numPartition

  // 根据pairRDD的key 计算该条数据应该在的分区号
  override def getPartition(key: Any): Int = {
    /*
      如果直接返回0 ，然后传入进来的 numPartition>1,那么还是会将数据分发为3个分区，只不过只有一个分区有数据
      其他都没有分区

      所以这里应该是经过 key 的逻辑运算，然后只返回有限的数据 [0,numPartition-1]
     */
    0
  }
}
