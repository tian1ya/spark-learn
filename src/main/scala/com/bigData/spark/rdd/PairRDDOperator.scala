package com.bigData.spark.rdd

import com.bigData.spark.Commons
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

object PairRDDOperator {


  def main(args: Array[String]): Unit = {

    val spark = Commons.sparkSession
    val sc = spark.sparkContext


    var numList = List(1, 2, 3, 4,4,5,6,7,8,9,0)


    val pairRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)),2)

    /*
        partitionBy: 根据 key 分区

        这个算子主要的作用是并不是改变分区个数，而是定义数据属于哪个分区的计算逻辑，从而决定哪些数据
        在同一个分区中

        class HashPartitioner(partitions: Int) extends Partitioner {
            require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

            def numPartitions: Int = partitions

            def getPartition(key: Any): Int：
              计算 partition 分区号的逻辑，输入是没一个数据的key，根据key计算得到这条数据应该在的分区

            override def equals(other: Any)
            override def hashCode: Int = numPartitions
        }
     */
    val partitionedByKeyRDD: RDD[(String, Int)] = pairRDD.partitionBy(new HashPartitioner(3))

  }
}
