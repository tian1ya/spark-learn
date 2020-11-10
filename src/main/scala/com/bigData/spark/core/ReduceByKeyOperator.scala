package com.bigData.spark.core

import com.bigData.spark.Commons
import org.apache.spark.rdd.RDD

object ReduceByKeyOperator {

  /*

   */
  def main(args: Array[String]): Unit = {

    /*
        reduceByKey

        会有 shuffle 过程

        和 groupByKey(
          注意和 groupby 的区别：
          groupBy：通过制定的规则分组
          groupByKey：只能通过 pairRDD 中的 key 分组
        )

      reduceByKey(func: (V, V) => V) 和   groupByKey() 的区别
        reduceByKey:
          会先在每个分区中执行 func这个函数，将相同的 key 进行func 聚合操作，然后在执行shuffle，而这个时候每个分区中每个key 只会对应
          一条数据，大大减少了shuffle 过程中数据的移动，而在shuffle 过程在将不同分区中相同的key 在进行 func 聚合，得到最终的记过
          所以reduceByKey 会涉及到分区内聚合(combine)，然后分区之间聚合
        groupByKey:
          直接将 key 相同的数据挪到同一个分区中(shuffle过程)，涉及到所有数据的移动
          然后在使用 map/mapValues 将计算逻辑应用到每个组中
          所以 groupByKey 只有分区之间聚合

       总结：reduceByKey 和 groupByKey 结果是一样的，不过 reduceByKey 因为有预聚合，shuffle 效率高，算子也会有更高的性能
     */
    val spark = Commons.sparkSession
    val sc = spark.sparkContext


    val pairRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1),("a", 20), ("b", 2), ("c", 3)),2)

    val result: RDD[(String, Int)] = pairRDD.reduceByKey(_+_)
    println("reduceByKey")
    println(result.collect().mkString(" : "))

    val groupByKeyRDD: RDD[(String, Iterable[Int])] = pairRDD.groupByKey()
    val result1: RDD[(String, Int)] = groupByKeyRDD.map(pair => (pair._1, pair._2.sum))
    println("groupByKey")
    println(result1.collect().mkString(" : "))

  }
}
