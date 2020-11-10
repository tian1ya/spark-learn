package com.bigData.spark.core

import com.bigData.spark.Commons
import org.apache.spark.rdd.RDD

object aggregateByKeyOperator {

  /*

   */
  def main(args: Array[String]): Unit = {

    val spark = Commons.sparkSession
    val sc = spark.sparkContext

    /*
        reduceByKey
          在 reduceByKey 的例子中，使用的 func 既做预聚合，也做shuffle之后的聚合，也就是分区内的聚合和分区间的聚合
          reduceByKey(func: (V, V) => V) func 使用的函数的类型都是一样的，不能改变的

        aggregateByKey(zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U)
          seqOp： 做分区间的预聚合，并且可以将原来的类型V，和初始化的类型U进行seqOp操作，输出类型U
          combOp：将分区的输出类型U进行再次分区间的操作，然后输出最后的结果

        reduceByKey 分区内核分区间的操作都是一样的，如分区内相加，那么分区间还是相加
        aggregateByKey： 就比 reduceByKey 更加的灵活，分区内可以相加，分区间可以相乘，甚至可以改变原来的数据类

        reduceByKey： 分区内和分区间逻辑和类型都需相同
        aggregateByKey： 分区内和分区间逻辑和类型都可以不同


     */
    val pairRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 20), ("a", 4), ("a", 22), ("b", 2), ("c", 3)), 2)

    val resultRDD = pairRDD.aggregateByKey(0)(
      (zero, first) => math.max(zero, first),
      (zero, first) => zero + first
    )

    //    println(resultRDD.collect().mkString(" : "))


    val resultRDD2 = pairRDD.aggregateByKey(0)(_ + _, _ + _)

    // 如果分区内的逻辑和分区间的逻辑一样，那么可以使用 foldByKey 替换
    val resultRDD3 = pairRDD.foldByKey(0)(_ + _)
    //    println(resultRDD2.collect().mkString(" : "))
    //    println(resultRDD3.collect().mkString(" : "))

    //TODO: 求相同key 的值的平均值
    val pairRDD2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 20), ("a", 4), ("b", 22), ("b", 2), ("c", 3)), 2)
    // 使用 groupBy 可以实现
    val avgRDD: RDD[(String, Iterable[(String, Int)])] = pairRDD2.groupBy(_._1)
    val result = avgRDD.map(data => (data._1, data._2.map(_._2).sum / data._2.size))
    //    println(avgRDD.collect().mkString(","))
    //    println(result.collect().mkString(","))
    // 上面的实现性能不够好，会涉及到大量的数据shuffle 操作

    val resultRDD5: RDD[(String, (Double, Long))] = pairRDD2.aggregateByKey((0.0, 0L))(
      (zero, first) => (zero._1 + first, zero._2 + 1L),
      (first, second) => (first._1+second._1, first._2+second._2)
    )

//    resultRDD5.collect().foreach(data => {
//      println(data._1 + "=>(" + data._2._1 + " , " + data._2._2 + ")")
//    })
//
//    val resultAveRDD: RDD[(String, Double)] = resultRDD5.mapValues{
//      case (value, 0L) => value
//      case (value, cnt) => value/cnt
//    }
//
//    println(resultAveRDD.collect().mkString(","))

    //TODO： 使用 combineByKey 实现上求均值
    /*
        combineByKey[C](
          createCombiner: V => C,      转换数据结构，
          mergeValue: (C, V) => C,     分区内计算规则
          mergeCombiners: (C, C) => C, 分区间计算规则
          partitioner: Partitioner,
          mapSideCombine: Boolean = true,
          serializer: Serializer = null
        )
     */

    val combineResuRDD = pairRDD2.combineByKey(
      num => (num, 1),
      (t:(Int, Int), value:Int) => (t._1 + value, t._2 + 1),
      (t1:(Int,Int), value1:(Int, Int)) => (t1._1 + value1._1, t1._2 +value1._2)
    )

    combineResuRDD.collect().foreach{
      case (world, (sum, count)) => println(world + " => " + sum/count)
    }
  }

  /*
      combineByKey 和 aggregateByKey 的不同

          combineByKey[C](
            createCombiner: V => C,      转换数据结构，
            mergeValue: (C, V) => C,     分区内计算规则
            mergeCombiners: (C, C) => C, 分区间计算规则
        )

        aggregateByKey(zeroValue: U)(
                seqOp: (U, V) => U,
                combOp: (U, U) => U)
      二者都是先在分区内聚合，然后在分区间聚合
      二者的不同在于初始化不同， aggregateByKey 需要给一个初始化结果，combineByKey 直接在第一个进来的数据上做出初始化

      二者底层都调用 combineByKeyWithClassTag

   */
}
