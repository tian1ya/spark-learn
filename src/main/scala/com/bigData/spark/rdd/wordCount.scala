package com.bigData.spark.rdd

import com.bigData.spark.Commons
import org.apache.spark.sql.{DataFrame, Dataset, Row}
//import com.bigData.spark.rdd.ManOrder.manOrder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/*
    extends Comparable[Boy]: 实现比较排序功能
    Serializable: 可以序列化类

    而使用 case class 默认是可以序列化接口的
 */
class Boy(val name: String, val id: Int, val score: Double) extends Comparable[Boy] with Serializable {
  override def compareTo(o: Boy): Int = if (this.score >= o.score) 1 else -1

  override def toString = s"Boy($name, $id, $score)"
}

/*
  属性默认就是 val 的
  本身就是可以 Serializable 的
 */
case class Man(name: String, id: Int, score: Double)

object wordCount {

  implicit val manOrder = new Ordering[Man] {
    override def compare(x: Man, y: Man): Int = if (x.score > y.score) 1 else 0
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("dd").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    //    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("hbase", 1), ("hadoop", 1), ("flink", 1), ("spark", 1)))
    //    val rdd2: RDD[(String, Int)] = sc.parallelize(List(("spark", 2), ("hadoop", 2), ("hbase", 2), ("hive", 2)))
    //
    //    val value: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    //    print(value.collect().mkString(","))
    /*
      (hive,(CompactBuffer(),CompactBuffer(2))),
      (flink,(CompactBuffer(1),CompactBuffer())),
      (hbase,(CompactBuffer(1),CompactBuffer(2))),
      (spark,(CompactBuffer(1),CompactBuffer(2))),
      (hadoop,(CompactBuffer(1),CompactBuffer(2)))
     */

    //    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("hbase", 1), ("hadoop", 1), ("flink", 1), ("spark", 1)))
    //    val rdd2: RDD[(String, Int)] = sc.parallelize(List(("spark", 2), ("hadoop", 2), ("hbase", 2), ("hbase", 22),("hive", 2),("hive", 22)))
    //
    //    val value: RDD[(String, (Option[Int], Option[Int]))] = rdd1.fullOuterJoin(rdd2)
    //    // (hbase,(1,2)), (spark,(1,2)), (hadoop,(1,2))

    //    val value: RDD[Int] = sc.parallelize(List(1, 2, 3, 4,5,6,7,8), 4)
    //    val tuples = value.map(x => (x, 1)).reduceByKey(_ + _)
    //    tuples.collect()
    //
    //    value.cache()
    //
    //    value.unpersist()


    //    val function: (String, Int) => String = (a, b) => a + b.toString
    //    val function1: (String, String) => String = (a, b) => a + " : " + b
    //    val str: String = value.aggregate("0")(function, function1)

    //    println("aaaaaa " + value.getNumPartitions) // 分区数是 2
    //    val value1: RDD[Int] = value.coalesce(4, false)
    //    println("aaaaaa " + value1.getNumPartitions) // 这里分区数还是不变，是2

    //    value.foreachPartition((a: Iterator[Int]) => {
    //      // 注意这里的打印是在 executor 上打印的，二不是在 driver 中的
    //      print("*****")
    //      for (elem <- a) {
    //        print(elem)
    //      }
    //    })

    //    value.foreach(a => {
    //      // 注意这里的打印是在 executor 上打印的，二不是在 driver 中的
    //      print(a)
    //    })


    //    val i: Int = value.reduce((a, b) => a + b)
    //    println(value.collect().mkString(", "))

    //
    //    val lines: RDD[String] = sc.parallelize(List("laoda,90,90", "nihao,4,2", "wode,9,8"))
    //
    //    val value: RDD[(String, Int, Double)] = lines.map(line => {
    //      val strings = line.split(",")
    //      val name = strings(0)
    //      val int = strings(1).toInt
    //      val double = strings(2).toDouble
    //      (name, int, double)
    //    })

    /*
        Tuple 中是有 Order 的排序规则的
        类型 Int 或者 String 也都是可以直接排序的，* (-1) 就可以逆序了
     */
    //    val value1: RDD[(String, Int, Double)] = value.sortBy(x => -1 * x._3)
    //    print(value1.collect().mkString(","))

    val value: RDD[(Int, Int, Int)] = sc.parallelize(Seq((1, 2, 3), (11, 22, 33), (4, 5, 7)))
    val df: DataFrame = value.toDF("a", "b", "c")
    val dfSelectted: RDD[Row] = df.select("a", "b", "c").rdd
    val value1: RDD[String] = dfSelectted.flatMap(row => row.toSeq.zipWithIndex.map { case (v, i) => s"${i}:${v}" })

    val tupleToLong: collection.Map[String, Long] = value1.countByValue()

    val stringToStrings: Map[String, Iterable[String]] = tupleToLong
      .keys
      .groupBy(key => key.substring(0, key.indexOf(":")))

    val a: Seq[(String, Iterable[Seq[Char]])] = stringToStrings.toSeq.map {
      case (colIdx, categories) => (
        Map(0 -> "a", 1 -> "b", 2 -> "c").get(colIdx.toInt).get,
        categories.map(cate => cate.substring(cate.indexOf(":" + 1)).toSeq)
      )
    }


    //    Thread.sleep(100 * 10000)
    spark.close()

  }
}
