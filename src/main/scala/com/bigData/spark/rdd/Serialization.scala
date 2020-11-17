package com.bigData.spark.rdd

import com.bigData.spark.Commons
import org.apache.spark.rdd.RDD

object Serialization {

  def main(args: Array[String]): Unit = {

    val spark = Commons.sparkSession
    val sc = spark.sparkContext


    /*
        在进行spark 进行编程的时候，初始化工作是在Driver 端完成的，而实际的执行程序是在 executor 端进行的
        所以就涉及到了进程之间的通信，数据是需要序列化的

        在 spark2.0 开始使用 kryo序列话，在shuffle 过程中，简单数据类型及其的数组和字符串类型以及开始使用 kryo

        使用类的时候，也需要继承 Serializable 才能够序列化
     */

    val rdd = sc.makeRDD(Array("hello world", "hello spark", "hello hadoop"))
    val searcher = new Searcher("hello")

    val result = searcher.getMatchedRDD2(rdd)

    result.foreach(println)

  }
}

/*
    Task not serializable, Searcher 序列化失败
    出错的原因：
      spark 的计算逻辑是在 executor 端执行的，而初始化是在Driver 端执行的
      在执行到 rdd.filter(isMatch) 代码的时候实际上是 rdd.filter(this.isMatch)
      也就是需要将初始化之后的 Searcher 对应由 Driver 发送给 Executor， 发送的过程中就设计到序列化
      而 Searcher 是不能序列化的，所以出错了

      而继承 extends Serializable 之后就可以序列化了，代码就可以执行成功了

      同时执行 searcher.getMatchedRDD2(rdd) 但是并不去继承 Serializable 也是会出错的
      这里虽然使用的匿名函数(不会涉及到 Searcher 对象)， 也仅仅使用到 query 这个String，且String
      也继承了 Serializable，但是还是会有序列化问题， 因为 new Searcher(val  query: String)
      使用val 修饰 query，所以这个时候 query 是 Searcher 对应的一部分，还是会设计到这个对象，所以还是会要求这个对象能够
      序列化，所以报错 继承 Serializable 之后就运行OK

      但是如 Searcher2 的中  getMatchedRDD2.getMatchedRDD2 给一个局部变量，那么就可以不序列化也可以执行，因为过程中并没有涉及到 Searcher2 对象


 */
class Searcher(val  query: String){
  def isMatch(s: String): Boolean = s.contains(query)
  def getMatchedRDD1(rdd: RDD[String]): RDD[String] = rdd.filter(isMatch)
  def getMatchedRDD2(rdd: RDD[String]): RDD[String] = {
    rdd.filter(_.contains(query))
  }
}


class Searcher2(val  query: String){
  def isMatch(s: String): Boolean = s.contains(query)
  def getMatchedRDD1(rdd: RDD[String]): RDD[String] = rdd.filter(isMatch)
  def getMatchedRDD2(rdd: RDD[String]): RDD[String] = {
    val a = query
    rdd.filter(_.contains(a))
  }
}
