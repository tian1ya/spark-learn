package com.bigData.spark.rdd

import com.bigData.spark.Commons
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object wordCount {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = Commons.sparkSession
    val sc: SparkContext = spark.sparkContext

    val lines: RDD[String] = sc.textFile("/workcount.txt")

    val words: RDD[String] = lines.flatMap(line => line.split(" "))

    val pairWords = words.map(word => (word, 1))

    val wordCount = pairWords.reduceByKey((a, b) => (a + b))

    wordCount.collect().foreach(data => println(data._1 + " => " + data._2))

    spark.close()
  }
}
