package com.bigData.spark.structuredStreaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object WordCount extends App {


  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("appName")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._

  /*
      lines: unbounded table,
      包括有一个列，列明是 value
   */
  val lines: DataFrame = spark
    .readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999).load()

  // lines.as[String]， DataFrame -> Dataset
  private val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))
  private val wordCount: DataFrame = words.groupBy("value").count()

  private val query: StreamingQuery = wordCount
    .writeStream
    .outputMode(OutputMode.Complete())
    .format("console")
    .start()

  query.awaitTermination()


}
