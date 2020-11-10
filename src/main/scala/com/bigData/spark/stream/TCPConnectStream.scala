package com.bigData.spark.stream

import com.bigData.spark.Commons
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object TCPConnectStream {
  def main(args: Array[String]): Unit = {
    val spark = Commons.sparkSession

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    import spark.implicits._

    /*
       lines unbounded table containing the streaming text data

        可支持的Source
          FileSource： text/csv/json/parquet
          KafkaSource
          RateSource（for test）
          SocketSource(for test)

         注意有些源是不支持容错的，因为不能够记住每次读了数据的 offset
     */

    // 这个方法返回的是一个动态表的 loader
    val streamReader: DataStreamReader = spark.readStream

    // 这里的 lines 是一个动态的 DataFrame，数据在时刻发生变化
    val lines: DataFrame = streamReader
      .format("socket")
      .option("host", "localhost")
      .option("port", 7777)
      .load()

    /*
    val userSchema = new StructType()
      .add("name","String")
      .add("age","integer")

    val csvDF: DataFrame = spark.readStream
      .option("sep",",")
      .schema(userSchema)
      .csv("src/main/data/user.csv")
  */

    /*
        可以在 structure DataFrame 上使用任何的 DataFrame 可以使用的算子
        .as[String] df 转为 ds
     */

    /*
      val wordsToCount= lines.as[String]
        .flatMap(_.split(" "))
        .map((_,1))
        .rdd    SSR 中不可以使用 rdd 的转换，出错
        .reduceByKey(_+_)
        .toDF("word","count")
    */

    val wordsToCount: DataFrame = lines.as[String]
      .flatMap(_.split(" "))
      .groupBy("value")
      .count()

    val wordCount: DataFrame = wordsToCount

    /*
      The query object is a handle to that active streaming query,

     */

    // 返回一个 streamWriter
    val stream1: DataStreamWriter[Row] = wordCount.writeStream

    // 然后 .start() 就启动write，那么写到哪里呢？ 就是 .outputMode("complete") 定义的地方
    val query: StreamingQuery = wordCount.writeStream
      .outputMode(OutputMode.Complete().toString)
      .format("console")
      .start()

    query.awaitTermination()

  }
}
