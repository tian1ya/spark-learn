package com.bigData.spark.dataframe

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object NewFeature extends App {

  private val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("name")
//    .config("spark.sql.optimizer.dynamicPartitionPruning.enabled","false")
    .getOrCreate()
  private val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("WARN")

  import spark.implicits._

  spark.range(10000)
    .select($"id", $"id".as("k"))
    .write
    .partitionBy("k")
    .format("parquet")
    .mode("overwrite")
    .saveAsTable("table1")

  spark.range(100)
    .select($"id", $"id".as("k"))
    .write
    .partitionBy("k")
    .format("parquet")
    .mode("overwrite")
    .saveAsTable("table2")


  spark.sql("select * from table1 t1 join table2 t2 on t1.k=t2.k and t2.id<2").explain()
  spark.sql("select * from table1 t1 join table2 t2 on t1.k=t2.k and t2.id<2").show()

  Thread.sleep(Long.MaxValue)
  spark.close()


 // 7 s

}
