package com.bigData.spark.dataframe

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._


object udaf {

  /*
      Spark SQL 给数据提供了更多的结构化信息，以及计算时候优化需要的信息

      Dataset: 强类型，使用lambda 高级函数

      DataFrame: 在Dataset 的基础之上，给每个列增加了column name，这个关系型数据库中的一张表的概念类似
   */

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    spark.udf.register("myAverage", MyAverage)

    val df = spark.read.json("examples/src/main/resources/employees.json")
    df.createOrReplaceTempView("employees")
    df.show()
    // +-------+------+
    // |   name|salary|
    // +-------+------+
    // |Michael|  3000|
    // |   Andy|  4500|
    // | Justin|  3500|
    // |  Berta|  4000|
    // +-------+------+

    val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()
    // +--------------+
    // |average_salary|
    // +--------------+
    // |        3750.0|
    // +--------------+

    // ****************************************************************************************88
    // ****************************************************************************************88
    // ****************************************************************************************88

    val ds = spark.read.json("examples/src/main/resources/employees.json").as[Employee]
    ds.show()
    // +-------+------+
    // |   name|salary|
    // +-------+------+
    // |Michael|  3000|
    // |   Andy|  4500|
    // | Justin|  3500|
    // |  Berta|  4000|
    // +-------+------+

    // Convert the function to a `TypedColumn` and give it a name
    val averageSalary = MyAverage2.toColumn.name("average_salary")
    val result2 = ds.select(averageSalary)
    result2.show()
    // +--------------+
    // |average_salary|
    // +--------------+
    // |        3750.0|
    // +--------------+
  }
}

// 用户自定义无类型UDAF
object MyAverage extends UserDefinedAggregateFunction {
  // 构造函数 apply(exprs: Column*)， 这里可以给进来多个列

  // Data types of input arguments of this aggregate function
  def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

  // Data types of values in the aggregation buffer
  def bufferSchema: StructType = {
    StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
  }

  // The data type of the returned value
  def dataType: DataType = DoubleType

  // Whether this function always returns the same output on the identical input
  def deterministic: Boolean = true

  // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
  // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
  // the opportunity to update its values. Note that arrays and maps inside the buffer are still
  // immutable.

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // Updates the given aggregation buffer `buffer` with new input data from `input`
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }
  // Calculates the final result
  def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
}

// 用户自定义有类型，类型安全的 UDAF
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

case class Employee(name: String, salary: Long)
case class Average(var sum: Long, var count: Long)

// -IN, BUF, OUT
object MyAverage2 extends Aggregator[Employee, Average, Double] {
  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: Average = Average(0L, 0L)

  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  // 分区内的聚合
  def reduce(buffer: Average, employee: Employee): Average = {
    buffer.sum += employee.salary
    buffer.count += 1
    buffer
  }

  // Merge two intermediate values
  // 分区之间的聚合
  def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  // Transform the output of the reduction
  def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count
  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Average] = Encoders.product
  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}