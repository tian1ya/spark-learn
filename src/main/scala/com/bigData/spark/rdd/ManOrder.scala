package com.bigData.spark.rdd

object ManOrder {
  implicit object manOrder extends Ordering[Man] {
    override def compare(x: Man, y: Man): Int = if (x.score >= y.score) 1 else -1
  }
}
