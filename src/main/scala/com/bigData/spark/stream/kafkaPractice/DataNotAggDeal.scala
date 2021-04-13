package com.bigData.spark.stream.kafkaPractice

object DataNotAggDeal {

  /*
      会有这样的场景
        处理数据有几亿条用户id数据，然后我们需要将用户id的信息关联上省市区等其他信息，然后在写会去，输入输出数据量是一样的
        事实表关联维度表数据，然后在写会到事实表，存储中。例如写到 hbase

     而 HBASE 是只支持行级的事务，所以需要将 数据的偏移和数据写到hnase 的一行中
     保证数据和offset 是始终是一致的。
        主义整个队数据的操作过程是不能shuffle 的，否则 offset 就乱了



   */
}
