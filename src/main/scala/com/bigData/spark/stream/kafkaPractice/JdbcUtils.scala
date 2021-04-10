package com.bigData.spark.stream.kafkaPractice


import com.alibaba.druid.pool.DruidDataSourceFactory

import java.sql.Connection
import java.util.Properties
import javax.sql.DataSource

object JdbcUtils {


  val dataSource: DataSource = init()

  def init(): DataSource = {
    val properties = new Properties()

    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver");
    properties.setProperty("url", "jdbc.mysql://localhost:3306/sparkstreaming");
    properties.setProperty("username", "root");
    properties.setProperty("password", "liuxu123");

    DruidDataSourceFactory.createDataSource(properties);
  }

  def getConnection: Connection = dataSource.getConnection

}
