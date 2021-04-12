package com.bigData.spark.stream.kafkaPractice

import java.sql.{Connection, DriverManager, PreparedStatement}

object MySQLTransactionTest {

  def main(args: Array[String]): Unit = {

    // 获取连接
    var connection: Connection = null

    // Mysql 的 jdbc 默认是自动提交事务的，省事，但是无法精确控制事务
    var ps: PreparedStatement = null

    try {

      connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "liuxu123")
      connection.setAutoCommit(false);

      // Mysql 的 jdbc 默认是自动提交事务的，省事，但是无法精确控制事务
      ps = connection.prepareStatement("insert into t_wordcount(word, count) values (?,?)")


      /*
        CREATE TABLE t_wordcount(
          word varchar(255),
          count int
        )
         下面插入2条数据，会是在2个事务中的。会出现一个插入，一个插入失败的问题
       */
      ps.setString(1, "spark")
      ps.setLong(2, 1L)
      ps.executeUpdate() // 这里会自动提交事务

      ps.setString(1, "flink")
      ps.setLong(2, 1L)
      ps.executeUpdate() // 这里会自动提交事务

      connection.commit()
    } catch {
      case e: Exception => {
        e.printStackTrace()
        // 如果出现异常，回滚数据
        connection.rollback()
      }
    }
    finally {
      if (ps != null) {
        ps.close()
      }
      if (connection != null) {
        connection.close()
      }
    }
  }
}
