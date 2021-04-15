//package com.bigData.spark.stream
//
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//
//import java.text.SimpleDateFormat
//import scala.util.parsing.json.JSON
//
///*
//    Streaming 实现双流join，如一个订单数据，另一个数订单详情数据，现在需要将2个流按照某个公共字段进行 join 操作
//    同时订单数据和订单详情数据理论是同时产生的，但是实际上会有延迟，2个流不一定同时到达，
//
//    所以需要对数据做缓存处理，前提是2个流的批次大小必须一致，首选明白订单数和订单详情数据是1对多的关系
//
//    考虑订单数据就算在同批次数据中 join 上了，也是需要缓存的，因为不确定是否还有其他的订单详情迟到
//
//    对于订单详情信息，如果join 上了，那么久结束了，先同批次订单数据进行join，没有join上，那么和缓存下来的订单join，如果还没有join上，
//    那么就来早了需要缓存下来
//
//
//    问题1： 采用什么样的join 方法呢？
//    join、leftOuterJoin、rightOuterJoin、fullOuterJoin
//
//    缓存介质，考虑到实时性，以及数量的问题，可以使用redis 和 Hbase
//      订单中的数据往往比较大，缓存中的数据一旦被命中，那么立马删除，对于订单数据只要等待一段时间就会被删除，设定一个延迟时间阈值，
//      如果大于这个阈值，那么久可以删除，计算有极端的情况下某条详情数据过来了，那也没有必要join了，已经过了实时计算的范畴了
// */
//
//case class OrderInfo(id: String,
//                     province_id: String,
//                     consignee: String,
//                     order_comment: String,
//                     var consignee_tel: String,
//                     order_status: String,
//                     payment_way: String,
//                     user_id: String,
//                     img_url: String,
//                     total_amount: Double,
//                     expire_time: String,
//                     delivery_address: String,
//                     create_time: String,
//                     operate_time: String,
//                     tracking_no: String,
//                     parent_order_id: String,
//                     out_trade_no: String,
//                     trade_body: String,
//                     var create_date: String,
//                     var create_hour: String)
//
//case class OrderDetail(id: String,
//                       order_id: String,
//                       sku_name: String,
//                       sku_id: String,
//                       order_price: String,
//                       img_url: String,
//                       sku_num: String)
//
//case class UserInfo(id: String, birthday: String, gender: String, user_level: String)
//
//case class SaleDetail(var order_detail_id: String = null,
//                      var order_id: String = null,
//                      var order_status: String = null,
//                      var create_time: String = null,
//                      var user_id: String = null,
//                      var sku_id: String = null,
//                      var user_gender: String = null,
//                      var user_age: Int = 0,
//                      var user_level: String = null,
//                      var sku_price: Double = 0D,
//                      var sku_name: String = null,
//                      var dt: String = null) {
//
//  def mergeOrderInfo(orderInfo: OrderInfo) = {
//    if (orderInfo != null) {
//      this.order_id = orderInfo.id
//      this.order_status = orderInfo.order_status
//      this.create_time = orderInfo.create_time
//      this.dt = orderInfo.create_date
//      this.user_id = orderInfo.user_id
//    }
//  }
//
//  def mergeOrderDetail(orderDetail: OrderDetail): Unit = {
//    if (orderDetail != null) {
//      this.order_detail_id = orderDetail.id
//      this.sku_id = orderDetail.sku_id
//      this.sku_name = orderDetail.sku_name
//      this.sku_price = orderDetail.order_price.toDouble
//    }
//  }
//
//  def mergeUserInfo(userInfo: UserInfo): Unit = {
//    if (userInfo != null) {
//      this.user_id = userInfo.id
//      val sdf = new SimpleDateFormat("yyyy-MM-dd")
//      val date = sdf.parse(userInfo.birthday)
//      val curTs: Long = System.currentTimeMillis()
//      val betweenMs = curTs - date.getTime
//      val age = betweenMs / 1000L / 60L / 60L / 24L / 365L
//      this.user_age = age.toInt
//      this.user_gender = userInfo.gender
//      this.user_level = userInfo.user_level
//    }
//  }
//
//
//  def this(orderInfo: OrderInfo, orderDetail: OrderDetail) {
//    this
//    mergeOrderInfo(orderInfo)
//    mergeOrderDetail(orderDetail)
//  }
//
//}
//
//object StreamingsJoin extends App {
//
//  def joinKindsTest: Unit = {
//    val spark = SparkSession.builder().master("local[*]").appName("name").getOrCreate()
//    val sc = spark.sparkContext
//    sc.setLogLevel("ERROR")
//    val rdd1 = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")))
//    val rdd2 = sc.makeRDD(List((1, "a1"), (1, "b1"), (4, "c1")))
//    println("join: " + rdd1.join(rdd2).collect().toList)
//    /*
//        List((1,(a,a1)), (1,(a,b1)))
//     */
//    println("leftOuterJoin: " + rdd1.leftOuterJoin(rdd2).collect().toList)
//    /*
//      List((1,(a,Some(a1))), (1,(a,Some(b1))), (2,(b,None)), (3,(c,None)))
//     */
//    println("rightOuterJoin: " + rdd1.rightOuterJoin(rdd2).collect().toList)
//    /*
//        List((1,(Some(a),a1)), (1,(Some(a),b1)), (4,(None,c1)))
//     */
//    println("fullOuterJoin: " + rdd1.fullOuterJoin(rdd2).collect().toList)
//    /*
//        List((1,(Some(a),Some(a1))), (1,(Some(a),Some(b1))), (2,(Some(b),None)), (3,(Some(c),None)), (4,(None,Some(c1))))
//     */
//  }
//
//  //1.创建SparkConf
//  val sparkConf = new SparkConf().setAppName("SaleApp").setMaster("local[*]")
//
//  //2.创建streamingContext
//  val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
//
//  //3.消费三个主题的数据
//  val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstant.GMALL_EVENT))
//  val orderDetailInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstant.GMALL_EVENT))
//  val userInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstant.GMALL_EVENT))
//
//  //4.将数据转换成样例类，同时转换结构 (orderInfo.id, orderInfo)
//  val orderInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.map(record => {
//    val jsonString: String = record.value()
//    // 1 转换成 case class
//    val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
//    // 2 脱敏 电话号码 1381*******
//    val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
//    orderInfo.consignee_tel = telTuple._1 + "*******"
//    // 3 补充日期字段
//    val datetimeArr: Array[String] = orderInfo.create_time.split(" ")
//    orderInfo.create_date = datetimeArr(0) //日期
//    val timeArr: Array[String] = datetimeArr(1).split(":")
//    orderInfo.create_hour = timeArr(0) //小时
//    (orderInfo.id, orderInfo)
//  })
//
//  // (orderDetail.order_id, orderDetail)
//  val orderDetailDStream: DStream[(String, OrderDetail)] =
//    orderDetailInfoKafkaDStream.map(record => {
//      val orderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
//      (orderDetail.order_id, orderDetail)
//    })
//  //5.order和order_detail做join
//
//  val joinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] =
//    orderInfoDStream.fullOuterJoin(orderDetailDStream)
//
//
//  //6.处理joinDStream业务逻辑
//  val noUserInfoDStream = joinDStream.mapPartitions(iter => {
//    //清空保存数据的集合
//    noUserSaleDetails.clear()
//    //获取 redis 连接，自己封装的工具类
//    val jedisClient = RedisUtil.getJedisClient
//    //导入隐式转换，将样例类转换为json字符串
//    import org.json4s.native.Serialization
//    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
//
//    iter.foreach {
//      case (orderId, (orderInfoOpt, orderDetailOpt)) =>
//        //定义redis的key
//        val orderKey = s"order:$orderId"
//        val detailKey = s"detail:$orderId"
//
//        //判断order是否为空
//        if (orderInfoOpt.isDefined) {
//          //order
//          val orderInfo = orderInfoOpt.get
//          if (orderDetailOpt.isDefined) {
//            //order_detail不为空，保存到集合 => 订单数据来晚了
//            val orderInfoDetail = orderDetailOpt.get
//            noUserSaleDetails += new SaleDetail(orderInfo, orderInfoDetail)
//          }
//
//          //orderInfo数据无条件写入redis
//          //scala 将样例类对象转换为json字符串
//          val jsonStr: String = Serialization.write(orderInfo)
//
//          jedisClient.setnx(orderKey, jsonStr)
//          //设置过期时间
//          jedisClient.expire(orderKey, 300)
//
//          //无条件查询detail缓存
//          val details: util.Set[String] = jedisClient.smembers(detailKey)
//          import collection.JavaConversions._
//          details.foreach((detail: String) => {
//            //将str => orderDetail
//            val orderDetail = JSON.parseObject(detail, classOf[OrderDetail])
//            noUserSaleDetails += new SaleDetail(orderInfo, orderDetail)
//          })
//        } else {
//          //order 为空，则 order_detail 一定不为空
//          val orderDetail = orderDetailOpt.get
//          //获取order缓存数据，如果有直接join，如果没有把自己写入redis
//          val orderStr = jedisClient.get(orderKey)
//          if (orderStr != null) {
//            //存在 => 订单详情数据来晚了
//            val orderInfo = JSON.parseObject(orderStr, classOf[OrderInfo])
//            noUserSaleDetails += new SaleDetail(orderInfo, orderDetail)
//          } else {
//            //不存在 => 订单详情数据来早了
//            val orderDetailStr = Serialization.write(orderDetail)
//            jedisClient.sadd(detailKey, orderDetailStr)
//            //设置过期时间
//            jedisClient.expire(detailKey, 300)
//          }
//
//        }
//    }
//
//    //关闭连接
//    jedisClient.close()
//
//    noUserSaleDetails.toIterator
//  }
//}
//
