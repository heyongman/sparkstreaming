package com.he.onlineOrder

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.fasterxml.jackson.databind.ObjectMapper
import com.he.onlineOrder.utils.{ConstantUtils, ObjectMapperSingleton, SaleOrder}
import com.he.util.JedisPoolUtil
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{HashPartitioner, SparkConf}

/**
  * 从Kafka Topic中获取数据，实时统计分析订单数据（窗口统计Top5订单量和累加订单销售额）
  */
object RealOrderStreaming {

  val logger: Logger = Logger.getLogger(RealOrderStreaming.getClass)

  // 设置SparkStreaming应用的Batch Interval
  val STREAMING_BATCH_INTERVAL = Seconds(5)
  // 设置窗口时间间隔
  val STREAMING_WINDOW_INTERVAL = STREAMING_BATCH_INTERVAL * 3
  // 设置滑动时间间隔
  val STREAMING_SLIDER_INTERVAL = STREAMING_BATCH_INTERVAL * 2

  // Streaming实时应用检查点目录
  val CHECK_POINT_PATH = "/datas/spark/streaming/order-chkpt-0000009"

  // 存储Redis中实时统计销售额
  val REDIS_KEY_ORDERS_TOTAL_PRICE = "orders:total:price"

  /**
    * 贷出模式中贷出函数：管理StreamingContext流式上下文对象创建与管理
     * @param args
    *             应用程序接收参数
    * @param operation
    *                  用户函数：读取流式数据，依据需要分析处理输出
    */
  def sparkOperation(args: Array[String])(operation: StreamingContext => Unit): Unit = {

    /**
      * 当第一次运行 SparkStreaming应用的时候，创建StreamingContext实例对象
      */
    val creatingFunc = () => {
      // TODO: a. 构建SparkConf应用配置
      val sparkConf: SparkConf = new SparkConf()
        .setMaster("local[3]")
        .setAppName("RealOrderStreaming")
        // 每秒钟获取Topic中每个分区的最大条目, 比如设置1000条，Topic三个分区，每秒钟最大数据量为30000, 批处理时间间隔为5秒，则每批次处理数据量最多 150000
        .set("spark.streaming.kafka.maxRatePerPartition", "10000")
        // 设置数据处理本地性等待超时时间
        .set("spark.locality.wait", "100ms")
        // 设置使用Kryo序列化, 针对非基本数据类型，进行注册
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(Array(classOf[SaleOrder]))
        // 设置启用反压机制
        .set("spark.streaming.backpressure.enabled", "true")
        // 设置Driver JVM的GC策略
        .set("spark.driver.extraJavaOptions", "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC")

      // TODO: b. 设置StreamingContext批次处理时间间隔为 5s
      val ssc: StreamingContext = new StreamingContext(sparkConf, STREAMING_BATCH_INTERVAL)
      // 设置日志级别
      ssc.sparkContext.setLogLevel("WARN")
      // TODO: c. 处理数据
      operation(ssc)
      // TODO： d. 设置检查点目录
      ssc.checkpoint(CHECK_POINT_PATH)
      // 返回值
      ssc
    }

    // 创建StreamingContext实例对象
    var context: StreamingContext = null
    try{
      // 依据实时流式运行的第一次还是失败恢复运行不同，创建StreamingContext不同
      context = StreamingContext.getActiveOrCreate(CHECK_POINT_PATH, creatingFunc)
      context.sparkContext.setLogLevel("WARN")
      // 启动实时应用
      context.start()
      // 等待实时应用的终止
      context.awaitTermination()
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      // 停止StreamingContext
      if(null != context) context.stop(stopSparkContext = true, stopGracefully = true)
    }
  }

  /**
    * 贷出模型中的用户函数，实时获取Kafka Topic中数据，依据业务分析统计
    * @param ssc
    *            StreamingContext流式实例对象
    */
  def processStreamingData(ssc: StreamingContext): Unit ={

    /** ============== a. 从Kafka Topic 中读取数据，采用Direct方式  ==============*/
    // a. Kafka Brokers 配置参数信息
    val kafkaParams: Map[String, String] = Map(
      "metadata.broker.list" -> ConstantUtils.METADATA_BROKER_LIST, //
      "auto.offset.reset" -> ConstantUtils.AUTO_OFFSET_RESET
    )
    // b. 从哪些Topic中读取数据，此处可以设置多个Topic
    val topics: Set[String] = Set("saleOrderTopic")
    // c. 采用Direct方式，使用Simple Consumer API读取数据
    val kafkaDStream: DStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, // StreamingContext
      kafkaParams, // Map[String, String]
      topics // Set[String]
    ) //

    /** ============== b. 解析Message数据为SaleOrder格式数据  ==============*/
    val orderDStream: DStream[(Int, SaleOrder)] = kafkaDStream.transform(rdd => {
      rdd.mapPartitions(iter => {
        iter.map{ case (_, message) =>
          val mapper: ObjectMapper = ObjectMapperSingleton.getInstance()
          // 解析JSON格式的数据为SaleOrder对象
          val saleOrder = mapper.readValue(message, classOf[SaleOrder])
          // 返回
          (saleOrder.provinceId, saleOrder)
        }
      })
    })
    // orderDStream.print()

    /** ============== c. 处理订单数据：实时累加统计各省份销售额 ==============*/
    val orderPriceDStream: DStream[(Int, Float)] = orderDStream.updateStateByKey(
      // updateFunc: (Time, K, Seq[V], Option[S]) => Option[S]
      (batchTime: Time, provinceId: Int, orders: Seq[SaleOrder], state: Option[Float]) => {
        // i. 获取当前批次Key的状态信息
        val currentOrderPrice: Float = orders.map(_.orderPrice).sum
        // ii. 获取Key以前的状态信息
        val previousPrice = state.getOrElse(0.0f)
        // iii. 更新并返回
        Some(previousPrice + currentOrderPrice)
      }, //
      new HashPartitioner(ssc.sparkContext.defaultParallelism), //
      rememberPartitioner = true
    )
    // orderPriceDStream.print(10)

    /** ============== d. 将实时统计分析各省份销售订单额存储Redis中  ==============*/
    orderPriceDStream.foreachRDD((rdd, time) => {
      // 处理时间
      val batchTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(time.milliseconds))
      logger.warn("-------------------------------------------------")
      logger.warn(s"batchTime: $batchTime")
      logger.warn("-------------------------------------------------")
      if(!rdd.isEmpty()){
        // TODO： 降低分区数据，将结果存储到Redis数据库中，Value数据类型为哈希
        rdd.coalesce(1).foreachPartition(iter => {
          // TODO：i. 获取Jedis连接
          val jedis = JedisPoolUtil.getJedisPoolInstance.getResource
          // TODO: ii. 将统计销售订单额存储到Redis中
          iter.foreach{
            case (provinceId, orderTotal) =>
              jedis.hset(REDIS_KEY_ORDERS_TOTAL_PRICE, provinceId.toString, orderTotal.toString)
              logger.warn(s"============ provinceId: $provinceId, orderTotal: $orderTotal")
          }
          // TODO: iii. 关闭连接
          JedisPoolUtil.release(jedis)
        })
      }
    })

    /** ============== e. 实时统计出最近时间段内订单量最高前10个省份==============*/
    // 连接数据属性信息
    val (url,  props) = {
      // 连接数据库的URL
      val jdbcUrl = "jdbc:mysql://bigdata-training01.erongda.com:3306/"
      // JDBC database connection arguments, 至少包含连接数据库用户名和密码
      val jdbcProps: Properties = new Properties()
      jdbcProps.put("user", "root")
      jdbcProps.put("password", "123456")

      // 返回连接数据信息
      (jdbcUrl, jdbcProps)
    }

    orderDStream
      // 设置窗口大小和滑动大小
      .window(STREAMING_WINDOW_INTERVAL, STREAMING_SLIDER_INTERVAL)
      // 集成SparkSQL完成
      .foreachRDD((rdd, time) => {
        // 处理时间
        val batchTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(time.milliseconds))
        logger.warn(s"batchTime: $batchTime")
        if(!rdd.isEmpty()){
          // RDD 数据缓存
          rdd.cache()

          // TODO：i. 构建SparkSession实例对象
          val spark: SparkSession = SparkSession.builder()
            .config(rdd.sparkContext.getConf)
            .config("spark.sql.shuffle.partitions", "6")
            .getOrCreate()
          import spark.implicits._

          // TODO: ii. 将RDD转换为Dataset或DataFrame，由于RDD中数据类型SaleOrder为CaseClass，隐式转换
          val saleOrderDF = rdd.map(_._2).toDF()

          // TODO: iii. 编写DSL分析
          val top10ProvinceOrderCountDF: Dataset[Row] = saleOrderDF
            .groupBy($"provinceId").count()
            .orderBy($"count".desc).limit(10)

          // TODO: iv. 将结果存储MySQL数据库表中
          top10ProvinceOrderCountDF.write
            .mode(SaveMode.Overwrite)
            .jdbc(url, "test.order_top10_count", props)

          // TODO: 作业1，考虑使用SQL分析，将DataFrame注册为临时视图，编写SQL完成
          // TODO: 作业2，考虑使用RDD完成Top10操作，考虑使用aggregateByKey函数完成

          // 释放RDD的数据
          rdd.unpersist()
        }
    })

  }

  /**
    * Spark Application程序运行的入口
    * @param args
    *             传递的参数
    */
  def main(args: Array[String]): Unit = {
    // 调用贷出函数，传递用户函数
    sparkOperation(args)(processStreamingData)
  }
}
