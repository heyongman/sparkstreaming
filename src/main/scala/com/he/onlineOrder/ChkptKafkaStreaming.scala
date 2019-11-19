package com.he.onlineOrder

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从Kafka Topic中读取数据，采用Direct方式，实时累加统计各省份销售订单额
  */
object ChkptKafkaStreaming {

  // 检查点目录
  val CHECK_POINT_PATH: String = "/datas/spark/streaming/chkpt-000000000"

  /**
    * 真正业务逻辑实现代码的地方，从Kafka Topic中读取数据，实时累加统计各省份销售订单额
    * @param ssc
    */
  def processData(ssc: StreamingContext): Unit = {
    /**
      * TODO: 2. 从数据源端读取数据
      */
    // a. Kafka Brokers 配置参数信息
    val kafkaParams: Map[String, String] = Map(
      "metadata.broker.list" -> "bigdata-training01.erongda.com:9092,bigdata-training01.erongda.com:9093,bigdata-training01.erongda.com:9094", //
      "auto.offset.reset" -> "largest"
    )
    // b. 从哪些Topic中读取数据，此处可以设置多个Topic
    val topics: Set[String] = Set("orderTopic")
    // c. 采用Direct方式，使用Simple Consumer API读取数据
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, // StreamingContext 流式上下文对象
      kafkaParams, // 连接Kafka Cluster配置参数信息
      topics // 从哪些Topic中读取数据
    )


    /**
      * TODO：3. 按照需求分析 -> 实时累加统计各省份销售订单额
      */
    val provinceOrderTotalDStream = kafkaDStream.transform((rdd, time) => {
      // TODO：针对RDD进行操作
      rdd
        .filter(msg => null != msg._2 && msg._2.trim.length > 0 && msg._2.trim.split("\\,").length >= 3)
        // 每个省份出现一次订单, 获取省份ID和订单销售额
        .mapPartitions(iter => {
        // 针对每个分区数据操作： TODO：RDD中每个分区数据对应于Kafka Topic中每个分区的数据
        iter.map(item => {
          val Array(_, provinceId, orderPrice) = item._2.split(",")
          // 返回二元组，按照省份ID进行统计订单销售额，所以省份ID为Key
          (provinceId.toInt, orderPrice.toDouble)
        })
      })
    }).updateStateByKey(
      // updateFunc: (Seq[V], Option[S]) => Option[S]
      // TODO: 针对此应用来说，V就是每个订单销售额为Double类型，状态就是各省份总的订单销售额为Double类型
      (values: Seq[Double], state: Option[Double]) => {
        // i. 获取当前Key的以前状的数据
        val previousState: Double = state.getOrElse(0.0)
        // ii. 计算当前批次中Key的状态
        val currentState = values.sum
        // iii. 合并状态信息并返回
        Some(previousState + currentState)
      }
    )


    /**
      * TODO： 4. 将分析的结果打印在控制台上
      */
    provinceOrderTotalDStream.foreachRDD((rdd, time) => {
      println("-------------------------------------------")
      println(s"Batch Time: ${new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS").format(new Date(time.milliseconds))}")
      println("-------------------------------------------")
      if (!rdd.isEmpty()) {
        rdd.coalesce(1).foreachPartition(iter => iter.foreach(println))
      }
      println()
    })
  }

  def main(args: Array[String]): Unit = {

    // 设置整个应用日志级别
    // Logger.getRootLogger.setLevel(Level.WARN)

    /**
      * TODO: 1. 构建StreamingContext实例对象，调用StreamingContext#getOrCreate
      */
    val context: StreamingContext = StreamingContext.getOrCreate( CHECK_POINT_PATH,
      // creatingFunc: () => StreamingContext
      () => {
        // 设置Spark Application基本信息（应用名称和运行模型、配置参数等）
        val conf: SparkConf = new SparkConf()
          .setMaster("local[3]") // 启动三个线程Thread运行应用
          .setAppName("ChkptKafkaStreaming")
        // TODO: batchDuration: Duration 表示的是每批次Batch的时间间隔, 此处设置为 5秒
        val ssc = new StreamingContext(conf, Seconds(5))
        // 设置日志级别
        ssc.sparkContext.setLogLevel("WARN")

        /**
          * 设置检查点目录
          * a. 保存状态State信息
          * b. 保存从Kafka Topic中采用Direct方式读取数据，以及最近消费的偏移量信息
          * c. 保存DStream的来源、DStream数据处理函数操作与输出函数操作
          */
        ssc.checkpoint(CHECK_POINT_PATH)

        // TODO: 读取数据流式数据，实时处理
        processData(ssc)

        ssc
      } //
    )

    // 如果从检查点目录构建StreamingContext，需求重新设置日志级别
    context.sparkContext.setLogLevel("WARN")

    /**
      * TODO：5. 启动实时流式应用，开始准备接收数据并进行处理分析
      */
    context.start()
    // 实时应用一旦运行起来，正常情况下不会自动的停止，除非遇到特殊情况，如人为终止程序，或者程序出现异常，需要等待
    context.awaitTermination()  // 方法中实现代码 -> waiter.waitForStopOrError()

    // 停止StreamingContext, 此处建议使用下面方式停止应用
    context.stop(stopSparkContext = true, stopGracefully = true)
  }

}
