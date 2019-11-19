package com.he.onlineOrder

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
  * 从Kafka Topic中读取数据，采用Direct方式，实时累加统计各省份销售订单额
  */
object MapWithStateKafkaStreaming {

  // 检查点目录
  val CHECK_POINT_PATH: String = "/datas/spark/streaming/chkpt-2222222"

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
    val orderDStream: DStream[(Int, Double)] = kafkaDStream.transform((rdd, time) => {
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
        // TODO: 针对使用mapWithState的时候，建议先聚合，在更新状态
        .reduceByKey(_ + _)
    })

    /**
      * 实时累加统计函数：
      *   def mapWithState[StateType: ClassTag, MappedType: ClassTag](
            spec: StateSpec[K, V, StateType, MappedType]
          ): MapWithStateDStream[K, V, StateType, MappedType]

        1. 针对每一条数据进行处理，更新状态信息的
          StateSpec 是状态封装类，里面保存如何更新状态、保存状态、获取状态的函数
            def function[KeyType, ValueType, StateType, MappedType](
              mappingFunction: (KeyType, Option[ValueType], State[StateType]) => MappedType
            ): StateSpec[KeyType, ValueType, StateType, MappedType]
            上面的函数，定义了如何更新状态

        2. 函数中泛型说明（结合实例业务说 -> 统计各省份销售订单额）：
          StateType -> 表示的是 存储Key的状态的数据类型
            针对业务来说，各个省份的销售订单额，总的订单额，Double类型
          MappedType -> 表示每条数据处理以后，返回值的类型，map函数式一对一的关系，
            针对业务来说，每个省份有订单数据以后，更新总的销售额，返回的值，需要（省份Id, 总销售订单额）
      */
    // 定义 每条数据如何更新状态
    val  mappingFunction =
      (provinceId: Int, orderPrice: Option[Double], state: State[Double]) => {
        // i. 获取当前 省份 对应的 以前的状态
        val previousState: Double = state.getOption().getOrElse(0.0)
        // ii. 最新状态
        val lastestState: Double = previousState + orderPrice.getOrElse(0.0)
        // iii. 更新状态
        state.update(lastestState)
        // iv. 返回值，后续要知道最新的状态数据
        (provinceId, lastestState)
    }
    // 调用mapWithState实时状态统计（更细操作）
    val provinceOrderTotalDStream: DStream[(Int, Double)] = orderDStream.mapWithState(
      StateSpec.function[Int, Double, Double, (Int, Double)](mappingFunction)
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
          .setAppName("MapWithStateKafkaStreaming")
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
