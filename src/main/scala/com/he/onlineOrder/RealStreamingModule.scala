package com.he.onlineOrder

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从Kafka Topic中获取数据，实时统计分析订单数据（窗口统计Top5订单量和累加订单销售额）
  */
object RealStreamingModule {

  // 设置SparkStreaming应用的Batch Interval
  val STREAMING_BATCH_INTERVAL = Seconds(5)

  // Streaming实时应用检查点目录
  val CHECK_POINT_PATH = "/datas/spark/streaming/order-chkpt-0000000"

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
    // TODO: a. 从Kafka读取流式数据


    // TODO: b. 依据业务需求对数据进行分析处理

    
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
