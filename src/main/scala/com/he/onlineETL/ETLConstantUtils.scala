package com.he.onlineETL

/**
  * 存储一些常量数据
  */
object ETLConstantUtils {

  /**
    * 定义KAFKA相关集群配置信息
    */
  // KAFKA CLUSTER BROKERS
  val METADATA_BROKER_LIST = "bigdata-training01.erongda.com:9092,bigdata-training01.erongda.com:9093,bigdata-training01.erongda.com:9094"

  // OFFSET
  val AUTO_OFFSET_RESET = "largest"

  // 序列化类
  val SERIALIZER_CLASS = "kafka.serializer.StringEncoder"

  // 发送数据方式
  val PRODUCER_TYPE = "async"

  // Topic的名称
  val ORDER_TOPIC = "etlTopic"
}
