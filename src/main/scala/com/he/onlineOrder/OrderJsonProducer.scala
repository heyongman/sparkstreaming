package com.he.onlineOrder

import java.util.{Properties, UUID}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.he.onlineOrder.utils.{ConstantUtils, RandomUtils, SaleOrder}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.collection.mutable.ArrayBuffer

/**
  * 模拟产生订单数据Order
  */
object OrderJsonProducer {

  def main(args: Array[String]): Unit = {

    /**
      * 使用Jackson ObjectMapper类，主要提供将Java对象批JSON结构数据
      */
    val mapper: ObjectMapper =   new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    /**
      * Kafka Producer API使用
      */
    // Producer Configs
    val props = new Properties()
    props.put("metadata.broker.list", ConstantUtils.METADATA_BROKER_LIST)
    props.put("producer.type", ConstantUtils.PRODUCER_TYPE)
    props.put("serializer.class", ConstantUtils.SERIALIZER_CLASS)
    props.put("key.serializer.class", ConstantUtils.SERIALIZER_CLASS)
    // Kafka Producer 实例对象声明
    var producer: Producer[String, String] = null

    try{
      //
      val config: ProducerConfig = new ProducerConfig(props)
      // 构建Kafka Producer实例对象
      producer = new Producer[String, String](config)

      // 采用每次发送多条数据，使用ArrayBuffer存储
      val mgsArrayBuffer = new ArrayBuffer[KeyedMessage[String, String]]()

      // TODO: 模拟一直产生订单数据，每次产生N条数据，此处使用while死循环模拟发送数据
      while(true){
        // 清空数组中Message数据
        mgsArrayBuffer.clear()

        // 每次循环产生多少条订单数据，发送到Topic中
        val randomNumber = RandomUtils.getRandomNum(10000) + 50000
        // TODO: startTime
        val startTime = System.currentTimeMillis()
        // 循环产生多条数据
        for(index <- 0 until randomNumber){
          // TODO: 构建订单数据SaleOrder，转换为JSON格式字符串
          val orderItem: SaleOrder = {
            // 订单ID, 采用UUID产生随机数
            val orderId = UUID.randomUUID().toString
            // 省份ID
            val provinceId = RandomUtils.getRandomNum(34) + 1
            // 订单金额
            val orderPrice = RandomUtils.getRandomNum(80) + 0.5f
            // 构建订单实例对象，并返回
            SaleOrder(orderId, provinceId, orderPrice)
          }
          // CaseClass对象转换为JSON字符串
          val orderJson = mapper.writeValueAsString(orderItem)
          // def this(topic: String, key: K, message: V)
          mgsArrayBuffer += new KeyedMessage(ConstantUtils.ORDER_TOPIC, orderItem.orderId, orderJson)
        }

        // TODO: 如何调用API，发送数据到Topic中, def send(messages: KeyedMessage[K,V]*)
        producer.send(mgsArrayBuffer:_ *)

        // TODO: endTime
        val endTime = System.currentTimeMillis()
        println(s"--------- Send Messages: $randomNumber, Spent Time: ${endTime - startTime} ------")

        // 每次发送数据以后，稍微休息
        Thread.sleep(RandomUtils.getRandomNum(10)  * 100)
      }
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      if(null != producer) producer.close()
    }
  }

}
