package com.he.onlineETL

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.he.util.JedisPoolUtil
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * SparkStreaming采用Direct方式从Kafka Topic中读取数据JSON格式数据，按照Tag字段将数据插入到不同的HBase表中
  *     此处使用RDD自定义分区器实现功能
  * TODO：HBase 数据库中，表的说明：
  *   为了简单期间，orderType类型有四种，所以HBase数据库中有四张表，每张表的列簇为info，仅有一个列为value
  * 创建表的时候，设置预分区，表的ROWKEY就是orderId（模拟产生订单数据的时候，订单ID使用UUID随机生成）
  *   etl_alipay, etl_weixin, etl_card, etl_other
  * 预分区RowKey的值：
  *   484e652e-bd60-43c2-b3e7-95f8b3b7cba7
  *   82fed365-b055-4074-a125-2242d68216e9
  *   e44263cf-9129-4bba-a2e0-d5c1e1859406
  * HBase Shell 操作：
  *   - 创建命名空间
  *     create_namespace 'etlns'
  *   - 创建表，指定预分区和使用snappy压缩
  *     create 'etlns:etl_alipay', {NAME => 'info', COMPRESSION => 'SNAPPY'}, SPLITS => ['484e652e-', '82fed365-', 'e44263cf-']
  *     create 'etlns:etl_weixin', {NAME => 'info', COMPRESSION => 'SNAPPY'}, SPLITS => ['484e652e-', '82fed365-', 'e44263cf-']
  *     create 'etlns:etl_card', {NAME => 'info', COMPRESSION => 'SNAPPY'}, SPLITS => ['484e652e-', '82fed365-', 'e44263cf-']
  *     create 'etlns:etl_other', {NAME => 'info', COMPRESSION => 'SNAPPY'}, SPLITS => ['484e652e-', '82fed365-', 'e44263cf-']
  */
object ETLKafka2HBaseOffsets {

  // 记录程序中相关日志信息
  val LOGGER: Logger = Logger.getLogger(ETLKafka2HBase.getClass)

  // HBase 表中的列簇名称
  val HBASE_ETL_TABLE_FAMILY_BYTES: Array[Byte] = Bytes.toBytes("info")
  // HBase 表中列名称
  val HBASE_ETL_TABLE_COLUMN_BYTES: Array[Byte] = Bytes.toBytes("value")

  // 将Topic中各个分区Partition消费的最新偏移量信息存储在Redis中
  val REDIS_ETL_KEY: String = "streamin:etl:offsets"

  /**
    * Spark Application应用程序的入口，创建SparkContext实例对象，读取数据和调度Job执行
    * @param args
    */
  def main(args: Array[String]): Unit = {

    /**
      * 1. 构建StreamingContext流式上下文实例对象，用于读取数据流式数据
      */
    // 创建SparkConf实例对象，设置应用配置信息
    val sparkConf = new SparkConf()
      .setMaster("local[3]").setAppName("ETLKafka2HBase Application")
      // 设置每秒中每个分区读取数据最大条目数
      .set("spark.streaming.kafka.maxRatePerPartition", "3000")
    // 构建StreamingContext实例对象，设置批次处理时间间隔batchInterval
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 设置日志级别
    ssc.sparkContext.setLogLevel("WARN")


    /**
      * 2. 从流式数据源实时读取数据，进行ETL存储到HBase表中
      */
    // 从Kafka中读取数据的相关配置信息的设置
    val kafkaParams: Map[String, String] = Map(
      "metadata.broker.list" -> "bigdata-training01.erongda.com:9092,bigdata-training01.erongda.com:9093,bigdata-training01.erongda.com:9094",
      "auto.offset.reset" -> "largest"
    )
    // 从Redis中读取每个Topic中各个分区消费的最新偏移量数据
    val fromOffsets: Map[TopicAndPartition, Long] = {
      // 第一步、获取Jedis连接
      val jedis = JedisPoolUtil.getJedisPoolInstance.getResource

      // 第二步、依据Key获取所有field和value数据
      val offsetMap: util.Map[String, String] = jedis.hgetAll(REDIS_ETL_KEY)

      // 第三步、组装TopicAndPartition -> Long 实例，放入Map（Scala）中
      var tpoMap = scala.collection.mutable.Map[TopicAndPartition, Long]()
      import scala.collection.JavaConverters._
      for((field, value) <- offsetMap.asScala){
        val splited = field.split("_")
        tpoMap += TopicAndPartition(splited(0), splited(1).toInt) -> value.toLong
      }

      // 关闭Jedis连接
      JedisPoolUtil.release(jedis)

      // 第四步，返回从Redis中获取偏移量数据
      tpoMap.toMap
    }
    // 表示从Kafka Topic中获取每条数据以后的处理方式，此处获取Offset和Message（Value）
    val messageHandler = (mam: MessageAndMetadata[String, String]) => (mam.offset, mam.message())
    // 采用Direct方式从Kafka Topic中读取ETLTopic订单数据
    val kafkaDStream: DStream[(Long, String)] = KafkaUtils.createDirectStream[String, String,
      StringDecoder, StringDecoder, (Long, String)](
      ssc, //
      kafkaParams, //
      fromOffsets, //
      messageHandler
    )
    // kafkaDStream.print(20)

    // 解析获取KafkaDStream数据，对每批次RDD进行重分区，最终将数据插入大HBase表中
    kafkaDStream.foreachRDD((rdd ,time) => {

      // TODO：kafkaDStream直接从Topic中读取数据的DStream实例对象，DStream中每批次RDD是KafkaRDD
      // TODO：RDD中包含了每个分区Partition数据来源于Topic中每个分区的偏移量信息（fromOffset和untilOffset）
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val batchTime = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS").format(new Date(time.milliseconds))
      println(s"================= Batch Time = $batchTime =================")
      // TODO: a. 针对RDD每个分区数据进行操作，解析Message的JSON格式数据，获取orderType类型
      val orderRDD: RDD[(String, String)] = rdd.mapPartitions(iter => {
        iter.map{ case(_, msg) =>
          // 解析获取orderType
          val orderType = JSON.parseObject(msg).getString("orderType")
          // 返回
          (orderType, msg)
        }
      })

      // TODO: b. 对orderRDD进行重分区，分区的数目为orderType类别数目，将相同的orderType放在同一个分区中
      val partitionerRDD: RDD[(String, String)] = orderRDD.partitionBy(new OrderTypePartitioner())

      // TODO: c. 将相同支付类型的订单数据（存储在同一个RDD分区中）插入到HBase表中
      partitionerRDD.foreachPartition(iter => {
        // 由于RDD中每个分区的数据被一个Task处理，每个Task运行的时候有TaskContext上下文对象，可以获取分区ID
        TaskContext.getPartitionId() match {
          case 0 => insertIntoHBase("etlns:etl_alipay", iter)
          case 1 => insertIntoHBase("etlns:etl_weixin", iter)
          case 2 => insertIntoHBase("etlns:etl_card", iter)
          case _ => insertIntoHBase("etlns:etl_other", iter)
        } //
      }) //

      // TODO: d. 当每批次RDD数据插入到HBase成功以后，就可以更新OFFSET信息
      // 第一步、获取Jedis连接
      val jedis = JedisPoolUtil.getJedisPoolInstance.getResource
      // 第二步、更新Redis中偏移量数据
      for(offsetRange <- offsetRanges){
        jedis.hset(
          REDIS_ETL_KEY, //
          offsetRange.topic + "_" + offsetRange.partition, //
          offsetRange.untilOffset.toString//
        ) //
      }
      // 第三步、关闭Jedis连接
      JedisPoolUtil.release(jedis)
    })
    /**
      * 3. 启动实时流式应用，实时获取流式数据并处理ETL
      */
    ssc.start()
    // 实时应用一旦运行起来，正常情况下不会自动的停止，除非遇到特殊情况，如人为终止程序，或程序出现异常，需要等待
    ssc.awaitTermination()

    // 停止StreamingContext
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

  /**
    * 将JSON格式数据插入到HBase表中，其中标的ROWKEY为orderId（解析JSON格式获取）
    * @param tableName
    *                  表的名称
    * @param iter
    *             迭代器，二元中Value为存储的数据，在HBase表中列名为info:value
    */
  def insertIntoHBase(tableName: String, iter:  Iterator[(String, String)]): Unit = {

    // a. 获取HBase Connection
    val hbaseConf = HBaseConfiguration.create()
    val hbaseConn = ConnectionFactory.createConnection(hbaseConf)

    // b. 获取表的句柄
    val table: HTable = hbaseConn.getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]

    // c. 迭代数据插入表中
    import java.util
    val puts: util.ArrayList[Put] = new util.ArrayList[Put]()
    iter.foreach{ case(_, jsonValue) =>
      // 解析JSON格式数据，获取RowKey
      val rowkey = JSON.parseObject(jsonValue).getString("orderId")

      // 创建Put对象
      val put = new Put(Bytes.toBytes(rowkey))

      // 添加列
      put.addColumn(
        HBASE_ETL_TABLE_FAMILY_BYTES, //
        HBASE_ETL_TABLE_COLUMN_BYTES, //
        Bytes.toBytes(jsonValue) //
      )

      // 将put加入到list中
      puts.add(put)
    }

    // d. 批量插入数据到HBase表
    table.put(puts)

    // e. 关闭连接
    hbaseConn.close()
  }

}
