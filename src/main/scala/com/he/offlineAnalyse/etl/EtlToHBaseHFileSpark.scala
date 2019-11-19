package com.he.offlineAnalyse.etl

import java.util
import java.util.zip.CRC32

import com.he.common.EventLogConstants
import com.he.common.EventLogConstants.EventEnum
import com.he.util.{LogParser, TimeUtil}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, KeyValue, TableName}
import org.apache.hadoop.mapreduce.{Job => NewAPIHadoopJob}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * 基于SparkCore框架读取HDFS上日志文件数据，进行ETl操作，最终将数据插入到HBase表中
  *   -a. 为什么选择ETL数据到HBase表中？？？？
  *     采集的数据包含很多Event事件类型的数据，不同Event事件类型的数据在字段是不一样只的，数据量相当大的
  *   -b. HBase表的设计？？？
  *     -i. 每天的日志数据，ETL到一张表中（一天一张表）
  *       本系统，主要针对日志数据进行分析，基本上每天的数据分析一次，为了分析更快，加载更少的数据
  *     -ii. 每次ETL数据的时候，创建一张表
  *       表的名称：（前缀）event_logs + （后缀）日期（年月日），每次先判断表是否存在，存在先删除，后创建
  *       第一、创建表的时候，考虑创建预分区、使得加载数据到表中的手，数据分布在不同的Region中，减少写热点，避免Region Split
  *       第二、可以考虑表中的数据压缩，使用snappy或Lz4压缩
  *     -iii. RowKey 设计原则：
  *       - 唯一性（不重复）
  *       - 结合业务考虑
  *         某个时间段数据分析（时间）、某个EventType数据分析
  *       RowKey = 服务器时间戳 + CRC32编码（用户ID、会员ID、事件名称）
  *     -iv. 列簇和列
  *       只有一个列簇，名称为info，解析每条日志获取的Map中的Key就是列名，Value就是列值，考虑将服务器时间戳设置为每列数据的版本version
  */
object EtlToHBaseHFileSpark {

  // 记录开发程序日志
  val logger: Logger = Logger.getLogger(EtlToHBaseSpark.getClass)

  /**
    * Spark Application应用程序入口
    *
    * @param args
    * 程序的参数，实际业务中需要传递 处理哪天数据的日期（processDate）
    */
  def main(args: Array[String]): Unit = {

    // TODO: 需要传递一个参数，表明ETL处理的数据是哪一天的
    if (args.length < 1) {
      println("Usage: EtlToHBaseHFileSpark process_date")
      System.exit(1)
    }

    // TODO: 设置日志级别
    Logger.getRootLogger.setLevel(Level.WARN)

    /**
      * 创建SparkContext实例对象，读取数据，调度Job执行
      */
    val sparkConf = new SparkConf()
      .setMaster("local[3]").setAppName("EtlToHBaseHFileSpark Application")
      // 设置使用Kryo序列化
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Put]))
    // 构建SparkContext
    val sc = SparkContext.getOrCreate(sparkConf)


    /**
      * TODO: a. 依据传入的处理日期，读取存储在HDFS上日志文件
      */
    val eventLogsRDD: RDD[String] = sc.textFile(
      s"/datas/usereventlogs/${args(0)}/", // 拼凑HDFS数据存储路径
      minPartitions = 3 // 设置分区数
    )

    logger.warn(s"================== Count = ${eventLogsRDD.count()} ==================")
    logger.warn(eventLogsRDD.first())


    /**
      * TODO: b. 解析每条日志数据
      */
    // 日志数据格式：IP地址^A服务器时间^Ahost^A请求参数
    val parseEventLogsRDD: RDD[(String, util.Map[String, String])] = eventLogsRDD
      // 通过解析工具类，按照日志数据格式规则解析数据，存储到Map集合中
      .mapPartitions(iter => {
      iter.map(log => {
        // 调用工具类进行解析得到 Map集合
        val logInfo: util.Map[String, String] = new LogParser().handleLogParser(log)
        // 获取事件类型
        val eventAlias = logInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME)
        // 以二元组
        (eventAlias, logInfo)
      }) //
    })

    logger.warn(s"================== Parse Log First ==================")
    logger.warn(parseEventLogsRDD.first())

    // 使用列表List存储六种事件类型
    val eventTypeList = List(
      EventEnum.LAUNCH, EventEnum.PAGEVIEW, EventEnum.EVENT,
      EventEnum.CHARGEREQUEST, EventEnum.CHARGEREFUND, EventEnum.CHARGESUCCESS
    )
    // TODO: 通过广播变量将事件类型的列表广播给各个Executor（存储内存中）
    val eventTypeListBroadcast: Broadcast[List[EventEnum]] = sc.broadcast(eventTypeList)

    /**
      * TODO: c. 过滤无效数据（通用性，其他过滤在分析的数据进行）和转换数据格式
      */
    val eventsKeyValueRDD: RDD[(ImmutableBytesWritable, KeyValue)] = parseEventLogsRDD
      // 过滤事件类型EventType不存的数据和解析获取的Map集合为空
      .filter { case (eventAlias, logInfo) =>
        // logInfo.size() != 0 && eventTypeList.contains(EventEnum.valueOfAlias(eventAlias))
        logInfo.size() != 0 && eventTypeListBroadcast.value.contains(EventEnum.valueOfAlias(eventAlias))
      }
      // 数据转换，组装RDD数据，将数据保存到HBase表中：RDD[(ImmutableBytesWritable, Put)]
      .mapPartitions(iter => {
        iter.flatMap{ case (eventAlias, logInfo) =>
          // i. 表的主键RowKey的值
          val rowKey: String = createRowKey(
            TimeUtil.parseNginxServerTime2Long(logInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)), //
            logInfo.get(EventLogConstants.LOG_COLUMN_NAME_UUID), //
            logInfo.get(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID), //
            eventAlias
          )
          // 构建Bytes类型的RowKey
          val row: Array[Byte] = Bytes.toBytes(rowKey)
          // ii. 创建KeyValue对象，添加列名和列值
          val list = new ListBuffer[((String, String), KeyValue)]()
          // TODO: 注意此处需要将Java中Map集合转换为Scala中Map集合，方能进行操作
          import scala.collection.JavaConverters._
          for((key, value) <- logInfo.asScala){
            val keyvalue = new KeyValue(
              row, //
              EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME, // cf
              Bytes.toBytes(key.toString), // column
              Bytes.toBytes(value.toString) //
            ) //
            // 加入List中
            list += (rowKey, key) -> keyvalue
          }
          // iii. list列表
          list.toList
        }
      })
      // 由于HFile文件的数据按照RowKey升序排序，每行数据按照Column名称升序排序，所有进行排序操作
      .sortByKey(ascending = true)
      .map(tuple => (new ImmutableBytesWritable(Bytes.toBytes(tuple._1._1)), tuple._2))

    logger.warn(s"============== Transformation Count = ${eventsKeyValueRDD.count()} ==============")
    logger.warn(eventsKeyValueRDD.first())


    /**
      * TODO：d. 将RDD中数据保存到HBase表中
      */
    // i. 连接HBase 数据库Client配置信息
    val conf: Configuration = HBaseConfiguration.create()

    /**
      * 由于ETL每天执行一次（ETL失败，再次执行），对原始的数据进行处理，将每天的数据存储到HBase表中
      *     表的名称：
      *         create 'event_logs20151220', 'info'
      */
    val tableName: String = createHBaseTable(args(0), conf)

    // ii. 设置向哪张表写数据
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    // 创建Job
    val job = NewAPIHadoopJob.getInstance(conf)
    // HTable 对象
    val table: HTable = new HTable(conf, tableName)
    // 设置HFileOutputFormat2
    HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor, table.getRegionLocator)

    // 设置输出的HFile文件的目录
    val outputDir = "/datas/spark/hbase/etl/" + System.currentTimeMillis()
    logger.warn(s"-------------- HFile Output Path = $outputDir --------------")
    val outputPath = new Path(outputDir)

    // iii. 调用saveAsNewAPIHadoopFile函数保存数据
    eventsKeyValueRDD.saveAsNewAPIHadoopFile(
      outputDir, //
      classOf[ImmutableBytesWritable], //
      classOf[KeyValue], //
      classOf[HFileOutputFormat2], //
      job.getConfiguration //
    )

    // 当将数据转换为为KeyValue格式，并存储为HFile文件格式，以后直接批量加载数据到表中
    if(FileSystem.get(conf).exists(outputPath)){
      // 加载生成的HFile文件
      val load = new LoadIncrementalHFiles(job.getConfiguration)
      load.doBulkLoad(outputPath, table)

      // 删除输出目录
      FileSystem.get(conf).delete(outputPath, true)
    }

    // 为了开发测试，线程休眠，WEB UI监控查看
    Thread.sleep(10000000)

    // 关闭资源
    sc.stop()
  }


  /**
    * 依据字段信息构建RowKey
    * @param time
    *             访问服务器的时间
    * @param uuid
    *             用户ID（访问网站的时候，生成的全局GUID)
    * @param umd
    *            用户会员ID（注册会员用户）
    * @param eventAlias
    *                   事件Event名称
    * @return
    *         拼凑组合字符串
    */
  def createRowKey(time: Long, uuid: String, umd: String, eventAlias: String): String = {

    // 创建StringBuilder实例对象，用于拼接字符串
    val sBuilder = new StringBuilder()
    sBuilder.append(time + "_")

    // 创建CRC32实例对象，进行字符串编码，将字符串转换为Long类型数字
    val crc32 = new CRC32()
    // 重置
    crc32.reset()
    if(StringUtils.isNotBlank(uuid)){
      crc32.update(Bytes.toBytes(uuid))
    }
    if(StringUtils.isNotBlank(umd)){
      crc32.update(Bytes.toBytes(umd))
    }
    crc32.update(Bytes.toBytes(eventAlias))

    sBuilder.append(crc32.getValue)
    // return
    sBuilder.toString()
  }


  /**
    * 创建HBase表，创建的时候判断表是否存在，存在的话先删除后创建
    * @param processDate
    *                    要处理哪天数据的日期，格式为：2015-12-20
    * @param conf
    *             HBase Client要读取的配置信息
    * @return
    *         表的名称
    */
  def createHBaseTable(processDate: String, conf: Configuration): String = {
    // 处理时间格式
    val time: Long = TimeUtil.parseString2Long(processDate)  // "yyyy-MM-dd"
    val dateSuffix: String = TimeUtil.parseLong2String(time, "yyyyMMdd")

    // table name
    val tableName = EventLogConstants.HBASE_NAME_EVENT_LOGS + dateSuffix

    // 定义连接HBase Cluster
    var conn: Connection = null
    var admin: HBaseAdmin = null
    try{
      // 获取连接
      conn = ConnectionFactory.createConnection(conf)
      // 获取HBaseAdmin实例对象
      admin = conn.getAdmin.asInstanceOf[HBaseAdmin]

      // 判断表
      if (admin.tableExists(tableName)) {
        // 先禁用再删除
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
      }

      // 创建表的描述符TableDesc
      val desc = new HTableDescriptor(TableName.valueOf(tableName))
      // 创建表的列簇描述符
      val familyDesc = new HColumnDescriptor(EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME)

      /**
        * 针对列簇进行属性设置
        */
      // 由于HBase表中的数据主要业务为分析数据，而不是查询数据，所以进行查询缓存
      familyDesc.setBlockCacheEnabled(false)
      // 设置数据压缩
      // familyDesc.setCompressionType(Compression.Algorithm.SNAPPY)
      // 向表中添加列簇
      desc.addFamily(familyDesc)

      // 设置表的预分区，针对整个表来说的，不是针对某个列簇
      // Unit createTable(desc: HTableDescriptor, splitKeys: Array[Array[Byte]])
      admin.createTable(desc, //
        Array( Bytes.toBytes("1450570259817_"),
          Bytes.toBytes("1450570783007_"), Bytes.toBytes("1450571196341_") //
        ) //
      ) //
    }
    catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (null != admin) admin.close()
      if (null != conn) conn.close()
    }
    tableName
  }

}
