package com.he.offlineAnalyse.analystics

import java.util.{Calendar, Properties}

import com.he.common.EventLogConstants
import com.he.common.EventLogConstants.EventEnum
import com.he.util.TimeUtil
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{CompareFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 封装日期（当月的第几天）、平台和浏览器信息到CaseClass中，方便后续将RDD转换为DataFrame（通过反射自动推断类型）
  * @param uuid
  * @param day
  * @param platformDimension
  * @param browserDimension
  */
case class DayPlatformBrowser(
      uuid: String, day: Int, platformDimension: String, browserDimension: String)

/**
  * 基于SparkCore实现，从HBase表中读取数据，统计新增用户个数，按照不同维度进行分析
  */
object NewInstallUserCountSparkSQL {

  // 记录开发程序日志
  val logger: Logger = Logger.getLogger(NewInstallUserCountSpark.getClass)

  def main(args: Array[String]): Unit = {

    // TODO: 需要传递一个参数，表明处理的数据是哪一天的
    if (args.length < 1) {
      println("Usage: NewInstallUserCountSparkSQL process_date")
      System.exit(1)
    }

    // TODO: 设置日志级别
    Logger.getRootLogger.setLevel(Level.WARN)

    /**
      * TODO: 创建SparkContext实例对象，读取数据，调度Job执行
      */
    val sparkConf = new SparkConf()
      .setMaster("local[3]").setAppName("NewInstallUserCountSparkSQL Application")
      // 设置使用Kryo序列化
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result]))
    // 构建SparkContext
    val sc: SparkContext = SparkContext.getOrCreate(sparkConf)

    /**
      * TODO: 从HBase表中读取数据，依据需要进行过滤筛选，不同维度统计分析
      * 在此业务分析中，只需要 事件Event类型为launch类型的数据即可，字段信息如下：
          - en -> 过滤字段，事件类型，e_l 第一次加载网站
          - s_time: 访问服务器的时间，用于获取时间维度
          - version: 平台的版本
          - pl: platform 平台的名称
          - browserVersion：浏览器的名称
          - browserName: 浏览器的名称
          - uuid：用户ID，如果此字段的值为空，说明属于脏数据 ，不合格，过滤掉，不进行统计分析
      */
    // a. 创建Configuration对象，包含相关配置信息，比如HBase Client配置
    val conf: Configuration = HBaseConfiguration.create()

    // 处理时间格式
    val time: Long = TimeUtil.parseString2Long(args(0))  // "yyyy-MM-dd"
    val dateSuffix: String = TimeUtil.parseLong2String(time, "yyyyMMdd")
    // table name
    val tableName = EventLogConstants.HBASE_NAME_EVENT_LOGS + dateSuffix

    // b. 设置从哪张表读取数据
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    /**
      * TODO: 从HBase表中查询数据如何进行筛选呢？？？？？
      */
    // 创建Scan实例对象，扫描表中的数据
    val scan = new Scan()

    // i. 设置查询某一列簇
    val FAMILY_NAME = EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME
    scan.addFamily(FAMILY_NAME)

    // ii. 设置查询的列
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME))
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID))
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME))
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM))
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_VERSION))
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME))
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION))

    // iii. 设置过滤器，en事件类型的值必须为e_l，注意一点，先查询此列的值，再进行过滤
    scan.setFilter(
      new SingleColumnValueFilter(
        FAMILY_NAME, // cf
        Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), // column
        CompareFilter.CompareOp.EQUAL, // compare
        Bytes.toBytes(EventEnum.LAUNCH.alias)
      )
    )

    // 设置Scan扫描器，进行过滤操作
    conf.set(
      TableInputFormat.SCAN, //
      Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray) //
    )

    // c. 调用SparkContext类中newAPIHadoopRDD方法读取数据
    val eventLogsRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf, //
      classOf[TableInputFormat], //
      classOf[ImmutableBytesWritable], //
      classOf[Result]
    )

    logger.warn(s"============== Load Count = ${eventLogsRDD.count()} ==============")
/*
    eventLogsRDD.take(5).foreach{ case (key, result) =>
      println(s"RowKey = ${Bytes.toString(key.get())}")
      // 从Result中获取每条数据（列簇、列名和值）
      for(cell <- result.rawCells()){
        // 获取列簇
        val cf = Bytes.toString(CellUtil.cloneFamily(cell))
        // 获取列名
        val column = Bytes.toString(CellUtil.cloneQualifier(cell))
        // 获取值
        val value = Bytes.toString(CellUtil.cloneValue(cell))
        println(s"\t$cf:$column = $value -> ${cell.getTimestamp}")
      }
    }
*/

    // TODO: 将从HBase表中读取数据进行转换
    val newUserRDD: RDD[(String, String, String, String, String, String)] = eventLogsRDD
      // 解析获取每个列的值，由于不使用RowKey，所以不获取值
      .mapPartitions(_.map{ case (_, result) =>
        val uuid = Bytes.toString(result.getValue(FAMILY_NAME,
          Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID)))
        val serverTime = Bytes.toString(result.getValue(FAMILY_NAME,
          Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)))
        val platformName = Bytes.toString(result.getValue(FAMILY_NAME,
          Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)))
        val platformVersion = Bytes.toString(result.getValue(FAMILY_NAME,
          Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_VERSION)))
        val browserName = Bytes.toString(result.getValue(FAMILY_NAME,
          Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)))
        val browserVersion = Bytes.toString(result.getValue(FAMILY_NAME,
          Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)))

        // 以元组的形式返回
        (uuid, serverTime, platformName, platformVersion, browserName, browserVersion)
      })
      // 过滤数据 ，当uuid和serverTime为null的话，过滤掉，属于不合格的数据
      .filter(tuple => null != tuple._1 && null != tuple._2)

    // 对获取的 平台维度数据和浏览器维度数据进行处理组合，时间得到本月的第几天
    val dayPlatformBrowserNewUserRDD: RDD[DayPlatformBrowser] = newUserRDD.mapPartitions(_.map{
      case (uuid, serverTime, platformName, platformVersion, browserName, browserVersion) =>
        // 从访问服务器 时间戳，得到属于当月的第几天
        val calendar = Calendar.getInstance()
        calendar.setTimeInMillis(TimeUtil.parseNginxServerTime2Long(serverTime))
        val dayDimension = calendar.get(Calendar.DAY_OF_MONTH)

        // 平台维度信息
        var platformDimenson: String = ""
        if(StringUtils.isBlank(platformName)){
          platformDimenson = "unknown:unknown"
        }else if(StringUtils.isBlank(platformVersion)){
          platformDimenson = platformName + ":unknown"
        }else{
          platformDimenson =  platformName + ":" + platformVersion
        }

        // 浏览器维度信息
        var browserDimension: String = ""
        if(StringUtils.isBlank(browserName)){
          browserDimension = "unknown:unknown"
        }else if(StringUtils.isBlank(browserVersion)){
          browserDimension = browserName + ":unknown"
        }else{
          // 由于浏览器版本：info:browser_v = 46.0.2486.0，需要截取字符串，获取大版本号
          if(0 <= browserVersion.indexOf(".")){
            browserDimension =  browserName + ":" + browserVersion.substring(0, browserVersion.indexOf("."))
          }else {
            browserDimension = browserName + ":" + browserVersion
          }
        }
        // 最后同样以元组的形式返回
        DayPlatformBrowser(uuid, dayDimension, platformDimenson, browserDimension)
    })
    logger.warn(s"================= Dimension Count = ${dayPlatformBrowserNewUserRDD.count()} =================")
    // dayPlatformBrowserNewUserRDD.take(5).foreach(println)

    /**
      * 构建SparkSession实例对象
      */
    val spark: SparkSession = SparkSession.builder()
      .config(sc.getConf)
      .config("spark.sql.shuffle.partitions", "6")
      .getOrCreate()
    import spark.implicits._

    // 将RDD转换为DataFrame，通过RDD中类型为CaseClass，进行反射的方式自动推断
    val dayPlatformBrowserDF = dayPlatformBrowserNewUserRDD.toDF()
    dayPlatformBrowserDF.printSchema()
    dayPlatformBrowserDF.show(5, truncate = false)

    // 缓存数据
    dayPlatformBrowserDF.persist(StorageLevel.MEMORY_AND_DISK_2)

    /**
      * TODO：基本维度分析：时间维度 + 平台维度
      */
    logger.warn("============== 新增用户统计（基本维度）==============")
    // 使用DSL分析：调用DataFrame中API
    dayPlatformBrowserDF
      .select($"day", $"platformDimension")
      .groupBy($"day", $"platformDimension").count()
      .show(30, truncate = false)

    // 注册为临时视图：此时图仅仅在当前SparkSession中可用，Session终止及不可用和其他Session不共享
    // dayPlatformBrowserDF.createOrReplaceTempView("view_tmp_dpb")

    // 注册为全局视图，不同的SparkSession实例对象可以共用，视图保存在global_temp数据库中，使用的时指定数据库
    dayPlatformBrowserDF.createGlobalTempView("view_tmp_dpb")
    /**
      * TODO：基本维度分析 + 浏览器维度分析
      */
    logger.warn("============== 新增用户统计（基本维度和浏览器维度）==============")
    spark.sql(
      """
        |SELECT
        |  day, platformDimension, browserDimension, COUNT(1) AS total
        |FROM
        |  global_temp.view_tmp_dpb
        |GROUP BY
        |  day, platformDimension, browserDimension
      """.stripMargin)
      .show(30, truncate = false)


    // 直接调用DataFrame.write.jdbc()保存数据到MySQL数据库中
    /**
      * TODO: 针对此应用来说，比如开始运行失败，其中已经向数据库插入分析结果，再次运行的时候，结果如何保存呢？？
      *   经分析可发现：SaveMode中各个选项的值，并不合适，要实现的是：
      *   -a. 当主键存在，更新数据
      *   -b. 当主键不存在，插入数据
      * 在MySQL数据库中SQL语法为：
      *   INSERT INTO tableName(field1, field2, ...) VALUES(?, ?, ...) ON DUPLICATE KEY UPDATE field1=value1, field2=value2, ...
      */
    spark.sql(
      """
        |SELECT
        |  day, platformDimension, browserDimension, COUNT(1) AS total
        |FROM
        |  global_temp.view_tmp_dpb
        |GROUP BY
        |  day, platformDimension, browserDimension
      """.stripMargin)
      // 对结果数据进行降低分区数
      .coalesce(1).foreachPartition(iter => {
        // ..................
      })



    // 释放缓存的数据
    dayPlatformBrowserDF.unpersist()

    // 为了开发测试，线程休眠，WEB UI监控查看
    Thread.sleep(10000000)

    // 关闭资源
    sc.stop()
  }

}
