package com.he.offlineAnalyse.analystics

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Calendar

import com.he.common.EventLogConstants
import com.he.common.EventLogConstants.EventEnum
import com.he.util.TimeUtil
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{CompareFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableSnapshotInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

/**
  * 基于SparkCore实现，从HBase表中读取数据，统计新增用户个数，按照不同维度进行分析
  */
object NewInstallUserCountHFileSpark {

  // 记录开发程序日志
  val logger: Logger = Logger.getLogger(NewInstallUserCountSpark.getClass)

  def main(args: Array[String]): Unit = {

    // TODO: 需要传递一个参数，表明处理的数据是哪一天的
    if (args.length < 1) {
      println("Usage: NewInstallUserCountSpark process_date")
      System.exit(1)
    }

    // TODO: 设置日志级别
    Logger.getRootLogger.setLevel(Level.WARN)

    /**
      * TODO: 创建SparkContext实例对象，读取数据，调度Job执行
      */
    val sparkConf = new SparkConf()
      .setMaster("local[3]").setAppName("NewInstallUserCountSpark Application")
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
    // b. 设置从哪张表读取数据 -> TODO: 实际对应的是表的快照信息
    val snapshotName = "snapshot_" + tableName

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

    // TODO: b. 设置TableSnapshotInputFormat从哪个快照中读取数据
    val job = Job.getInstance(conf)

    // 设置输入路径 public static Unit setInput(job: Job, snapshotName: String, restoreDir: Path)
    val restoreDir: Path = new Path("/datas/hbase/snapshot/")
    TableSnapshotInputFormat.setInput(job, snapshotName, restoreDir)


    // c. 调用SparkContext类中newAPIHadoopRDD方法读取数据
    val eventLogsRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      job.getConfiguration, //
      classOf[TableSnapshotInputFormat], //
      classOf[ImmutableBytesWritable], //
      classOf[Result] //
    )

    logger.warn(s"============== Load Count = ${eventLogsRDD.count()} ==============")

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
    val dayPlatformBrowserNewUserRDD: RDD[(String, Int, String, String)] = newUserRDD.mapPartitions(_.map{
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
        (uuid, dayDimension, platformDimenson, browserDimension)
    })
    logger.warn(s"================= Dimension Count = ${dayPlatformBrowserNewUserRDD.count()} =================")
    // dayPlatformBrowserNewUserRDD.take(5).foreach(println)

    // 由于后续针对RDD进行多次分析，进行缓存
    dayPlatformBrowserNewUserRDD.persist(StorageLevel.MEMORY_AND_DISK)

    /**
      * TODO：基本维度分析（时间维度 + 平台维度） + 浏览器维度分析
      * 将数据保存到MySQL数据库中
          CREATE TABLE `result_installer_user_cnt` (
            `day_dim` int(255) NOT NULL,
            `platform_dim` varchar(255) NOT NULL,
            `browser_dim` varchar(255) NOT NULL,
            `cnt` int(255) NOT NULL,
            PRIMARY KEY (`day_dim`,`platform_dim`,`browser_dim`)
          ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
      */
    logger.warn("============== 新增用户统计（基本维度和浏览器维度）==============")
    val dayPlatformBrowserNewUserCountRDD = dayPlatformBrowserNewUserRDD
      // 提取字段， 组合维度，标记一次
      .mapPartitions(_.map{
        case (_, dayDim, platformDim, browserDim) => ((dayDim, platformDim, browserDim), 1)
      })
      // 聚合统计
      .reduceByKey(_ + _)
    dayPlatformBrowserNewUserCountRDD.coalesce(1).foreachPartition(iter => {
      Try{
        // i. 获取connection
        val conn: Connection = {
          // 加载驱动类
          Class.forName("com.mysql.jdbc.Driver")
          // 链接数据三要素
          val url: String = "jdbc:mysql://bigdata-training01.erongda.com:3306/test?characterEncoding=UTF-8"
          val userName: String = "root"
          val password: String = "123456"
          // 获取链接并返回
          DriverManager.getConnection(url, userName, password)
        }

        // 获取数据库原始事务设置
        val oldAutoCommit = conn.getAutoCommit
        // TODO: 设置事务，针对当前分区数据要么都插入数据库，要么都不插入
        conn.setAutoCommit(false)

        // ii. 创建PreparedStatement 实例对象
        // 插入数据的SQL语句，实现Insert Or Update
        val sqlStr = "INSERT INTO result_installer_user_cnt (day_dim, platform_dim, browser_dim, cnt) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE cnt = VALUES(cnt)"
        val pstmt: PreparedStatement = conn.prepareStatement(sqlStr)

        // iii. 对数据进行迭代输出操作
        iter.foreach{ case((dayDim, platformDim, browserDim), cnt) =>
          println(s"------ $dayDim - $platformDim - $browserDim:  $cnt ------")
          // 设置列的值
          pstmt.setInt(1, dayDim)
          pstmt.setString(2, platformDim)
          pstmt.setString(3, browserDim)
          pstmt.setInt(4, cnt)

          // 添加批次
          pstmt.addBatch()
        }

        // iv. 进行提交批次，插入数据
        pstmt.executeBatch()
        // 手动提交事务，将批量数据要么全部插入，要么全不插入（失败)
        conn.commit()

        // 如果操作成功返回Connection
        (oldAutoCommit, conn)
      }match {
        // 执行成功
        case Success((oldCommit, connection)) =>
          // 当数据插入成功插入到数据库以后，回复原先数据库事务相关设置
          Try(connection.setAutoCommit(oldCommit))
          if(null != connection) connection.close()
        case Failure(exception) => throw exception
      }
    })

    // 释放缓存的数据
    dayPlatformBrowserNewUserRDD.unpersist()

    // 为了开发测试，线程休眠，WEB UI监控查看
    Thread.sleep(10000000)

    // 关闭资源
    sc.stop()
  }

}
