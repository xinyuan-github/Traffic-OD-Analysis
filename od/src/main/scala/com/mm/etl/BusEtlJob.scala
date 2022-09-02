package com.mm.etl

import com.mm.utils.EtlUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

/*
 * 从 HDFS 文件系统中加载数据到 hive表中
 */

// 对公交刷卡数据执行 ETL 操作
object BusEtlJob {
  // 创建日志记录器
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    // 先判断，是否正确地指定了输入文件路径和 hive 表名
    if (args.length != 2) {
      logger.warn("请指定输入文件路径和要写入的Hive表名。")
      System.exit(-1)
    }

    // 1 创建SparkSession的实例
    val spark = SparkSession.builder()
      //      .master("local[*]")
      .appName("bus etl job")
      .enableHiveSupport() // 需要启用对 Hive 的支持
      .getOrCreate()

    // 打开 Hive 动态分区的标志
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    // 2 执行 ETL
    busEtlTask(spark, args)

    // 3 关闭 SparkSession
    spark.close()
  }

  // 对公交数据执行 ETL 的方法
  def busEtlTask(spark: SparkSession, args: Array[String]): Unit = {
    // 定义数据文件路径
    // val busFile = "data/T08_TRADE_IC_DETAIL20000.dat" // 公交刷卡数据集路径
    val busFile = args(0) // 公交刷卡数据集路径

    // 定义文件选项配置
    val busFileOptions = Map("filePath" -> busFile)

    // 定义字段
    val busFields = Seq(
      StructField("card_id", StringType, true),
      StructField("card_type", StringType, true),
      StructField("trade_type", StringType, true),

      StructField("mark_time", TimestampType, true),
      StructField("mark_line_id", StringType, true),
      StructField("mark_station", StringType, true),
      StructField("mark_bus_id", StringType, true),
      StructField("mark_tms_id", StringType, true),

      StructField("trade_time", TimestampType, true),
      StructField("trade_line_id", StringType, true),
      StructField("trade_station", StringType, true),
      StructField("trade_bus_id", StringType, true),
      StructField("trade_tms_id", StringType, true),

      StructField("trade_date", StringType, true)
    )

    // 定义schema
    val busSchema = StructType(busFields)

    // 定义Hive选项配置
    // val hiveOptions = Map("db" -> "jt", "tb" -> "s_ods_bus", "partitionColumn" -> "trade_date")
    val hiveOptions = Map("db" -> "jt2", "tb" -> args(1), "partitionColumn" -> "trade_date")

    // ELT
    // EtlUtil.extractFromFile(spark,busFile,busSchema).show()
    EtlUtil.eltFromFileToHive(spark, busFileOptions, busSchema, hiveOptions)

    // 测试
    spark.table("jt2.s_ods_bus").show(5)
  }
}
