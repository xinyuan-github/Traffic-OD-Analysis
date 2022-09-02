package com.mm.clean

import com.mm.utils.EtlUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

/*
  公交刷卡数据预处理
 */
object BusCleanJob {

  // 创建日志记录器
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    // 在 windows 下开发时设置
    // System.setProperty("HADOOP_USER_NAME", "cxy")

    // 先判断，是否正确地指定了输入 hive 表名和要写入的 hive 表名
    if (args.length != 2) {
      logger.warn("请指定要读取的hive表名和要写入的hive表名。")
      System.exit(-1)
    }

    // 创建 SparkSession
    val spark = SparkSession.builder()
      // .master("local[*]")
      .appName("jt OD")
      .enableHiveSupport()
      .getOrCreate()

    // 打开 Hive 动态分区的标志
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    // 对公交刷卡数据执行预处理
    busCleanTask(spark, args)

    // 关闭 SparkSession
    spark.close()
  }

  // 公交刷卡数据预处理方法
  def busCleanTask(spark: SparkSession, args: Array[String]): Unit = {
    // 1. 从 Hive 数仓中加载 ODS 源数据到 DataFrame 中

    // val busjtDF = spark.table("jt.s_ods_bus")
    val busjtDF = spark.table("jt2." + args(0))

    /*属性转换
      首先增加列 "mark_trade_time"，代表本次刷卡(交易)的时间
      - 如果刷卡记录的 "mark_time" 不是 null，则 mark_trade_time 取 mark_time 时间
      - 如果刷卡记录的 "trade_time" 不是 null，则 mark_trade_time 取 trade_time 时间
      再增加列 "tms_id"，代表趟次
    */
    val busjtDF2 = busjtDF
      // 交易时间
      .withColumn("mark_trade_time", coalesce(col("mark_time"), col("trade_time")))
      // 趟次
      .withColumn("tms_id", coalesce(col("mark_tms_id"), col("trade_tms_id")))
      // 属性选择
      .withColumn("IO_TYPE", when(col("mark_time").isNotNull, "I").otherwise("O"))
      .select("card_id", "trade_type",
        "mark_time", "mark_line_id", "mark_station", "mark_bus_id", "mark_tms_id",
        "trade_time", "trade_line_id", "trade_station", "trade_bus_id", "trade_tms_id",
        "trade_date", "tms_id", "mark_trade_time", "IO_TYPE")

    // 2. 将清洗过的数据集写回 Hive DW 层

    // 调用定义好的 load 方法
    // EtlUtil.loadToHive(spark, busjtDF2, "jt", "f_bus_cleaned", Some("trade_date"))
    EtlUtil.loadToHive(spark, busjtDF2, "jt2", args(1), Some("trade_date"))

    spark.table("jt2.f_bus_cleaned").show(5)
  }
}
