package com.mm.combiner

import com.mm.utils.EtlUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

/*
  公交数据和地铁数据整合：union
 */
object BusUnionMetroJob {

  // 创建日志记录器
  val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    // 在windows下开发时设置
    // System.setProperty("HADOOP_USER_NAME", "hduser")

    // 先判断，是否正确地指定了输入文件路径和 hive ods 表名
    if (args.length != 3) {
      logger.warn("请指定要要对齐的公交数据集和地铁数据集，以及合并后的union表名。")
      System.exit(-1)
    }

    // 创建 SparkSession
    val spark = SparkSession.builder()
      // .master("local[*]")
      .appName("jt OD union")
      .enableHiveSupport()
      .getOrCreate()

    // 打开 Hive 动态分区的标志
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    // spark.sqlContext.setConf("spark.sql.shuffle.partitions", "5")   // 默认 200(当 join 或聚合时)

    // 对公交数据和地铁数据执行 union 连接
    busUnionMetroTask(spark, args)

    // 关闭 SparkSession
    spark.close()
  }

  // 对公交数据和地铁数据执行 union 连接的方法
  def busUnionMetroTask(spark: SparkSession, args: Array[String]): Unit = {
    import spark.implicits._

    // 1 选择公交属性保留字段；
    val fields = List("card_id",
      "mark_time", "mark_line_id", "mark_station", "mark_tms_id",
      "trade_time", "trade_line_id", "trade_station", "trade_tms_id",
      "mark_trade_time",
      "tms_id", "IO_TYPE", "line_no", "station_name", "station_id", "station_longitude", "station_latitude", "trade_date",
    )
    val alignedBusData = spark
      // .table("jt.a_bus_withsite")
      .table("jt2." + args(0))
      .select(fields.head, fields.tail: _*)

    // 2 将地铁属性字段与公交属性字段对齐
    val algnedMetroData = spark
      // .table("jt.a_metro_withsite")
      .table("jt2." + args(1))
      // 刷卡时间
      .withColumnRenamed("trade_time", "mark_trade_time")
      // 对齐字段
      .withColumn("mark_time", when($"io_type" === "I", $"mark_trade_time").otherwise(null))
      .withColumn("mark_line_id", when($"io_type" === "I", $"line_no").otherwise(null))
      .withColumn("mark_station", when($"io_type" === "I", $"station_name").otherwise(null))
      .withColumn("mark_tms_id", when($"io_type" === "I", $"tms_id").otherwise(null))
      .withColumn("trade_time", when($"io_type" === "O", $"mark_trade_time").otherwise(null))
      .withColumn("trade_line_id", when($"io_type" === "O", $"line_no").otherwise(null))
      .withColumn("trade_station", when($"io_type" === "O", $"station_name").otherwise(null))
      .withColumn("trade_tms_id", when($"io_type" === "O", $"tms_id").otherwise(null))
      // 选择属性
      .select(fields.head, fields.tail: _*)

    // 3 执行 union 合并
    val ggjtDF = alignedBusData.union(algnedMetroData).distinct

    // 4 将连接后的数据集写回 Hive DW 层
    // 调用定义好的 load 方法
    // EtlUtil.loadToHive(spark, ggjtDF, "jt", "a_ggjt", Some("trade_date"))
    EtlUtil.loadToHive(spark, ggjtDF, "jt2", args(2), Some("trade_date"))

    spark.table("jt2.a_ggjt").show(5)
  }
}
