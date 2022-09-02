package com.mm.od

import com.mm.utils.EtlUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

/*
  用户OD出行分析
 */
object JtODJob {
  // 创建日志记录器
  val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    // 在 windows 下开发时设置
    // System.setProperty("HADOOP_USER_NAME", "hduser")

    // 先判断，是否正确地指定了输入文件路径和 hive ods 表名
    if (args.length != 2) {
      logger.warn("请指定要读取的union表名和要写入数据集市的客流统计表名。")
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
    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "5") // 默认 200(当 join 或聚合时)

    // 用户出行 OD 分析
    jtodTask(spark, args)

    // 关闭 SparkSession
    spark.close()
  }

  // 用户出行 OD 分析
  // 怎样确定一次出行的 OD?
  // 当下一次 O 的时间和上一次 D 的时间之差超过一个小时（1*60*60），则认为是一次新的 OD；
  // 否则，则认为是同一次 OD。
  def jtodTask(spark: SparkSession, args: Array[String]): Unit = {
    import spark.implicits._

    // 1 加载数据：从 hive 加载合并后的数据集
    // val ggjtData = spark.table("jt.a_ggjt")
    val ggjtData = spark.table("jt2." + args(0))

    // 定义窗口
    val w4 = Window.partitionBy("trade_date", "card_id").orderBy("tms_id", "mark_trade_timestamp")
    val ascWindow = Window.partitionBy("trade_date", "card_id", "new_tms_id").orderBy("tms_id", "mark_trade_timestamp") // 升序窗口，用于取第一条(I) -O
    val descWindow = Window.partitionBy("trade_date", "card_id", "new_tms_id").orderBy(col("tms_id"), col("mark_trade_timestamp").desc) // 降序窗口，用于取倒数一条(O) -D

    // 定义两个窗口，一个按交易时间正序，一个按交易时间倒序。
    val w43 = Window.partitionBy("trade_date", "card_id").orderBy($"mark_trade_time")
    val w44 = Window.partitionBy("trade_date", "card_id").orderBy($"mark_trade_time".desc)

    val odDF = ggjtData
      // 为了计算一次新的 OD，需要将标注时间或交易时间转换为长整数
      .selectExpr("*", "bigin t(mark_trade_time) as mark_trade_timestamp")
      .withColumn("mask", when(col("io_type") === "I", 1).otherwise(lit(0)))
      .withColumn("duration", col("mark_trade_timestamp") - lag("mark_trade_timestamp", 1, 0).over(w4))
      .withColumn("new_mask", when(col("duration") * col("mask") > 3600, 1).otherwise(0))
      .withColumn("new_tms_id", sum("new_mask").over(w4))
      .withColumn("new_io_type", when(col("new_mask") === 1, "I").otherwise("O"))
      // 在一次出行中，只取第一条 O 和最后一条 D，构成一组 O-D
      .withColumn("marker", when(rank.over(ascWindow) === 1, 0).otherwise(9)) // 识别出一次 OD 中的 O
      .withColumn("marker", when(rank.over(descWindow) === 1, 999).otherwise(col("marker"))) // 识别出一次 OD 中的 D
      .filter(col("marker") === 0 || col("marker") === 999) // 过滤出 O 和 D
      .drop("marker")
      // 构造 OD 字段
      .withColumn("od_desc", concat(first("station_name").over(w43), lit("-"), first("station_name").over(w44)))
      .withColumn("od_type", when($"IO_TYPE" === "I", 0).otherwise(1))
      // 属性选择
      .select(col("card_id"),
        col("trade_date"),
        col("mark_time").as("o_time"),
        col("mark_line_id").as("o_line"),
        col("mark_station").as("o_station"),
        col("trade_time").as("d_time"),
        col("trade_line_id").as("d_line"),
        col("trade_station").as("d_station"),
        col("od_desc"),
        col("od_type"),
        col("station_name"),
        col("station_longitude"),
        col("station_latitude")
      )

    // 客流统计
    val passenger_flow = odDF
      .select("trade_date", "card_id", "od_desc", "od_type", "station_name", "station_longitude", "station_latitude")
      .groupBy("trade_date", "od_desc", "od_type", "station_name", "station_longitude", "station_latitude")
      .count()
      .withColumnRenamed("count", "passenger_flow")
      .orderBy("trade_date", "od_desc", "od_type")

    // 4 将客流统计结果集写回 Hive 数据集市层
    // 调用定义好的 load 方法
    // EtlUtil.loadToHive(spark, passenger_flow, "jt", "m_od_flow", Some("trade_date"))
    EtlUtil.loadToHive(spark, passenger_flow, "jt2", args(1), Some("trade_date"))

    // 测试写入是否成功
    spark.table("jt2.m_od_flow").show()
  }
}
