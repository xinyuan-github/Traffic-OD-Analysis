package com.mm.combiner

import com.mm.utils.EtlUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

/*
  公交刷卡数据和公交站点数据整合：join 连接
 */
object BusJoinSiteJob {
  // 创建日志记录器
  val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    // 在 windows 下开发时设置
    // System.setProperty("HADOOP_USER_NAME", "hduser")

    // 先判断，是否正确地指定了输入文件路径和hive ods表名
    if(args.length != 2){
      logger.warn("请指定要读取的清洗后公交刷卡数据和要写入的 join site 后的表名。")
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
    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "5")   // 默认200(当join或聚合时)

    // 对公交刷卡数据和公交站点数据执行 join 连接
    busSiteJoinTask(spark, args)

    // 关闭 SparkSession
    spark.close()
  }

  // 公交刷卡数据和公交站点数据执行 join 连接的方法
  def busSiteJoinTask(spark: SparkSession, args:Array[String]): Unit = {
    import spark.implicits._

    // 1 从 Hive 数仓中加载公交站点数据到 DataFrame 中并注册临时视图
    spark
      .table("jt.bus_station_tb")
      // 注册为临时视图
      .createOrReplaceTempView("bus_site_tb")

    // 2 从 Hive 数仓中加载公交预处理后的数据并注册临时视图
    spark
    // .table("jt2.f_bus_cleaned")
      .table("jt2."+args(0))
      // 对齐字段，站点属性选择
      .withColumn("line_no", coalesce($"mark_line_id", $"trade_line_id"))
      .withColumn("station_name", coalesce($"mark_station", $"trade_station"))
      .createOrReplaceTempView("bus_jt_tb")

    // 3 执行 join 连接
    val sqlStr1 =
      """
        |select /*+ MAPJOIN(s) */ b.*, s.station_id,s.station_longitude,s.station_latitude
        |from bus_jt_tb b join bus_site_tb s
        |on trim(b.line_no) = trim(s.line_no) and trim(b.station_name) = trim(s.station_name)
        |""".stripMargin

    val busWithSiteDF = spark.sql(sqlStr1)

    // 4 将连接后的数据集写回 Hive DW 层
    // 调用定义好的 load 方法
    // EtlUtil.loadToHive(spark, busWithSiteDF, "jt", "a_bus_withsite", Some("trade_date"))
    EtlUtil.loadToHive(spark, busWithSiteDF, "jt2", args(1), Some("trade_date"))

    spark.table("jt2.a_bus_withsite").show(5)
  }
}
