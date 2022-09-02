package com.mm.combiner

import com.mm.utils.EtlUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

/*
  地铁刷卡数据和地铁站点数据整合：join 连接
 */
object MetroJoinSiteJob {

  // 创建日志记录器
  val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    // 在 windows 下开发时设置
    // System.setProperty("HADOOP_USER_NAME", "hduser")

    // 先判断，是否正确地指定了输入文件路径和 hive ods 表名
    if(args.length != 2){
      logger.warn("请指定要读取的清洗后地铁刷卡数据和要写入的join site后的表名。")
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
    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "5")   // 默认 200(当 join 或聚合时)

    // 对地铁刷卡数据和地铁站点数据执行 join 连接
    metroSiteJoinTask(spark, args)

    // 关闭 SparkSession
    spark.close()
  }

  // 地铁刷卡数据预处理方法
  def metroSiteJoinTask(spark: SparkSession, args: Array[String]): Unit = {
    import spark.implicits._

    // 1 从 Hive 数仓中加载地铁站点数据到 DataFrame 中并注册临时视图
    spark
      .table("jt2.metro_station_tb")
      // 线路属性抽取
      .withColumn("line_no", regexp_extract($"line_name", "(\\d+).*", 1))
      // 注册为临时视图
      .createOrReplaceTempView("metro_site_tb")

    // 2 从 Hive 数仓中加载地铁预处理后的数据并注册临时视图
    spark
      // .table("jt.f_metro_cleaned")
      .table("jt2."+args(0))
      // 对齐线路和站点名称
      .withColumnRenamed("line_id","line_no")
      .withColumnRenamed("station_id","station_name")
      .createOrReplaceTempView("metro_jt_tb")

    // 3 执行 join 连接
    val sqlStr1 =
      """
        |select /*+ MAPJOIN(s) */ m.*, s.station_id,s.station_longitude,s.station_latitude
        |from metro_jt_tb m join metro_site_tb s
        |on trim(m.line_no) = trim(s.line_no) and trim(m.station_name)=trim(s.station_name)
        |""".stripMargin

    val metroWithSiteDF = spark.sql(sqlStr1)

    // 4 将连接后的数据集写回 Hive DW 层
    // 调用定义好的 load 方法
    // EtlUtil.loadToHive(spark, metroWithSiteDF, "jt", "a_metro_withsite", Some("trade_date"))
    EtlUtil.loadToHive(spark, metroWithSiteDF, "jt2", args(1), Some("trade_date"))

    spark.table("jt2.a_metro_withsite").show(5)
  }
}
