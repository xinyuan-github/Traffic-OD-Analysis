package com.mm.clean

import com.mm.utils.EtlUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

/*
  地铁刷卡数据预处理
 */
object MetroCleanJob {

  // 创建日志记录器
  val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    // 在windows下开发时设置
//    System.setProperty("HADOOP_USER_NAME", "hduser")

    // 先判断，是否正确地指定了输入hive表名和要写入的hive表名
    if(args.length != 2){
      logger.warn("请指定要读取的hive表名和要写入的hive表名。")
      System.exit(-1)
    }

    // 创建SparkSession
    val spark = SparkSession.builder()
//      .master("local[*]")
      .appName("jt OD")
      .enableHiveSupport()
      .getOrCreate()

    // 打开Hive动态分区的标志
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    // 对地铁刷卡数据执行预处理
    metroCleanTask(spark, args)

    // 关闭SparkSession
    spark.close()
  }

  // 地铁刷卡数据预处理方法
  def metroCleanTask(spark: SparkSession, args: Array[String]): Unit = {
    // 1. 从Hive数仓中加载ODS源数据到DataFrame中
//    val metroDF = spark.table("jt.s_ods_metro")
    val metroDF = spark.table("jt2."+args(0))

    // 窗口三要素：分区，排序，范围
    val ww = Window.partitionBy("trade_date","card_id").orderBy("trade_time")

    val metroDF2 = metroDF
      .withColumn("once_od", when(col("io_type")==="I",1).otherwise(0))
      .withColumn("tms_id", sum("once_od").over(ww))     // 趟次
      .withColumn("tct", concat_ws("_",col("trade_date"),col("card_id"),col("tms_id")))   // 唯一列
      .orderBy("card_id")

    // 找出出站漏刷卡记录子集
    val singleIn = metroDF2.groupBy("tct").count().where(col("count")===1)

    // 从地铁刷卡数据集中，把出站漏刷卡记录给排除掉
    // 使用：左反连接
    //      这种join类型能够发现来自左边数据集的哪些行在右边的数据集上没有任何匹配的行，而连接后的数据集只包含来自左边数据集的列。
    val metroDF3 = metroDF2.join(singleIn, metroDF2.col("tct")===singleIn.col("tct"), "left_anti")

    // 排除进站漏刷卡记录
    // -- 增加一个新列last_io_type，代表上次的IO_TYPE类型
    // -- 如果是进站漏刷卡，那么 io_type==last_io_type

    import spark.implicits._

    // lage窗口函数：取当前的前面第几行的哪个字段值  lag(列名,1)
    val metroDF4 = metroDF3
      .withColumn("last_io_type", lag("io_type",1,"O").over(ww))
      .where($"io_type" =!= $"last_io_type")
      .drop("device_id")    // device_id 列无意义，去掉

    // 2. 将清洗过的数据集写回Hive DW层
    // 调用定义好的load方法
//    EtlUtil.loadToHive(spark, metroDF4, "jt", "f_metro_cleaned", Some("trade_date"))
    EtlUtil.loadToHive(spark, metroDF4, "jt2", args(1), Some("trade_date"))

    spark.table("jt2.f_metro_cleaned").show(5)
  }
}
