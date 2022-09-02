package com.mm.etl

import com.mm.utils.EtlUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/*
  打包前：
  1. 注册掉 master
  2. 修改抽取的文件和要写入的 ODS 表名为从命令行动态传入
  3. 修改 pom.xml 文件，指定 <finalName>jt</finalName>，表示要打包的包名
  4. 打包：mvn clean package
  5. 准备好数据文件：将数据文件上传到 HDFS 中
  6. 提交作业：spark-submit --master spark://xueai8:7077 --class com.mm.etl.MetroEtlJob jt.jar \
             /data/spark/jt/T08_TRADE_IC_DETAIL_gdjt20000.dat \
             s_ods_metro
*
* */


/*
 * 从 hdfs 文件系统中加载数据到 hive 表中
 */

// 对地铁刷卡数据执行ETL操作
object MetroEtlJob {

  // 创建日志记录器
  val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    // 先判断，是否正确地指定了输入文件路径和hive ods表名
    if(args.length != 2){
      logger.warn("请指定要抽取的文件和要写入的ODS表名。")
      System.exit(-1)
    }

    // 1 创建 SparkSession 的实例
    val spark = SparkSession.builder()
    // .master("local[*]")  // 部署提交时动态指定
      .appName("metro etl job")
      .enableHiveSupport()  // 需要启用对 Hive 的支持
      .getOrCreate()

    // 打开 Hive 动态分区的标志
    spark.sqlContext.setConf("hive.exec.dynamic.partition","true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")

    // 2 执行 ETL
    metroEtlTask(spark,args)

    // 3 关闭 SparkSession
    spark.close()
  }

  // 对地铁数据执行 ETL 的方法
  def metroEtlTask(spark:SparkSession, args: Array[String]): Unit = {
    // 定义数据文件路径(本地)
    // val metroFile = "data/T08_TRADE_IC_DETAIL_gdjt20000.dat" // 地铁刷卡数据集路径
    // 定义数据文件路径(部署时需要动态指定)
    val metroFile = args(0) // 地铁刷卡数据集路径

    // 定义文件选项配置
    val metroFileOptions = Map("filePath" -> metroFile)

    // 定义字段
    // 定义 Schema
    val metroFields = Seq(
      StructField("card_id", StringType, true),
      StructField("card_type", StringType, true),
      StructField("line_id", StringType, true),
      StructField("station_id", StringType, true),
      StructField("device_id", StringType, true),
      StructField("trade_time", TimestampType, true),
      StructField("io_type", StringType, true),
      StructField("trade_date", StringType, true)
    )
    val metroSchema = StructType(metroFields)

    // 定义 Hive 选项配置
    // val hiveOptions = Map("db" -> "jt", "tb" -> "s_ods_metro", "partitionColumn" -> "trade_date")
    val hiveOptions = Map("db" -> "jt2", "tb" -> args(1), "partitionColumn" -> "trade_date")

    // ELT
    // EtlUtil.extractFromFile(spark,metroFile,metroSchema).show()
    EtlUtil.eltFromFileToHive(spark, metroFileOptions, metroSchema, hiveOptions)

    // 测试
    spark.table("jt2.s_ods_metro").show(5)
  }
}
