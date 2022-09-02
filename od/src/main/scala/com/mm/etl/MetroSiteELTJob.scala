package com.mm.etl

import com.mm.utils.EtlUtil
import org.apache.spark.sql.SparkSession

object MetroSiteELTJob {
  def main(args: Array[String]): Unit = {
    // 在 windows 下开发时设置
    // System.setProperty("HADOOP_USER_NAME", "cxy")

    // 创建 SparkSession
    val spark = SparkSession.builder()
    // .master("local[*]")
      .appName("jt OD")
      .enableHiveSupport() // 启动 hive 支持
      .getOrCreate()

    // 对地铁刷卡数据执行 ELT
    metroSiteELTask(spark)

    // 关闭 SparkSession
    spark.close()
  }

  // 地铁站点数据 ELT 方法
  def metroSiteELTask(spark: SparkSession): Unit = {
    // 1 定义文件数据源配置
    val DB_URL= "jdbc:mysql://node01:3306/jt?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai"       // 数据库连接url
    val metroJdbcMap = Map(
      "url" -> DB_URL,                	// jdbc url
      "dbtable" -> "metro_station_tb",  // 要读取的数据表
      "user" -> "root",               	// mysql 账号
      "password" -> "1111"           	  // mysql 密码
    )

    // 2 定义 Hive 配置
    val hiveOptions = Map("db" -> "jt2", "tb" -> "metro_station_tb")

    // 3 执行 ELT
    // EtlUtil.extractFromJDBC(spark, metroJdbcMap).show()    // 测试
    EtlUtil.eltFromJDBCToHive(spark, metroJdbcMap, hiveOptions)

    spark.table("jt2.metro_station_tb").show(5)
  }
}
