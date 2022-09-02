package com.mm.etl

import com.mm.utils.EtlUtil
import org.apache.spark.sql.SparkSession

/*
 * 从 mysql 中加载数据到 hive 表中
 */

object BusSiteELTJob {
  def main(args: Array[String]): Unit = {
    // 在windows下开发时设置
    // System.setProperty("HADOOP_USER_NAME", "cxy")

    // 创建SparkSession
    val spark = SparkSession.builder()
    // .master("local[*]")
      .appName("jt OD")
      .enableHiveSupport()
      .getOrCreate()

    // 对地铁站点数据执行ELT
    busSiteELTask(spark)

    // 关闭SparkSession
    spark.close()
  }

  // 地铁站点数据 ELT 方法
  def busSiteELTask(spark: SparkSession): Unit = {
    // 1 定义文件数据源配置
    val DB_URL= "jdbc:mysql://node01:3306/jt?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai"       // 数据库连接url
    val busJdbcMap = Map(
      "url" -> DB_URL,                	// jdbc url
      "dbtable" -> "bus_station_tb",    // 要读取的数据表
      "user" -> "root",               	// mysql 账号
      "password" -> "1111"           	  // mysql 密码
    )

    // 2 定义 Hive 配置
    val hiveOptions = Map("db" -> "jt2", "tb" -> "bus_station_tb")

    // 3 执行 ELT
    // ELT.extractFromJDBC(spark, busJdbcMap).show() // 测试
    EtlUtil.eltFromJDBCToHive(spark, busJdbcMap, hiveOptions)

    spark.table("jt2.bus_station_tb").show(5)
  }
}
