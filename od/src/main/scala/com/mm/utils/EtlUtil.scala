package com.mm.utils

import org.apache.spark.sql._
import org.apache.spark.sql.types._

// ETL 工具类
object EtlUtil {
  // extract from file
  /**
   * @param spark    SparkSession 实例
   * @param filePath 要加载的文件路径
   * @param schema   指定的模式
   * @return 返回一个 DataFrame（Dataset[Row]）
   *
   */
  def extractFromFile(spark: SparkSession,
                      filePath: String,
                      schema: StructType): Dataset[Row] = {
    // 读取文件数据源，创建 DataFrame
    val df = spark.read
      .option("header", value = false)
      .option("inferSchema", value = false)
      .option("sep", "\t")
      .schema(schema)
      .csv(filePath)

    // 返回
    df
  }

  // extract from mysql
  /**
   * @param spark   SparkSession 实例
   * @param jdbcMap 要加载的 JDBC 配置项
   * @return 返回一个 DataFrame（Dataset[Row]）
   *
   */
  def extractFromJDBC(spark: SparkSession, jdbcMap: Map[String, String]): Dataset[Row] = {
    // 读取 JDBC 数据源，创建 DataFrame
    val df = spark
      .read
      .format("jdbc")
      .options(jdbcMap)
      .load()

    // 返回
    df
  }

  // load to hive
  /**
   * @param spark
   * @param df              要装载到 Hive 中的 DataFrame
   * @param db              要装载到的 Hive 数据库
   * @param tb              要装载到的 Hive ODS 表
   * @param partitionColumn 指定分区列
   * @return unit
   */
  def loadToHive(spark: SparkSession,
                 df: Dataset[Row],
                 db: String,
                 tb: String,
                 partitionColumn: Option[String] = None): Unit = {

    // 打开指定数据库，这里使用了字符串插值
    spark.sql(s"use $db")

    // 有的表需要分区，有的不需要。这里使用模式匹配来分别处理
    partitionColumn match {
      case Some(column) =>
        df.write
          .format("parquet")
          .mode(SaveMode.Overwrite) // 覆盖
          .partitionBy(column) // 指定分区
          .saveAsTable(tb) // saveAsTable()方法：会将 DataFrame 数据保存到 Hive 表中
      case None =>
        df.write
          .format("parquet")
          .mode(SaveMode.Overwrite) // 覆盖
          .saveAsTable(tb)
    }
  }

  // 定义一个ELT方法，包含 extract + load
  def eltFromFileToHive(spark: SparkSession,
                        fileMap: Map[String, String],
                        schema: StructType, hiveMap: Map[String, String]): Unit = {
    // extract from file
    val filePath = fileMap("filePath") // 提取传入的文件路径
    val df = extractFromFile(spark, filePath, schema) // 调用抽取文件数据源的方法，返回一个 DataFrame

    // load to hive
    val database = hiveMap("db") // 数据库
    val table = hiveMap("tb") // 数据表
    val partitionColumn = hiveMap.get("partitionColumn") // 分区列，注意这里是Option类型
    loadToHive(spark, df, database, table, partitionColumn) // 调用装载数据仓库的方法
  }

  // 定义一个 ELT 方法，包含 extract + load
  def eltFromJDBCToHive(spark: SparkSession, jdbcMap: Map[String, String], hiveMap: Map[String, String]): Unit = {
    // extract from jdbc
    val df = extractFromJDBC(spark, jdbcMap)

    // load to hive
    val database = hiveMap("db") // 数据库
    val table = hiveMap("tb") // 数据表
    loadToHive(spark, df, database, table, None)
  }
}
