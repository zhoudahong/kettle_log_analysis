package utils

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConversions._

object DataFrameUtils {
  /**
    * 给df加上自增列line_no
    *
    * @param df      原始df
    * @param colName 自增列的列名
    * @param spark   SparkSession
    * @return df加上自增列line_no后的DataFrame
    */
  def withLineNOColumn(df: DataFrame, colName: String, spark: SparkSession): DataFrame = {
    val schema = df.schema.add(StructField(colName, LongType))

    val dfRDD = df.rdd.zipWithIndex()
    val rowRDD: RDD[Row] = dfRDD
      .map(tp => Row.merge(tp._1, Row(tp._2)))
    // 将添加了索引的RDD 转化为DataFrame
    val result = spark.createDataFrame(rowRDD, schema)
      .withColumn(colName, col(colName) + 1)
    result
  }


  /**
    * 获取行号
    *
    * @param df      原始df
    * @param colName 自增列的列名
    * @param spark   SparkSession
    * @return df加上自增列line_no后的DataFrame
    */
  def getLineNo(df: DataFrame, colName: String, spark: SparkSession): util.LinkedList[Long] = {
    import spark.implicits._
    var link_list = new util.LinkedList[Long]
    if (df.count() == 0) {
      link_list
    }
    else {
      val rows = df
        .select(colName)
        .orderBy($"line_no".asc)
        .distinct()
        .collectAsList()
      //      println(rows)

      for (elem <- rows) {
        val toLong = elem.toString.replaceAll("\\[", "").replaceAll("\\]", "").toLong
        link_list.add(toLong)
      }
      link_list
    }
  }

  def gennerate_log_df(spark: SparkSession, time: String,mes:String) = {
    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(Seq(("NAN", "NAN", time, mes)), 1)
    val frame = rdd.toDF("detail", "line_no", "analysis_time", "is_successed")
    //    |detail|line_no|      analysis_time|is_successed|
    //    |   NAN|    NAN|2020/05/16 06:06:14|          成功|
    frame
  }


}
