package log_analysis

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.{UDF1, UDF2}
import org.apache.spark.sql.types.DataTypes
import utils.{DBUtils, DateUtils}

object RegisterFunctions {
  def main(args: Array[String]): Unit = {
    lazy val spark = SparkSession.builder().master("local[20]").appName("ReadKettleKjb").config("spark.debug.maxToStringFields", "100")
      .getOrCreate()

    lazy val context = spark.sparkContext
    context.setLogLevel("WARN")

    //    join_table_log(spark, "E:  DevelopToolsFiles  IdeaProjects  kettle_log_analysis  src  main  log  kettle_dev_s.log", "2020/05/18 09:50:21", context)
  }

  def register_all_func(spark: SparkSession): Unit = {
    register_judge_day_equal(spark)
    register_get_time_diff(spark)
    register_get_IORWUE(spark)
    register_get_str_number_diff(spark)
    register_convert_format(spark)
  }


  //  def join_table_log(spark: SparkSession, fileName: String, analysis_time: String, sc: SparkContext) = {
  //    import spark.implicits._
  //    val ktr_result = DBUtils.getMysqlData(spark, "ktr_result", "20", "jobName")
  //
  //    ktr_result.createOrReplaceTempView("ktr_result")
  //    ktr_result
  //      .show(100, false)
  //
  //    val log_result = TestKtr.get_log_IORWUE(fileName, analysis_time, spark, sc)
  //    log_result.createOrReplaceTempView("log_result")
  //
  //
  //    ktr_result
  //      .join(log_result, $"from_table" === $"tableName", "left")
  //      .where("tableName is not null")
  //      .drop("tableName")
  //      .show(100, false)
  //  }

  def register_judge_day_equal(spark: SparkSession) = {
    //注册临时函数
    //临时函数的功能是判断两个日期是否为同一天
    //示例如下
    //"2012/9/13 02:03:04", "2012/9/12 00:00:00"，该临时函数返回值为“false”
    spark.sqlContext.udf.register("register_judge_day_equal", new UDF2[String, String, String]() {
      @throws[Exception]
      override def call(d1: String, d2: String): String =
        if (d1 == null || d2 == null || d1 == "null" || d2 == "null" || d1 == "" || d2 == "") "0"
        else {
          DateUtils.judge_date_day_equal(d1, d2)
        }
    }, DataTypes.StringType)
  }


  def register_get_time_diff(spark: SparkSession) = {
    //注册临时函数
    //临时函数的功能是获取两个日期的时间差
    //示例如下
    //"2012/9/13 02:03:04", "2012/9/12 00:00:00"，该临时函数返回值为“1天2小时3分钟4秒钟”
    spark.sqlContext.udf.register("get_time_diff", new UDF2[String, String, String]() {
      @throws[Exception]
      override def call(d1: String, d2: String): String =
        if (d1 == null || d2 == null) "null"
        else {
          DateUtils.get_time_diff(d1, d2)
        }
    }, DataTypes.StringType)
  }

  def register_get_IORWUE(spark: SparkSession) = {
    //将日志详细信息处理。
    // 示例：处理前是==》完成处理 (I=2968, O=0, R=0, W=2968, U=0, E=0)
    //处理后==》I=2968,O=0,R=0,W=2968,U=0,E=0
    spark.sqlContext.udf.register("analysis_detail", new UDF1[String, String]() {
      var detail_arr = new Array[String](6)
      var str = ""
      val split = ","

      @throws[Exception]
      override def call(detail: String): String =
        if (detail == null || detail == "null" || detail.length == 0) {
          str = "0,0,0,0,0,0"
          str
        }
        else {
          if (detail.contains("完成处理") || detail.contains("Finished processing")) {
            detail_arr = detail
              .replaceAll("完成处理", "")
              .replaceAll("Finished processing", "")
              .replaceAll(" ", "")
              .replaceAll("\\(", "")
              .replaceAll("\\)", "")
              .split(",")
            if (detail_arr.length == 6) {
              //              val i = detail_arr(0).toLong //I 当前步骤生成的记录数（从表输出、文件读入）
              //              val o = detail_arr(1).toLong //O 当前步骤输出的记录数（输出的文件和表）
              //              val r = detail_arr(2).toLong //R 当前步骤从前一步骤读取的记录数
              //              val w = detail_arr(3).toLong //W 当前步骤向后面步骤抛出的记录数
              //              val u = detail_arr(4).toLong //U 当前步骤更新过的记录数
              //              val e = detail_arr(5).toLong //E 当前步骤处理的记录数
              str = detail_arr(0).split("\\=")(1) + split + detail_arr(1).split("\\=")(1) + split + detail_arr(2).split("\\=")(1) + split + detail_arr(3).split("\\=")(1) + split + detail_arr(4).split("\\=")(1) + split + detail_arr(5).split("\\=")(1)
            }
          }
          else {
            str = "0,0,0,0,0,0"
            str
          }
          str
        }
    }, DataTypes.StringType)
  }


  /**
    * 获取两个字符串类型的数值的差值
    *
    * @param spark
    */
  def register_get_str_number_diff(spark: SparkSession) = {
    //注册临时函数
    //临时函数的功能是获取两个字符串类型的数值的差值
    //示例如下
    //字符串一是“500”，字符串二是“2000”，其差值为2000-500=1500
    spark.sqlContext.udf.register("get_str_number_diff", new UDF2[String, String, Long]() {
      @throws[Exception]
      override def call(d1: String, d2: String): Long =
        if (d1 == null || d2 == null) 0L
        else {
          d2.toLong - d1.toLong
        }
    }, DataTypes.LongType)
  }


  /**
    * 将一个字符串（时间）转换成指定格式的时间
    *
    * @param spark
    */
  def register_convert_format(spark: SparkSession) = {
    //注册临时函数
    spark.sqlContext.udf.register("register_convert_format", new UDF1[String, String]() {
      @throws[Exception]
      override def call(date: String): String = {
        date.split(" ")(0)
      }
    }, DataTypes.StringType)
  }
}
