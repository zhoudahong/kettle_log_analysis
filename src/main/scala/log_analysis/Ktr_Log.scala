package log_analysis

import java.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, split, _}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import utils._

object Ktr_Log {
  val NULL_STRING = "null"
  //  val logger: Logger = Logger.getLogger
  //    val analysis_time = "2020/06/08 10:06:14"
  val analysis_time = DateUtils.get_now_time()
  //  val fileName = "E:  DevelopToolsFiles  IdeaProjects  kettle_log_analysis  src  main  log  kettle_dev_s.log"
  val fileName = "E:\\MyWork\\Sisyphe_work\\sisyphe_kettle_data\\kettle_dev_s.log"
  val TEMP_STR = ""

  val spark = SparkSession
    .builder()
    .config("spark.sql.shuffle.partitions", 300)
    .master("local[10]")
    .appName("ktr")
    //      .enableHiveSupport()
    .getOrCreate()
  val context = spark.sparkContext
  context.setLogLevel("WARN")


  //读取日志文本文件
  val file: RDD[String] = context.textFile(fileName)

  import spark.implicits._

  //开始判断出现错误的日志
  val log_split = file
    .filter(x => ((x != null) && (x.length != 0) && (x != "null"))) //过滤掉空行
    .persist(StorageLevel.MEMORY_ONLY)


  val log_df = log_split.map(x => x.split(" - ")) //切分日志
    .map(x => {
    val len = x.length
    if (len == 3) {
      ((x(2), x(0), x(1).replaceAll(".0", "").replaceAll(" ", "")))
    }
    else {
      (x(0), NULL_STRING, NULL_STRING)
    }
  })
    .toDF("detail", "date", "tableName").persist(StorageLevel.MEMORY_ONLY)


  //获取包含‘error’的行（如果有的话返回该行号，如果不包含‘error’则返回-1）
  val error_df = file.toDF("detail")
  val error_line_df = DataFrameUtils.withLineNOColumn(error_df, "line_no", spark)
  val error_line = error_line_df
    .filter(x => (x.toString().contains("error") || x.toString().contains("ERROR") || x.toString().contains("Error")))
    .filter(x => !x.toString().matches(".*org.osgi.service.cm.ManagedService.*"))

  println("  error_line.show(30, false)")
  error_line.show(30, false)
  error_line_df.createOrReplaceTempView("file_line_df")
  val error_line_list = DataFrameUtils.getLineNo(error_line, "line_no", spark)

  RegisterFunctions.register_all_func(spark)


  def main(args: Array[String]): Unit = {

    //    DateUtils.get_now_time()
    //
    ReadKettleKjb.get_kjb_result()

    //    get_log_IORWUE(fileName, analysis_time, spark, context)

    analysis_range(fileName, spark, context)
    //    get_log_IORWUE(fileName, analysis_time, spark, context)

    //    val detail_log_save = get_detail_log_save(log_df, analysis_time)
    //    println(detail_log_save.schema)
    DBUtils.save_Mysql_Append(get_detail_log_save(log_df, analysis_time), DBUtils.kettle_database, DBUtils.kettle_t_log_detail)


  }


  /**
    * 将对应的分析时间的日志详情保存到数据库
    *
    * @param df
    * @param ana_time
    * @return
    */
  def get_detail_log_save(df: DataFrame, ana_time: String): DataFrame = {
    val value = log_split.zipWithIndex()
      .toDF("detail", "line_no")
      .withColumn("line_no", col("line_no") + 1)


    val log_df = log_split.map(x => x.split(" - ")) //切分日志
      .map(x => {
      val len = x.length
      if (len == 3) {
        (x(0) + " - " + x(1) + " - " + x(2), x(0))
      }
      //避免出现这样的情况
      // 2020/06/09 02:32:15 - logsystem_store.0 - Starting the null bulk Load in a separate thread - LOAD DATA LOCAL INFILE '/tmp/fifo9' INTO TABLE logsystem_store FIELDS TERMINATED BY '		' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\' (store_id,store_number,store_name,store_address,store_state,create_time,creator);
      else if (len > 3) {
        var temp = ""
        for (i <- 0 to len - 1) {
          temp = temp + x(i)
        }
        (x(0), temp)
      }
      else {
        (x(0), NULL_STRING)
      }
    })
      .toDF("detail", "date")
      .persist(StorageLevel.MEMORY_ONLY)

    // 在原Schema信息的基础上添加一列 “id”信息
    val schema: StructType = log_df.schema.add(StructField("line_no", LongType))
    // DataFrame转RDD 然后调用 zipWithIndex
    val dfRDD: RDD[(Row, Long)] = log_df.rdd.zipWithIndex()
    val rowRDD: RDD[Row] = dfRDD
      .map(tp => Row.merge(tp._1, Row(tp._2)))

    // 将添加了索引的RDD 转化为DataFrame
    val log_id_time = spark.createDataFrame(rowRDD, schema)
      .withColumn("line_no", col("line_no") + 1)
      .withColumn("ana_time", lit(ana_time.substring(0, 10)))

    log_id_time.createOrReplaceTempView("log_id_time")
    val time_diff = spark.sql("select detail,date,line_no,ana_time,register_judge_day_equal(date,ana_time) as time_diff from log_id_time")

    time_diff.createOrReplaceTempView("time_diff")
    val day_equal = time_diff
      .filter("date is not null")
      .filter("time_diff=='1'")


    val start_line = day_equal.select("line_no")
      .orderBy($"line_no".asc)
      .limit(1)
      .collectAsList().toString.replaceAll("\\[", "").replaceAll("\\]", "")
    val stop_line = day_equal.select("line_no")
      .orderBy($"line_no".desc)
      .limit(1)
      .collectAsList().toString.replaceAll("\\[", "").replaceAll("\\]", "")

    var result: DataFrame = null
    if (start_line != null && stop_line != null) {
      result = spark.sql(s"SELECT detail,line_no,substr(date, 0, 10) as date  FROM time_diff WHERE line_no>=$start_line  AND  line_no<=$stop_line")
    }

    result
  }


  /**
    * 对日志分析一个时间范围，范围是数据库表中最大分析时间和传入的时间差（集合）
    *
    * @param fileName
    * @param spark
    * @param sc
    */
  def analysis_range(fileName: String, spark: SparkSession, sc: SparkContext) = {
    import spark.implicits._
    import scala.collection.JavaConversions._


    val log_ana_result = DBUtils.getMysqlData(spark, DBUtils.kettle_t_log_ana_result, "30", "id")
      .persist(StorageLevel.MEMORY_ONLY)
    log_ana_result.createOrReplaceTempView("log_ana_result")

    //获取数据库表中的时间范围
    var table_range = new util.LinkedList[String] //数据库表中的时间范围集合
    val table_range_list = spark.sql("SELECT substr(ana_time, 0, 10) AS ana_time FROM log_ana_result WHERE ana_time is not null AND ana_time!=''")
      .orderBy($"ana_time".asc)
      .distinct()
      .collectAsList()
    for (x <- table_range_list) {
      table_range.add(x.toString.replaceAll("\\[", "").replaceAll("\\]", ""))
    }


    //max_date表示数据库表中存在最大的时间
    var need_ana_range_list = new util.LinkedList[String]
    var max_date = TEMP_STR
    if (table_range.length > 0) { //数据库表中存在记录，说明之前分析过日志
      max_date = table_range.getLast //获取数据库表中最大的分析时间
      need_ana_range_list = DateUtils.get_date_range(max_date, analysis_time) //表中最大时间和分析时间的时间范围，即需要分析的时间范围

      for (d <- table_range) {
        if (DateUtils.judge_date_day_equal(d, analysis_time.substring(0, 10)) == "1") { //分析时间存在于数据库表中，无需重新分析
          val frame = DataFrameUtils.gennerate_log_df(spark, analysis_time, "未执行")
          DBUtils.save_Mysql_Append(frame, DBUtils.kettle_database, DBUtils.kettle_t_log_error_info)
        }
      }


      var count = 0;
      for (d <- need_ana_range_list) {
        count += 1
        get_log_IORWUE(fileName, d, spark, sc)
      }
    }
    else { //没有分析过日志，第一次分析
      get_log_IORWUE(fileName, analysis_time, spark, sc)
     }


    //获取日志中的时间范围
    log_df
      .where("date is not null and date!='null'")
      .select("date")
      .createOrReplaceTempView("log_all_date")
    val log_date_range = spark.sql("SELECT register_convert_format(date) AS day FROM log_all_date")
      .orderBy($"day".asc)
      .distinct()
      .collectAsList()
    var log_date_range_list = new util.LinkedList[Row]()
    for (date <- log_date_range) {
      if (date.getAs[String]("day").length == 10) {
        log_date_range_list.add(date)
      }
    }

    var suc_count = 0


    if (max_date != TEMP_STR && DateUtils.judge_date_bigger(max_date, analysis_time)) {
      val date_range = DateUtils.get_date_range(max_date, analysis_time)
      for (d <- date_range) { //对每一个时间都进行日志分析
        if (DateUtils.judge_in_range(d, log_date_range_list)) { //含有传入时间的日志
          for (t_date <- table_range_list) { //遍历数据库表中的时间
            suc_count += 1
            if (!DateUtils.judge_in_range(analysis_time, table_range_list)) { //分析时间不存在于数据库表中，对该时间进行分析

              if (DateUtils.judge_date_day_equal(t_date.toString.replaceAll("\\[", "").replaceAll("\\]", ""), analysis_time) == "1") { //分析时间和表中的时间是同一天，那么就分析
                get_log_IORWUE(fileName, analysis_time, spark, sc)

              }
            }

          }
        }
      }
    }
  }

  /**
    *
    *
    * @param fileName      文件名，全路径。示例：file:///E: DevelopToolsFiles IdeaProjects kettle_log_analysis src main data 6_财务对账报表执行.kjb
    * @param analysis_time 想要解析日志的时间，格式为yyyy/MM/dd hh:mm:ss
    * @param spark         SparkSession
    * @param sc            SparkContext
    */
  def get_log_IORWUE(fileName: String, analysis_time: String, spark: SparkSession, sc: SparkContext): Unit = {
    //时间示例2020/05/15 18:00:26
    // 在原Schema信息的基础上添加一列 “line_no”(行号从0自增)信息
    val schema: StructType = log_df.schema.add(StructField("line_no", LongType))
    // DataFrame转RDD 然后调用 zipWithIndex
    val dfRDD: RDD[(Row, Long)] = log_df.rdd.zipWithIndex()
    val rowRDD: RDD[Row] = dfRDD
      .map(tp => Row.merge(tp._1, Row(tp._2)))
    // 将添加了索引的RDD 转化为DataFrame
    val log_id_time = spark.createDataFrame(rowRDD, schema)
      .withColumn("line_no", col("line_no") + 1)
      .withColumn("curr_time", lit(analysis_time)) //添加当前时间
    log_id_time.createOrReplaceTempView("log_id_time")


    //判断数据库中最大的分析时间，然后判断和当前时间的范围，来分析每一天的日志
    val time_diff_df = spark.sql("select tableName,line_no,detail,date,curr_time,register_judge_day_equal(date,curr_time) as time_dif from log_id_time")

    //获取起始行
    var start_row_no = 0L
    val start_line = time_diff_df
      //      .where("detail LIKE '%开始项[益华数据]%'")
      .where("detail LIKE '%Starting entry [益华数据]%'")
      .filter("time_dif='1'")
      .select("line_no")
      .distinct()
      .limit(1)
      .collectAsList
    var start_line_list = new util.LinkedList[Long]

    import scala.collection.JavaConversions._

    for (row <- start_line) {
      val u_id = row.toString.replaceAll("\\[", "").replaceAll("\\]", "").toLong
      start_line_list.add(u_id)
    }

    if (start_line_list.size() > 0) {
      start_row_no = start_line_list.getFirst //起始行结果
    }
    else {
      //将该错误信息保存到error表
      //      val frame = DataFrameUtils.gennerate_log_df(spark, analysis_time, "无法获取到起始行：开始项[益华数据]")
      //      DBUtils.save_Mysql_Append(frame, DBUtils.kettle_database, DBUtils.kettle_t_log_error_info)
    }


    //获取结尾行
    var end_row_no = 0L
    var end_line_list = new util.LinkedList[Long]
    val one_day_df = time_diff_df
      .where("date!='null'") //日期不为空
      .where("time_dif='1'")


    val end_line_df = one_day_df //和解析时间是同一天
      .orderBy($"line_no".desc, $"date".desc)
      .select("line_no")



    val end_line = end_line_df.limit(1)
      .collectAsList()
    for (row <- end_line) {
      val u_id = row.toString.replaceAll("\\[", "").replaceAll("\\]", "").toLong
      end_line_list.add(u_id)
    }


    if (end_line_list.size() > 0) {
      end_row_no = end_line_list.getFirst //结尾行结果
    }
    else {
      //将该错误信息保存到error表
      //      val not_in_log = DataFrameUtils.gennerate_log_df(spark, analysis_time, "无法获取到该日的最大日期行")
      //      DBUtils.save_Mysql_Append(not_in_log, DBUtils.kettle_database, DBUtils.kettle_t_log_error_info)
    }

    println("start_row_no:" + start_row_no + "  end_row_no:" + end_row_no)


    if (error_line_list.length > 0) {
      //如果大于0，说明日志中包含error，将error保存到数据库中
      var it = error_line_list.iterator()
      println("移除不在范围之前的error_line_list:" + error_line_list)
      while (it.hasNext) {
        val l = it.next()
        //        println("error line detail: " + "start_row_no:" + start_row_no + "  end_row_no:" + end_row_no)

        if ((start_row_no > 0) && (end_row_no > 0) && (l < start_row_no || l > end_row_no)) {
          //移除掉不在时间范围内的
          it.remove()
        }
      }

      if (error_line_list.length > 0) {
        println("移除不在范围之后的error_line_list:" + error_line_list)
        val error_line_first = error_line_list.getFirst
        //      println(error_line_first + "error_line_first")
        val error_result = spark.sql(
          s"""
             |SELECT detail,line_no
             |FROM file_line_df
             |WHERE line_no >=$error_line_first AND line_no>=$start_row_no  AND line_no<=$end_row_no
             |ORDER BY line_no ASC
  """.stripMargin)
          .withColumn("analysis_time", lit(analysis_time))
          .withColumn("is_successed", lit("失败"))

        error_result.show(false)
        DBUtils.save_Mysql_Append(error_result, DBUtils.kettle_database, "log_error_info")
      }
      else {
        deal_right_log(analysis_time, spark)
      }
    }
    else {
      deal_right_log(analysis_time, spark)
    }
  }

  def deal_right_log(analysis_time: String, spark: SparkSession): Unit = {
    //处理正常日志
    //切分每行，并过滤得到长度为3的每行，标准数据示例如下：
    //2020/05/16 02:30:45 - start_execution - 开始项[益华数据]
    //2020/05/16 02:30:45 - Carte - Installing timer to purge stale objects after 1440 minutes.
    val split_length = file
      .map(x => x.split("-"))
      .filter(x => x.length == 3)
    split_length.persist(StorageLevel.MEMORY_ONLY)


    //将2020/05/17 04:19:24 - coffee_sale_零售支付明细.0 - linenr 50000，
    //变成（coffee_sale_零售支付明细.0，2020/05/17 04:19:24，linenr 50000）
    val transform = split_length
      .map(x => (x(1).replaceAll(".0", "").replaceAll(" ", ""), x(0), x(2))) //(表名，日期，输出内容)
      .filter(x =>
      (x._2.replaceAll(" ", "").length == 18) && DateUtils.judge_date_day_equal(x._2, analysis_time) == "1") //过滤掉不含日期的行，日期格式为2020/05/17 04:19:24


    val data_df = transform.toDF("tableName", "date", "detail")
    data_df.show(false)
    data_df.createOrReplaceTempView("data_df")


    val min_max = spark.sql("select tableName,min(date) as min_date,max(date) as max_date,first(detail) as first_detail,last(detail) as last_detail from data_df group by tableName ")

    spark.catalog.dropTempView("data_df")

    min_max.createOrReplaceTempView("min_max")
    //    min_max.show(30, false)


    val time_diff = spark.sql("select tableName,min_date,max_date,last_detail,get_time_diff(max_date,min_date) as time_diff from min_max")
    //    time_diff.show(100, false)
    spark.catalog.dropTempView("min_max")

    time_diff.createOrReplaceTempView("time_diff")
    val IORWUE = spark.sql("select tableName,min_date,max_date,get_time_diff(max_date,min_date) as time_diff,analysis_detail(last_detail) as IORWUE from time_diff")
    println(" IORWUE.show(100, false)")
    IORWUE.show(30, false)

    //将IORWUE单个提取出来，成为每一列
    val OW_result = IORWUE
      //      .withColumn("I", split(col("IORWUE"), "  ,").getItem(0))
      .withColumn("O", split(col("IORWUE"), "  ,").getItem(1))
      //      .withColumn("R", split(col("IORWUE"), "  ,").getItem(2))
      .withColumn("W", split(col("IORWUE"), "  ,").getItem(3))
      //      .withColumn("U", split(col("IORWUE"), "  ,").getItem(4))
      //      .withColumn("E", split(col("IORWUE"), "  ,").getItem(5))
      .drop("IORWUE")

    //      .withColumn("now_time", lit(DateUtils.get_now_time())) //用lit关键字可以给一列的值作为固定值

    OW_result.show()
    //    println(OW_result.count())
    spark.catalog.dropTempView("time_diff")

    import spark.implicits._

    val ktr_result = DBUtils.getMysqlData(spark, "ktr_result", "20", "jobName")
    ktr_result.createOrReplaceTempView("ktr_result")
    OW_result.createOrReplaceTempView("log_result")


    val ktr_write = ktr_result
      .join(OW_result, $"from_table" === $"tableName", "left")
      .where("tableName is not null")
      .withColumn("ana_time", lit(analysis_time))
      .drop("tableName")
    //        println(ktr_write.schema)
    DBUtils.save_Mysql_Append(ktr_write, DBUtils.kettle_database, "log_ana_result")


    //写入成功状态
    val frame = DataFrameUtils.gennerate_log_df(spark, analysis_time, "成功")
    DBUtils.save_Mysql_Append(frame, DBUtils.kettle_database, DBUtils.kettle_t_log_error_info)
  }
}