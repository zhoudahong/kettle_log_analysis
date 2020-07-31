package log_analysis

import java.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, split, _}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import utils.{DBUtils, DataFrameUtils, DateUtils}

object Ktr_Log {
  val NULL_STRING = "null"
  //  val logger: Logger = Logger.getLogger
  //  val analysis_time = "2020/07/27 16:06:14"
  lazy val analysis_time = DateUtils.get_now_time()
  //  windows日志文件路径
  //  val fileName = "D:\\SoftWares\\developDatas\\kettle_datas\\kettle_dev_1.log"
  //  linux日志文件路径
  val fileName = "file:///data_dev/software/kettle_job/kettle_dev_1.log"
  val TEMP_STR = ""
  val spark: SparkSession = null


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.shuffle.partitions", 300)
      //      .master("local[*]") //windows测试用
      .master("yarn") //集群提交任务用
      .appName("kettle_log_ana，time： " + analysis_time)
      .config("spark.debug.maxToStringFields", "500")
      //      .enableHiveSupport()
      .getOrCreate()

    val context = spark.sparkContext
    context.setLogLevel("WARN")

    //  注册使用到的临时函数
    RegisterFunctions.register_all_func(spark)
    //  读取kjb和ktr文件（此处注释掉，让其单独本地运行，否则在集群上运行sparkSession会空指针）
    //  ReadKettleKjb.get_kjb_result(spark)
    //  开始分析日志
    analysis_range(fileName, spark, context)

    //  将全部日志文件写入到kettle_log_detail表
    val file = get_file(context) //读取日志文件
    val log_split = get_log_split(file) //获取日志文件的DF
    //  将日志文件DF保存到数据库kettle_log_detail表中
    DBUtils.save_Mysql_Append(get_detail_log_save(log_split, analysis_time, spark), DBUtils.kettle_database, DBUtils.kettle_t_log_detail)


    spark.stop()
  }

  //  读取日志文本文件
  def get_file(context: SparkContext): RDD[String] = {
    val file: RDD[String] = context.textFile(fileName)
    file.cache()
  }

  //  获取全部日志信息
  def get_log_split(file: RDD[String]): RDD[String] = {
    val log_split = file
      .filter(x => ((x != null) && (x.length != 0) && (x != "null"))) //过滤掉空行
      .persist(StorageLevel.MEMORY_ONLY)
    log_split
  }

  /**
   * 将对应的分析时间的日志详情保存到数据库
   *
   * @param log_split
   * @param ana_time
   * @return
   */
  def get_detail_log_save(log_split: RDD[String], ana_time: String, spark: SparkSession): DataFrame = {
    if (spark == null) {
      println("spark对象空了")
    }
    import spark.implicits._

    println("日志文件log_split.count总条数" + log_split.count())

    //  将日志文件切割，判断字段数量
    val log_df = log_split.map(x =>
      x.split(" - ")) //切分日志
      .map(x => {
        val len = x.length
        //  如果日志字段数长度为3，说明是正常的日志信息，转换成tuple2（日志全部内容，日志产生时间）
        if (len == 3) {
          (x(0) + " - " + x(1) + " - " + x(2), x(0))
        }

        //  避免日志内容出现以下这样的情况：（用“-”切割后长度不正常）
        //  2020/06/09 02:32:15 - logsystem_store.0 - Starting the null bulk Load in a separate thread - LOAD DATA LOCAL INFILE '/tmp/fifo9' INTO TABLE logsystem_store FIELDS TERMINATED BY '		' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\' (store_id,store_number,store_name,store_address,store_state,create_time,creator);
        else if (len > 3) {
          //  如果日志字段数长度大于3，说明是异常的日志信息，转换成tuple2（日志产生时间，日志全部内容）
          var temp = ""
          for (i <- 0 to len - 1) {
            temp = temp + x(i)
          }
          (temp, x(0))
        }
        else {
          (x(0), NULL_STRING)
        }
      })
      .toDF("detail", "date")
      .persist(StorageLevel.MEMORY_ONLY)
    println("日志文件切割后的总条数：" + log_df.count())

    //  在原Schema信息的基础上添加一列 “id”信息
    val schema: StructType = log_df.schema.add(StructField("line_no", LongType))
    // DataFrame转RDD，然后调用 zipWithIndex，给日志加上行号。
    val dfRDD: RDD[(Row, Long)] = log_df.rdd.zipWithIndex()
    val rowRDD: RDD[Row] = dfRDD
      .map(tp => Row.merge(tp._1, Row(tp._2)))

    //  将添加了索引的RDD 转化为DataFrame
    val log_id_time = spark.createDataFrame(rowRDD, schema)
      .withColumn("line_no", col("line_no") + 1)
      .withColumn("ana_time", lit(ana_time.substring(0, 10)))

    //  获取到日志中的时间等于参数“分析时间”的日志数据
    log_id_time.createOrReplaceTempView("log_id_time")
    val time_diff = spark.sql(
      s"""
         |SELECT detail, date, line_no, ana_time, register_judge_day_equal(date,ana_time) AS time_diff
         | FROM log_id_time
         |""".stripMargin)
    time_diff.createOrReplaceTempView("time_diff")
    val day_equal = time_diff
      .filter(" date is not null ")
      .filter(" time_diff == '1' ") //“1”表示日志中的时间等于参数“分析时间”的日志数据


    //  获取开始行号
    val start_line = day_equal.select("line_no")
      .orderBy($"line_no".asc)
      .limit(1)
      .collectAsList().toString.replaceAll("\\[", "").replaceAll("\\]", "")
    //  获取结束行号
    val stop_line = day_equal.select("line_no")
      .orderBy($"line_no".desc)
      .limit(1)
      .collectAsList().toString.replaceAll("\\[", "").replaceAll("\\]", "")

    //  截取日志数据，只需要日志中的时间等于分析时间，且开始行号与结束行号之间的数据
    var result: DataFrame = null
    if (start_line != null && stop_line != null) {
      result = spark.sql(s"SELECT detail,line_no,substr(date, 0, 10) as date " +
        s" FROM time_diff " +
        s" WHERE line_no>=$start_line  AND  line_no<=$stop_line")
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


    //  获得数据库表中最大分析时间和传入的时间差（集合）
    val log_ana_result = DBUtils.getMysqlData(spark, DBUtils.kettle_t_log_ana_result, "2", "id")
      .persist(StorageLevel.MEMORY_ONLY)
    log_ana_result.createOrReplaceTempView("log_ana_result")

    //  获取数据库表中的时间范围table_range_list
    val table_range = new util.LinkedList[String] //数据库表中的时间范围集合
    val table_range_list = spark.sql("SELECT substr(ana_time, 0, 10) AS ana_time FROM log_ana_result WHERE ana_time is not null AND ana_time!=''")
      .orderBy($"ana_time".asc)
      .distinct()
      .collectAsList()
    for (x <- table_range_list) {
      table_range.add(x.toString.replaceAll("\\[", "").replaceAll("\\]", ""))
    }


    //  max_date表示数据库表中存在最大的时间
    var need_ana_range_list = new util.LinkedList[String]
    var max_date = TEMP_STR
    if (table_range.length > 0) { //  数据库表中存在记录，说明之前分析过日志
      println("数据库表中存在记录，说明之前分析过日志")
      max_date = table_range.getLast // 获取数据库表中最大的分析时间
      println("数据库表中最大的分析时间:" + max_date)
      need_ana_range_list = DateUtils.get_date_range(max_date, analysis_time) //  表中最大时间和分析时间的时间范围，即需要分析的时间范围
      println("需要分析的时间范围：" + need_ana_range_list.toString)
      for (d <- table_range) {
        //  分析时间存在于数据库表中，无需重新分析，直接写入“未执行”为当前分析时间状态到kettle_log_error_info表中
        if (DateUtils.judge_date_day_equal(d, analysis_time.substring(0, 10)) == "1") {
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
    else { // 没有分析过日志，第一次分析
      println("没有分析过日志，第一次分析")
      get_log_IORWUE(fileName, analysis_time, spark, sc)
    }

    //  获取日志中的时间范围
    val file = get_file(sc)
    val log_split = get_log_split(file)
    val log_df: DataFrame = get_log_df(log_split, spark)
    log_df
      .where("date is not null and date!='null'")
      .select("date")
      .createOrReplaceTempView("log_all_date")
    val log_date_range = spark.sql("SELECT register_convert_format(date) AS day FROM log_all_date")
      .orderBy($"day".asc)
      .distinct()
      .collectAsList()
    val log_date_range_list = new util.LinkedList[Row]()
    for (date <- log_date_range) {
      if (date.getAs[String]("day").length == 10) {
        log_date_range_list.add(date)
      }
    }

    var suc_count = 0
    if (max_date != TEMP_STR && DateUtils.judge_date_bigger(max_date, analysis_time)) {
      val date_range = DateUtils.get_date_range(max_date, analysis_time)
      for (d <- date_range) { //  对每一个时间都进行日志分析
        if (DateUtils.judge_in_range(d, log_date_range_list)) { //  含有传入时间的日志
          for (t_date <- table_range_list) { // 遍历数据库表中的时间
            suc_count += 1
            if (!DateUtils.judge_in_range(analysis_time, table_range_list)) { //  分析时间不存在于数据库表中，对该时间进行分析
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
   * 将以下格式的日志数据转换
   * 2020/07/26 00:31:07 - coffee_bill_mistake_bill.0 - Connected to database [192.168.21.44_sisyphe_ods] (commit=1000)
   * 转换结果为tuple3（日志详情，日志时间，kettle输出或输出的相关表名），即tuple3（Connected to database [192.168.21.44_sisyphe_ods] (commit=1000)，2020/07/26 00:31:07，coffee_bill_mistake_bill）
   *
   * @param log_split
   * @param spark
   * @return
   */
  def get_log_df(log_split: RDD[String], spark: SparkSession): DataFrame = {
    import spark.implicits._
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
    log_df
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
    val file = get_file(sc)
    val log_split = get_log_split(file)
    val log_df: DataFrame = get_log_df(log_split, spark)

    //  时间示例2020/05/15 18:00:26
    //  在原Schema信息的基础上添加一列 “line_no”(行号从0自增)信息
    val schema: StructType = log_df.schema.add(StructField("line_no", LongType))
    // DataFrame转RDD 然后调用 zipWithIndex
    val dfRDD: RDD[(Row, Long)] = log_df.rdd.zipWithIndex()
    val rowRDD: RDD[Row] = dfRDD
      .map(tp => Row.merge(tp._1, Row(tp._2)))
    //  将添加了索引的RDD 转化为DataFrame
    val log_id_time = spark.createDataFrame(rowRDD, schema)
      .withColumn("line_no", col("line_no") + 1)
      .withColumn("curr_time", lit(analysis_time)) //添加当前时间
    log_id_time.createOrReplaceTempView("log_id_time")


    //  判断数据库中最大的分析时间，然后判断和当前时间的范围，来分析每一天的日志
    val time_diff_df = spark.sql(
      s"""
         |SELECT tableName, line_no, detail, date, curr_time,
         |register_judge_day_equal(date,curr_time) AS time_dif
         |FROM log_id_time
         |""".stripMargin)

    //  获取起始行
    var start_row_no = 0L
    val start_line = time_diff_df
      //      .where("detail LIKE '%开始项[益华数据]%'")
      .where("detail LIKE '%[益华数据]%'")
      .filter("time_dif='1'")
      .select("line_no")
      .distinct()
      .limit(1)
      .collectAsList
    val start_line_list = new util.LinkedList[Long]

    import scala.collection.JavaConversions._

    for (row <- start_line) {
      val u_id = row.toString.replaceAll("\\[", "").replaceAll("\\]", "").toLong
      start_line_list.add(u_id)
    }

    if (start_line_list.size() > 0) {
      start_row_no = start_line_list.getFirst //  最终得到的起始行行号
    }
    else {
      //  将该错误信息保存到error表
      //  val frame = DataFrameUtils.gennerate_log_df(spark, analysis_time, "无法获取到起始行：开始项[益华数据]")
      //  DBUtils.save_Mysql_Append(frame, DBUtils.kettle_database, DBUtils.kettle_t_log_error_info)
    }


    import spark.implicits._
    //  获取结尾行行号
    var end_row_no = 0L
    val end_line_list = new util.LinkedList[Long]
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
      //  将该错误信息保存到error表
      //  val not_in_log = DataFrameUtils.gennerate_log_df(spark, analysis_time, "无法获取到该日的最大日期行")
      //  DBUtils.save_Mysql_Append(not_in_log, DBUtils.kettle_database, DBUtils.kettle_t_log_error_info)
    }
    println("start_row_no:" + start_row_no + "  end_row_no:" + end_row_no)


    import spark.implicits._
    //  获取包含‘error’的行（如果有的话返回该行号，如果不包含‘error’则返回-1）
    val error_df = file.toDF("detail")
    val error_line_df = DataFrameUtils.withLineNOColumn(error_df, "line_no", spark)
    //  日志中包含“error”大小写的单词，均视为错误
    val error_line = error_line_df
      .filter(x => (x.toString().contains("error") || x.toString().contains("ERROR") || x.toString().contains("Error")))
      .filter(x => !x.toString().matches(".*org.osgi.service.cm.ManagedService.*"))

    println("  error_line.show(30, false)")
    error_line.show(30, false)
    error_line_df.createOrReplaceTempView("file_line_df")
    val error_line_list = DataFrameUtils.getLineNo(error_line, "line_no", spark)

    //  如果error_line_list大于0，说明日志中包含error，还需进一步判断这些error的时间是否跟分析时间相等
    if (error_line_list.length > 0) {
      val error_iterator = error_line_list.iterator()
      println("移除不在范围之前的error_line_list:" + error_line_list)
      while (error_iterator.hasNext) {
        val l = error_iterator.next()
        //  println("error line detail: " + "start_row_no:" + start_row_no + "  end_row_no:" + end_row_no)
        if ((start_row_no > 0) && (end_row_no > 0) && (l < start_row_no || l > end_row_no)) {
          //  移除掉不在时间范围内的
          error_iterator.remove()
        }
      }

      //  如果移除掉不在分析时间范围的error行之后，还存在error行。说明kettle运行出现的错误，将这些错误日志保存到log_error_info数据库表中
      if (error_line_list.length > 0) {
        println("移除不在范围之后的error_line_list:" + error_line_list)
        val error_line_first = error_line_list.getFirst
        //  获取到出现错误的日志DataFrame，并添加“失败”状态
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
        //  将日志中出现的error保存到数据库表中
        DBUtils.save_Mysql_Append(error_result, DBUtils.kettle_database, "log_error_info")
      }
      else {
        //  如果移除掉不在分析时间范围的error行之后，不存在error行，说明kettle运行正常，分析这些正常日志（得到输入输出结果量）
        deal_right_log(analysis_time, spark, sc)
      }
    }

    else {
      //  如果不大于0，说明日志中不包含error，直接分析该正常日志内容
      deal_right_log(analysis_time, spark, sc)
    }
  }


  /**
   * 处理正常日志（日志中不存在任何error信息）。
   * 示例：2020/07/26 00:31:07 - coffee_baseinfo_v2_货物数据表_全量.0 - 完成处理 (I=319, O=0, R=0, W=319, U=0, E=0)
   * 将以上格式的正常日志与kte
   *
   * @param analysis_time 想要分析日志的时间
   * @param spark         SparkSession
   * @param sc            SparkContext
   */
  def deal_right_log(analysis_time: String, spark: SparkSession, sc: SparkContext): Unit = {
    import spark.implicits._
    val file = get_file(sc)
    /*处理正常日志
    切分每行，并过滤得到长度为3的每行，标准数据示例如下：
    2020/07/26 00:31:07 - oauth_database_用户部门数据表.0 - 完成处理 (I=8683, O=0, R=0, W=8683, U=0, E=0)
    2020/05/16 02:30:45 - Carte - Installing timer to purge stale objects after 1440 minutes.*/
    val split_length = file
      .map(x => x.split("-"))
      .filter(x => x.length == 3)
    split_length.persist(StorageLevel.MEMORY_ONLY)

    /*   将2020/05/17 04:19:24 - coffee_sale_零售支付明细.0 - linenr 50000，
       变成Tuple3（coffee_sale_零售支付明细.0，2020/05/17 04:19:24，linenr 50000）*/
    val transform = split_length
      .map(x => (x(1).replaceAll(".0", "").replaceAll(" ", ""), x(0), x(2))) //(表名，日期，输出内容)
      .filter(x => { // 日期字符串去掉空格之后，其长度必须为18，日期格式为2020/05/17 04:19:24
        (x._2.replaceAll(" ", "").length == 18) && DateUtils.judge_date_day_equal(x._2, analysis_time) == "1" //过滤掉不含日期的行，日期格式为2020/05/17 04:19:24
      })

    val data_df = transform.toDF("tableName", "date", "detail")
    data_df.show(false)
    data_df.createOrReplaceTempView("data_df")
    //  获取tableName（kettle的表名）出现的最大日期，最小日期，第一次出现的日志详情，最后一次出现的日志详情。根据tableName进行分组
    val min_max = spark.sql(
      s"""
         | SELECT tableName, MIN(date) AS min_date, MAX(date) AS max_date, FIRST(detail) AS first_detail, LAST(detail) AS last_detail
         | FROM data_df
         | GROUP BY tableName
         |""".stripMargin)

    spark.catalog.dropTempView("data_df")
    min_max.createOrReplaceTempView("min_max")
    //  min_max.show(30, false)


    //  获取分析时间和日志时间相同的日志数据
    val time_diff = spark.sql(
      s"""
         | SELECT tableName, min_date, max_date, last_detail, get_time_diff(max_date,min_date) AS time_diff
         | FROM min_max
         |""".stripMargin)
    //  time_diff.show(100, false)
    spark.catalog.dropTempView("min_max")
    time_diff.createOrReplaceTempView("time_diff")

    /*  获取日志中的IORWUE信息。
      即2020/07/26 00:31:07 - oauth_database_用户部门数据表.0 - 完成处理 (I=8683, O=0, R=0, W=8683, U=0, E=0)
      I 是指当前 (步骤) 生成的记录（从表输入、文件读入）
      O 是指当前 (步骤) 输出的记录数（输出到文件、表）
      R 是指当前 (步骤) 从前一步骤读取的记录数
      W 是指当前 (步骤) 向后面步骤抛出的记录数
      U 是指当前 (步骤) 更新过的记录数
      E 是指当前 (步骤) 处理的记录数*/
    val IORWUE = spark.sql(
      s"""
         | SELECT tableName, min_date, max_date, get_time_diff(max_date,min_date) AS time_diff, analysis_detail(last_detail) AS IORWUE
         | FROM time_diff
         |""".stripMargin)
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
    val ktr_result = DBUtils.getMysqlData(spark, "ktr_result", "2", "jobName")
    ktr_result.createOrReplaceTempView("ktr_result")
    OW_result.createOrReplaceTempView("log_result")


    //  将ktr表和输入输出详情表join，得到要写入log_ana_result数据库表的结果
    val ktr_write = ktr_result
      .join(OW_result, $"from_table" === $"tableName", "left")
      .where("tableName is not null")
      .withColumn("ana_time", lit(analysis_time))
      .drop("tableName")
    //  println(ktr_write.schema)
    println("log_ana_result写入 count:" + ktr_write.count())
    DBUtils.save_Mysql_Append(ktr_write, DBUtils.kettle_database, "log_ana_result")


    //  写入“成功”状态到“kettle_t_log_error_info”表中
    val succeed_frame = DataFrameUtils.gennerate_log_df(spark, analysis_time, "成功")
    succeed_frame.show(false)
    DBUtils.save_Mysql_Append(succeed_frame, DBUtils.kettle_database, DBUtils.kettle_t_log_error_info)
  }
}