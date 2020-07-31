package log_analysis

import java.util

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import utils.{Constants, DBUtils, DateUtils, Logger}

/**
 * 用spark解析kettle产生的xml文件，读取kettle数据流转信息
 */
object ReadKettleKjb {
  val NULL_STRING = "null"
  val logger: Logger = Logger.getLogger
  val analysis_time = DateUtils.get_now_time()

  //  windows文件夹路径（存放kettle运行产生的kjb和ktr的文件夹）
  //  lazy val FOLDER_NAME = "file:///D:\\SoftWares\\kettle8.2\\datas\\XXF_EDW_SS_1\\"
  //  linux文件路径（存放kettle运行产生的kjb和ktr的文件夹）
  lazy val FOLDER_NAME = "file:///data_dev/software/kettle_job/"


  lazy val KTR_SUFFIX = ".ktr" //KTR文件后缀名
  lazy val KJB_SUFFIX = ".kjb" //KJB文件后缀名
  lazy val START_FILENAME = "start_execution.kjb" //入口文件名
  var spark: SparkSession = null
  var context: SparkContext = null


  def main(args: Array[String]): Unit = {
    spark = SparkSession.builder()
      //            .master("yarn")
      .master("local[*]") //local[*]表示使用尽可能多的线程来运行
      .appName("ReadKettleKjb,time :" + analysis_time)
      .config("spark.debug.maxToStringFields", "400")
      .getOrCreate()

    context = spark.sparkContext
    context.setLogLevel("WARN")
    get_kjb_result()
  }

  /**
   * 读取kettle产生的start_execution.kjb文件，并写入。
   * start_execution.kjb文件是kettle每一次运行都会生成的入口文件，里面包含全部流转过程内容。
   */
  def get_kjb_result() = {
    register_split_filename(spark)

    //  解析初始文件start_execution.kjb得到dataframe
    val get_name_filename = analysis_kjb("start", START_FILENAME, spark)
    get_name_filename.createTempView("name_filename")

    //  使用split_filename临时函数切分filename，获取name和对应的ktr或者kjb文件
    val split_filename = spark.sql(
      s"""
         |SELECT name, split_filename(filename) AS filename
         | FROM name_filename
         |""".stripMargin)
    println("split_filename.show(30, false)...")
    split_filename.show(30, false)
    /**
     * +----------------------+-------------------------------------------------+
     * |name                  |filename                                         |
     * +----------------------+-------------------------------------------------+
     * |DDS维度表数据抽取            |B_DDS_维度表查询.ktr                                  |
     * |ODS层_只抽取会员及咖啡所需的数据    |A_555_ODS_Liunx全量抽取_只抽取会员卡包member_database的数据.ktr|
     * |ods层_Liunx系统传数2       |linux系统传参2.ktr                                   |
     */

    //  出现错误：Error:(72, 23) Unable to find encoder for type stored in a Dataset.  Primitive types (Int, String, etc) and Product types (case classes) are supported by importing spark.implicits._  Support for serializing other types will be added in future releases.
    //  解决如下：
    implicit val encoder = org.apache.spark.sql.Encoders.STRING //添加字符串类型编码器
    implicit val matchError = org.apache.spark.sql.Encoders.tuple(Encoders.STRING, Encoders.STRING)
    implicit val matchError2 = org.apache.spark.sql.Encoders.kryo[util.HashMap[String, String]]

    //  每次写入前，先清空数据库中的ktr_result表
    Constants.trunc_table(DBUtils.kettle_database, DBUtils.kettle_t_ktr_result)

    //  对每一个ktr文件进行解析
    split_filename.foreach(x => {
      //  已解决：foreach里面sparkSession对象空指针问题
      //  （解决办法为在foreach算子里面调用sparkSession对象的时候，需要先判断该对象是否为空，如果为空，需要重新创建该对象）

      /**
       * 如果filename后缀名是.ktr，那么读取该ktr文件，然后找到<order>，遍历全部<hop>，
       * 拿到其中的 <from>member_database_会员积分明细</from>, <to>member_database_member_integral_details</to>
       * 再和日志文件kettle_dev_s.log匹配，进行日志解析
       */
      val jobName = x.getAs[String]("name")
      val fileName = x.getAs[String]("filename")
      if (fileName.contains(".ktr") && fileName != "kettle_job_all_succeed.ktr") {
        val str = fileName.replaceAll("\\[", "").replaceAll("\\]", "")


        if (spark != null) { // 此处spark对象容易为null，抛出空指针异常
          val ktr: DataFrame = analysis_ktr(jobName, str, spark)

          ktr.show(20, false)
          //+--------+------------+-------------+----------------------+
          //|jobName |ktrName     |from_table   |to_table              |
          //+--------+------------+-------------+----------------------+
          //|DDS_fifo|dds_fifo.ktr|2.0版本_营运日志_客流|station_passenger_flow|
          //将job名和ktr文件名和其中的读取和写入相关表信息，结果写入到mysql数据库中
          DBUtils.save_Mysql_Append(ktr, DBUtils.kettle_database, DBUtils.kettle_t_ktr_result)
        }
        else { //  sparkSession对象为空的时候，需要先创建该对象
          val newspark = SparkSession.builder()
            //                  .master("yarn")
            .master("local[*]") //local[*]表示使用尽可能多的线程来运行
            .getOrCreate()
          val ktr: DataFrame = analysis_ktr(jobName, str, newspark)
          ktr.show(20, false)
          //+--------+------------+-------------+----------------------+
          //|jobName |ktrName     |from_table   |to_table              |
          //+--------+------------+-------------+----------------------+
          //|DDS_fifo|dds_fifo.ktr|2.0版本_营运日志_客流|station_passenger_flow|
          //将job名和ktr文件名和其中的读取和写入相关表信息，结果写入到mysql数据库中
          DBUtils.save_Mysql_Append(ktr, DBUtils.kettle_database, DBUtils.kettle_t_ktr_result)
        }
      }

      else if (fileName.contains(".kjb")) { //  如果是kjb文件，就找到<entries>下面的<entry>，获取其<name>和<filename>
        analysis_kjb(jobName, fileName.replaceAll("\\[", "").replaceAll("\\]", ""), spark)
      }
    })
    println("get_kjb_result完成")
  }

  /**
   * 注册临时函数, 临时函数的功能是将字符串中的不需要的部分切除
   * 示例: 将" ${Internal.Entry.Current.Directory}/A_444_ODS_表日期更新.ktr "，变为" A_444_ODS_表日期更新.kt r"
   *
   * @param spark
   */
  def register_split_filename(spark: SparkSession) = {
    spark.sqlContext.udf.register("split_filename", new UDF1[String, String]() {
      @throws[Exception]
      override def call(s: String): String = if (s == null) "null"
      else {
        val st = s.split("\\/") //是以“/”进行切分的，注意切分的时候需要转义
        var filename = ""
        if (st.length == 2) filename = st(1)
        else filename = "null"
        filename
      }
    }, DataTypes.StringType)
  }


  /**
   * 如果是kjb文件，就找到<entries>下面的<entry>，获取其<name>和<filename>
   *
   * @param jobName  kettle中定义的jobName
   * @param fileName kettle运行产生的文件名
   * @param spark    SparkSession对象
   * @return
   */
  def analysis_kjb(jobName: String, fileName: String, spark: SparkSession): DataFrame = {
    import spark.implicits._
    val filePath = FOLDER_NAME + fileName //  文件全路径名

    //用spark读取kettle产生的kjb和ktr文件，它们都是xml文件（需要单独引入maven依赖，spark-xml_2.11包）
    val name_filename_df = spark.read.format("com.databricks.spark.xml")
      .option("rowTag", "job")
      //.schema(schema)
      .load(filePath)
      .withColumn("jobName", lit(jobName))

      .withColumn("entry", explode($"entries.entry"))
      .select("entry.name", "entry.filename")
      //      .withColumn("filename", explode($"entry.filename"))
      //      .withColumnRenamed("name", "jobName")
      //      .withColumnRenamed("filename", "ktrName")
      .na.drop() //  过滤掉含有null值的行
    //    println("当前kjb：" + filePath)
    name_filename_df
  }


  /**
   * 如果filename后缀名是.ktr，那么读取该ktr文件，然后找到<order>，遍历全部<hop>.
   * 再拿到其中的 <from>member_database_会员积分明细</from>和<to>member_database_member_integral_details</to>
   * 最后去日志文件kettle_dev_s.log，进行日志解析
   *
   * @param jobName
   * @param fileName
   * @param spark
   * @return
   */
  def analysis_ktr(jobName: String, fileName: String, spark: SparkSession): DataFrame = {
    //  def analysis_ktr(jobName: String, fileName: String): DataFrame = {
    import org.apache.spark.sql.functions.lit
    import spark.implicits._

    val filePath = FOLDER_NAME + fileName
    val result = spark.read.format("com.databricks.spark.xml")
      .option("rowTag", "transformation")
      //.schema(schema) //不指定schema的话，spark会自动推断。
      // 若报错，可参考https://blog.csdn.net/zpf336/article/details/88827081?ops_request_misc=%257B%2522request%255Fid%2522%253A%2522159615680519725219951327%2522%252C%2522scm%2522%253A%252220140713.130102334.pc%255Fblog.%2522%257D&request_id=159615680519725219951327&biz_id=0&utm_medium=distribute.pc_search_result.none-task-blog-2~blog~first_rank_v2~rank_blog_default-2-88827081.pc_v2_rank_blog_default&utm_term=spark%E8%AF%BB%E5%8F%96xml%E6%8A%A5%E9%94%99&spm=1018.2118.3001.4187
      .load(filePath) //指定xml文件路径
      /**
       * jobName对应start_execution路径
       * <entry>
       * <name>新财务对账</name>
       * <filename>${Internal.Entry.Current.Directory}/财务对账.ktr</filename>
       * </entry>
       */
      .withColumn("jobName", lit(jobName)) //添加jobName
      .withColumn("ktrName", lit(fileName))
      .filter("jobName!='kettle_job_all_succeed'")
      .filter("ktrName!='kettle_job_all_succeed.ktr'")

    println("result is......")
    result.select("jobName", "ktrName").show(false)

    val from_tp_df = result //添加ktrName（ktr文件名）


      /*解析kjb文件，文件格式如下：
      <order>
        <hop>
          <from>获取目的表最大日期</from>
          <to>日期维度</to>
          <enabled>Y</enabled>
        </hop>
        <hop>
          <from>日期维度</from>
          <to>复制记录到结果</to>
          <enabled>Y</enabled>
        </hop>
      </order>
      需要获取到order标签下面的from标签和to标签中的内容
      然后将结果和日志文件对比，得到对应的输入和输出结果*/
      .withColumn("hop", explode($"order.hop")) //解析后缀为“.ktr”，得到job的from_table和to_table
      //      .withColumn("filename", explode($"entry.filename"))
      //hop标签下的from标签，表示kettle读取的源（中间）表名。hop标签下的from标签，表示kettle写入的目的（中间）表名
      .select("jobName", "ktrName", "hop.from", "hop.to")
      .withColumnRenamed("from", "from_table") //  重命名一下from列名，避免和sql语句关键字冲突。
      .withColumnRenamed("to", "to_table") //  重命名一下to列名，避免和sql语句关键字冲突。
    from_tp_df.createTempView("from_tp_df")


    /* 通过RightJoin，使得from标签和to标签中，如果一个表同时出现在from_table和to_table中。例如：
     即A是from_table,B是to_table。然后再次出现B是from_table,C是to_table。那么就把B当成from_table,C当成to_table。
     得到最后的from_table和to_table结果集。*/
    val remove = spark.sql(
      s"""
         |SELECT b.jobName, b.ktrName, b.from_table, b.to_table
         | FROM from_tp_df a
         | RIGHT JOIN from_tp_df b
         | ON a.from_table=b.to_table
         | WHERE a.to_table IS NULL
         |""".stripMargin)


    spark.catalog.dropTempView("from_tp_df")

    //该返回值是一个去除了表输入名称不存在于表输出名称中的，且包含jobName和ktrName
    //    示例：
    //    |jobName     |ktrName   |from_table                |to_table                             |
    //    |测试数据仓库_229_1|测试环境_229_1|oauth_database_人员角色维度表    |dds_role                             |
    //    remove.show(100, false)
    remove
  }
}


case class KTRs(jobName: String, ktrName: String, from_table: String, to_table: String)

case class KJBs(name: String, fileName: String)