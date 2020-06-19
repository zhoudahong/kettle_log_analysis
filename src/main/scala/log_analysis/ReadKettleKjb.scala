package log_analysis

import java.util

import org.apache.spark.sql._
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import utils.{Constants, DBUtils, DateUtils, Logger}

object ReadKettleKjb {
  val NULL_STRING = "null"
  val logger: Logger = Logger.getLogger
  val analysis_time = DateUtils.get_now_time()
//  val log_fileName = "E:  DevelopToolsFiles  IdeaProjects  kettle_log_analysis  src  main  log  kettle_dev_s.log"

//  lazy val FOLDER_NAME = "file:///E:  DevelopToolsFiles  IdeaProjects  kettle_log_analysis  src  main  data  "
  lazy val FOLDER_NAME = "file:///E:\\MyWork\\Sisyphe_work\\sisyphe_kettle_data\\XXF_EDW_SS_1\\"
  lazy val KTR_SUFFIX = ".ktr"
  lazy val KJB_SUFFIX = ".kjb"
  lazy val START_FILENAME = "start_execution.kjb"
  lazy val spark = SparkSession.builder().master("local[20]").appName("ReadKettleKjb").config("spark.debug.maxToStringFields", "400")
    .getOrCreate()

  lazy val context = spark.sparkContext
  context.setLogLevel("WARN")


  def main(args: Array[String]): Unit = {
    get_kjb_result()
    //    Ktr_Log.analysis_range(log_fileName, spark, context)


  }

  def get_kjb_result() = {
    register_split_filename(spark)


    //解析初始文件start_execution.kjb得到dataframe
    val get_name_filename = analysis_kjb("start", START_FILENAME, spark)
    //    get_name_filename.show()
    //    get_name_filename.show(100, false)

    //注册成一张临时表
    get_name_filename.createTempView("name_filename")

    //使用临时函数切分filename
    //    val split_filename = spark.sql("select name , split_filename(filename) as filename from name_filename")
    val split_filename = spark.sql("select name, split_filename(filename) as filename from name_filename") //获取name和对应的ktr或者kjb文件
        split_filename.show(30, false)

    //出现错误：Error:(72, 23) Unable to find encoder for type stored in a Dataset.  Primitive types (Int, String, etc) and Product types (case classes) are supported by importing spark.implicits._  Support for serializing other types will be added in future releases.
    //解决如下：
    implicit val encoder = org.apache.spark.sql.Encoders.STRING //添加字符串类型编码器
    implicit val matchError = org.apache.spark.sql.Encoders.tuple(Encoders.STRING, Encoders.STRING)
    implicit val matchError2 = org.apache.spark.sql.Encoders.kryo[util.HashMap[String, String]]

    Constants.trunc_table(DBUtils.kettle_database, DBUtils.kettle_t_ktr_result)
    split_filename.foreach(x => {
      //如果filename后缀名是.ktr，那么读取该ktr文件，然后找到<order>，遍历全部<hop>，
      //拿到其中的 <from>member_database_会员积分明细</from>
      //<to>member_database_member_integral_details</to>
      //再去日志文件kettle_dev_s.log，进行日志解析
      val jobName = x.getAs[String]("name")
      val fileName = x.getAs[String]("filename")
      if (fileName.contains(".ktr")) {
        val str = fileName.replaceAll("\\[", "").replaceAll("\\]", "")
        val ktr: DataFrame = analysis_ktr(jobName, str, spark)
        ktr.show(20, false)

        //+--------+------------+-------------+----------------------+
        //|jobName |ktrName     |from_table   |to_table              |
        //+--------+------------+-------------+----------------------+
        //|DDS_fifo|dds_fifo.ktr|2.0版本_营运日志_客流|station_passenger_flow|
        //将结果写入到mysql数据库中
        DBUtils.save_Mysql_Append(ktr, DBUtils.kettle_database, DBUtils.kettle_t_ktr_result)
      }

      else if (fileName.contains(".kjb")) {
        //如果是kjb文件，就找到<entries>下面的<entry>，获取其<name>和<filename>
        val kjb = analysis_kjb(jobName, fileName.replaceAll("\\[", "").replaceAll("\\]", ""), spark)
        kjb
      }
    }
    )
    println("get_kjb_result完成")
  }

  def register_split_filename(spark: SparkSession) = {
    //注册临时函数
    //临时函数的功能是将字符串中的不需要的部分切除
    //示例如下
    //将${Internal.Entry.Current.Directory}/A_444_ODS_表日期更新.ktr，变为A_444_ODS_表日期更新.ktr
    spark.sqlContext.udf.register("split_filename", new UDF1[String, String]() {
      @throws[Exception]
      override def call(s: String): String = if (s == null) "null"
      else {
        val st = s.split("\\/")
        var filename = ""
        //是以“/”进行切分的，注意切分的时候需要转义
        if (st.length == 2) filename = st(1)
        else filename = "null"
        filename
      }
    }, DataTypes.StringType)
  }

  //如果是kjb文件，就找到<entries>下面的<entry>，获取其<name>和<filename>
  def analysis_kjb(jobName: String, fileName: String, spark: SparkSession): DataFrame = {
    import spark.implicits._
    val filePath = FOLDER_NAME + fileName

    val name_filename_df = spark.read.format("com.databricks.spark.xml")
      .option("rowTag", "job")
      //.schema(schema)
      .load(filePath)


      .withColumn("jobName", lit(jobName))
      //解析后缀为“.kjb”，得到job的name和对应的filename
      .withColumn("entry", explode($"entries.entry"))
      //      .withColumn("filename", explode($"entry.filename"))
      .select("entry.name", "entry.filename")
      //      .withColumnRenamed("name", "jobName")
      //      .withColumnRenamed("filename", "ktrName")
      .na.drop()
    //    println("当前kjb：" + filePath)
    name_filename_df
  }

  //如果filename后缀名是.ktr，那么读取该ktr文件，然后找到<order>，遍历全部<hop>，
  //拿到其中的 <from>member_database_会员积分明细</from>
  //<to>member_database_member_integral_details</to>
  //再去日志文件kettle_dev_s.log，进行日志解析
  def analysis_ktr(jobName: String, fileName: String, spark: SparkSession): DataFrame = {
    import org.apache.spark.sql.functions.lit
    import spark.implicits._

    val filePath = FOLDER_NAME + fileName

    val from_tp_df = spark.read.format("com.databricks.spark.xml")
      .option("rowTag", "transformation")
      //.schema(schema)
      .load(filePath)

      .withColumn("jobName", lit(jobName))
      .withColumn("ktrName", lit(fileName))

      //解析后缀为“.kjb”，得到job的name和对应的filename
      .withColumn("hop", explode($"order.hop"))
      //      .withColumn("filename", explode($"entry.filename"))
      .select("jobName", "ktrName", "hop.from", "hop.to")
      .withColumnRenamed("from", "from_table")
      .withColumnRenamed("to", "to_table")

    //    println("当前ktr：" + filePath)
    //    from_tp_df.show(100, false)

    //    println("===<<>>")
    //    from_tp_df.select("from_table").join(from_tp_df.select("to_table"), $"from_table" === $"to_table", "right")
    //      .show(100, false)
    from_tp_df.createTempView("from_tp_df")


    val remove = spark.sql("select b.jobName,b.ktrName,b.from_table,b.to_table from from_tp_df a right join from_tp_df b on a.from_table=b.to_table where a.to_table is null")


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