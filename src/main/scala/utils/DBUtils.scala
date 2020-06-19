package utils

import java.sql.{Connection, DriverManager}
import java.util
import java.util.{LinkedList, Properties}

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object DBUtils {
  val kettle_database = "sisyphe_kettle"


  val kettle_t_ktr_result = "ktr_result"
  val kettle_t_log_ana_result = "log_ana_result"
  val kettle_t_log_error_info = "log_error_info"
  val kettle_t_log_detail = "kettle_log_detail"
  //    //本地mysql连接信息
  val mysql_url_param = "?useSSL=false&useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true"
  //  var mysql_url: String = "jdbc:mysql://localhost:3306/" + kettle_database + "?useSSL=false&useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true"
  //  val mysql_url: String = "jdbc:mysql://localhost:3306/"
    val mysql_url: String = "jdbc:mysql://172.16.2.229:3306/"
//  val mysql_url: String = "jdbc:mysql://192.168.21.30:3306/"

  val mysql_driver = "com.mysql.jdbc.Driver" //mysql驱动

  //  val mysql_user = "root" //mysql用户名
  val mysql_user = "zdh" //mysql用户名

//  val mysql_password = "3imr4AIB5Zm8Bg$$" //192.168.21.30mysql密码
    val mysql_password = "123456" //172.16.2.229mysql密码

  var lowerBound = "0" //spark读取mysql表，设置的下限

  var upperBound = "12000000" //spark读取mysql表，设置的上限


  //将指标结果（追加地）写入到mysql表
  def getProperty(user: String, password: String): Properties = {
    val prop = new Properties
    prop.put("user", user)
    prop.put("password", password)
    prop
  }

  /** *
    * 写入mysql
    * 将指标结果（追加地）写入到mysql表
    *
    * @param resultDF
    */
  def save_Mysql_Overwrite(resultDF: DataFrame, result_database: String, result_table: String): Unit = {
    System.out.println(result_table + "====开始写入====")
    val prop = getProperty(mysql_user, mysql_password)
    //logger.warn("save2Mysql 中df是否为空：" + (resultDF == null));
    //    val persist = resultDF.persist(StorageLevel.MEMORY_ONLY)
    val persist = resultDF

    persist.write.mode(SaveMode.Overwrite)
      .option("isolationLevel", "NONE") //isolationLevel：事务隔离级别，DataFrame写入不需要开启事务，为NONE
      .option("batchsize", "500") // batchsize：DataFrame writer批次写入MySQL 的条数，也为提升性能参数
      .option("truncate", "true") // truncate：overwrite模式时可用，表时在覆盖原始数据时不会删除表结构而是复用
      .jdbc(mysql_url + result_database + mysql_url_param, result_table, prop)

    persist.unpersist
    System.out.println(result_table + "====写入完成====")
  }


  def save_Mysql_Append(resultDF: DataFrame, result_database: String, result_table: String): Unit = {
    System.out.println(result_table + "====开始写入====")
    val prop = getProperty(mysql_user, mysql_password)
    //logger.warn("save2Mysql 中df是否为空：" + (resultDF == null));
    //    val persist = resultDF.persist(StorageLevel.MEMORY_ONLY)
    val persist = resultDF
    persist.write
      .mode(SaveMode.Append)
      .option("isolationLevel", "NONE") //isolationLevel：事务隔离级别，DataFrame写入不需要开启事务，为NONE
      .option("batchsize", "2000000") // batchsize：DataFrame writer批次写入MySQL 的条数，也为提升性能参数
      .option("truncate", "true") // truncate：overwrite模式时可用，表时在覆盖原始数据时不会删除表结构而是复用
      .jdbc(mysql_url + result_database + mysql_url_param, result_table, prop)

    //    jdbc:mysql://localhost:3306/sisyphe_kettle?useSSL=false&useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true

    //    persist.unpersist
    System.out.println(result_table + "====写入完成====")
  }


  def save_Mysql_Append2(resultDF: DataFrame, ip: String, result_database: String, result_table: String, user: String, password: String): Unit = {
    //    System.out.println(result_table + "====开始写入====")
    val prop = getProperty(user, password)
    //logger.warn("save2Mysql 中df是否为空：" + (resultDF == null));
    //    val persist = resultDF.persist(StorageLevel.MEMORY_ONLY)
    val persist = resultDF
    persist.write.mode(SaveMode.Append)
      .option("isolationLevel", "NONE") //isolationLevel：事务隔离级别，DataFrame写入不需要开启事务，为NONE
      .option("batchsize", "10000") // batchsize：DataFrame writer批次写入MySQL 的条数，也为提升性能参数
      .option("truncate", "true") // truncate：overwrite模式时可用，表时在覆盖原始数据时不会删除表结构而是复用
      .jdbc("jdbc:mysql://" + ip + ":3306/" + result_database + mysql_url_param, result_table, prop)

    //    jdbc:mysql://localhost:3306/sisyphe_kettle?useSSL=false&useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true
    //    jdbc:mysql://localhost:3306/sisyphe_kettle?useSSL=false&useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true

    //    persist.unpersist
    System.out.println(result_table + "====写入完成====")
  }


  /**
    * 获取Mysql表数据
    *
    * @param spark
    * @param tableName       要读取的表名
    * @param numPartitions   对表分多少个区进行读取
    * @param partitionColumn 分区的字段名
    * @return myaql表数据
    */
  def getMysqlData(spark: SparkSession, tableName: String, numPartitions: String, partitionColumn: String): DataFrame = {

    spark
      .read
      .format("jdbc")
      .option("url", mysql_url + kettle_database + mysql_url_param)
      .option("driver", mysql_driver)
      .option("dbtable", tableName)
      .option("user", mysql_user)
      .option("password", mysql_password)
      .option("numPartitions", numPartitions)
      .option("partitionColumn", partitionColumn)
      .option("fetchsize", 20000) //用于读操作，每次读取多少条记录
      .option("lowerBound", lowerBound)
      .option("upperBound", upperBound)
      .load
  }


  def getMysqlData2(spark: SparkSession, ip: String, databaseName: String, tableName: String, numPartitions: String, partitionColumn: String,
                    userName: String, password: String): DataFrame = {

    spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://" + ip + ":3306/" + databaseName + mysql_url_param)
      .option("driver", mysql_driver)
      .option("dbtable", tableName)
      .option("user", userName)
      .option("password", password)
      .option("numPartitions", numPartitions)
      .option("partitionColumn", partitionColumn)
      .option("lowerBound", lowerBound)
      .option("upperBound", upperBound)
      .load
  }


}
