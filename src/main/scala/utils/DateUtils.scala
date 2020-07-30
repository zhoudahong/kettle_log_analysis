package utils

import java.util.Date
import java.text.SimpleDateFormat
import java.util

import org.apache.spark.sql.Row

object DateUtils {
  val logger = Logger.getLogger(DateUtils.getClass)


  val time_split = ":"


  def main(args: Array[String]): Unit = {


    println(judge_date_bigger("2020/06/12", "2020/06/15 10:58:39"))

    val list = get_date_range("2020/05/18 06:06:14", "2020/05/28 09:33:43")
    println(list)


    get_time_diff("2020/05/16 06:27:37", "2020/05/18 06:00:24")

    println(judge_date_day_equal("2012/9/13 02:03:04", "2012/9/13 00:00:00"))
    //    println(judge_date_day_equal("2012/9/13 02:03:04", "Wed Jun 03 03:32:27 CST 2020"))


    println(time_transform("2020/05/16 06:27:37"))

    val x = "*ERROR* [org.osgi.service.cm.ManagedService, id=255, bundle=52/mvn:org.apache.aries.transaction/org.apache.aries.transaction.manager/1.1.1]: Unexpected problem updating configuration org.apache.aries.transaction"
    val y = ".*org.osgi.service.cm.ManagedService.*"
    val z = ".*ERROR* [org.osgi.service.cm.ManagedService.*"
    println(x.matches(y))


  }

  // 此方法计算时间毫秒
  def fromDateStringToLong(inVal: String) = {
    lazy val sdf1: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    lazy val sdf2: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd")


    var date: Date = null
    // 定义时间类型
    try
        if (inVal != null && inVal != "null" && inVal.replaceAll(" ", "").length == 18)
          date = sdf1.parse(inVal) // 将字符型转换成日期型
        else date = null
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    if (date != null) date.getTime // 返回毫秒数
    else 0L
  }


  /**
    * 获取时间差，取第一个时间减去第二个时间
    *
    * @param time1 第一个时间，格式为yyyy/MM/dd hh:mm:ss
    * @param time2 第二个时间，格式为yyyy/MM/dd hh:mm:ss
    * @return
    */
  def get_time_diff(time1: String, time2: String): String = {
    if (time2 != null && time2 != "null") {
      // 解析第一个时间
      val startT = fromDateStringToLong(time1)
      // 解析第二个时间
      val endT = fromDateStringToLong(time2)
      // 共计秒数
      val total_ss: Long = (startT - endT) / 1000
      //得到总天数
      val dd = (total_ss / (3600 * 24)).toInt
      val days_remains = total_ss % (3600 * 24)
      //得到总小时数
      val hh = (days_remains / 3600).toInt
      val remains_hours = days_remains % 3600
      //得到分种数
      val MM = (remains_hours / 60).toInt
      //得到总秒数
      val ss = remains_hours % 60
      //打印结果第一个比第二个多32天2小时3分4秒
      //        println(dd.toString + "天" + hh.toString + "小时" + MM.toString + "分钟" + ss.toString + "秒钟")
      dd.toString + "天" + hh.toString + "小时" + MM.toString + "分钟" + ss.toString + "秒钟"
    }
    else "null"

  }

  /**
    * 获取当前时间，格式为yyyy/mm/dd hh:MM:ss
    *
    * @return 当前时间，格式为yyyy/mm/dd hh:MM:ss，示例==》2020/05/20 11:26:11
    */
  def get_now_time(): String = {
    lazy val sdf1: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    lazy val sdf2: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
    val now: Date = new Date()
    val date = sdf1.format(now)
    //    println(date)
    return date
  }


  /**
    * 判断两个时间是否为同一天
    *
    * @param date1 时间1
    * @param date2 时间2
    * @return true表示是同一天，false表示不是同一天
    */
  def judge_date_day_equal(date1: String, date2: String): String = {
    //    lazy val sdf1: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    lazy val sdf2: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
    //    println(date1 + "date1")
    //        println(date2 + "date2")
    //    Wed Jun 03 03:32:27 CST 2020


    if (date1.length == 19 || date1.length == 20) {
      val d11 = sdf2.parse(date1.substring(0, 10))

      val d22 = sdf2.parse(date2)

      val d1 = sdf2.format(d11)
      //    println(d1)

      val d2 = sdf2.format(d22)

      if (d1 == d2) "1"
      else "0"
    }
    else
      "0"

  }


  /**
    * 判断第二个时间和第一个时间大小关系
    *
    * @param date1 时间1
    * @param date2 时间2
    * @return true表示是第一个时间小于第二个时间，false表示第一个时间不小于第二个时间
    */
  def judge_date_bigger(date1: String, date2: String): Boolean = {
    lazy val sdf1: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    lazy val sdf2: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd")

    if (date1.length >= 10) {
      val d11 = sdf2.parse(date1)
      val d22 = sdf1.parse(date2)

      val d1 = sdf2.format(d11)
      val d2 = sdf2.format(d22)

      //      println("d1:"+d1+" d2:"+d2)
      d1 < d2 || d1 == d2
    }
    else
      false

  }

  /**
    * 将时间转换成指定的格式
    *
    * @param date 时间
    * @return true表示是第一个时间小于第二个时间，false表示第一个时间不小于第二个时间
    */
  def time_transform(date: String): String = {
    lazy val sdf1: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    lazy val sdf2: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
    val d11 = sdf1.parse(date)
    val d1 = sdf2.format(d11)
    d1
  }


  /**
    * 获取两个时间的范围集合
    *
    * @param max_date  数据库表中最大的时间
    * @param curr_date 当前时间
    * @return 两个时间的范围集合[LinkedList]
    */
  //  def get_date_range(max_date: String, curr_date: String) = {
  //    import java.text.SimpleDateFormat
  //    import java.util
  //    import java.util.Calendar
  //    //    val bigtime = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(max_date + " 00:00:00")
  //    val bigtime = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(max_date)
  //    val endtime = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(curr_date)
  //    //    val endtime = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(curr_date + " 00:00:00")
  //    //定义一个接受时间的集合
  //    val lDate = new util.ArrayList[Date]
  //    lDate.add(bigtime)
  //
  //    val calBegin = Calendar.getInstance
  //    // 使用给定的 Date 设置此 Calendar 的时间
  //    calBegin.setTime(bigtime)
  //
  //    val calEnd = Calendar.getInstance
  //    calEnd.setTime(endtime)
  //    // 测试此日期是否在指定日期之后
  //    while ( {
  //      endtime.after(calBegin.getTime)
  //    }) { // 根据日历的规则，为给定的日历字段添加或减去指定的时间量
  //      calBegin.add(Calendar.DAY_OF_MONTH, 1)
  //      lDate.add(calBegin.getTime)
  //    }
  //    val dates = new util.LinkedList[String]
  //    import scala.collection.JavaConversions._
  //    for (date <- lDate) {
  //      //      datas.add(new SimpleDateFormat("yyyy/MM/dd").format(date))
  //      dates.add(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(date))
  //    }
  //    //    dates.remove(0)
  //    dates
  //  }

  def get_date_range(max_date: String, curr_date: String): util.LinkedList[String] = {
    lazy val sdf1: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    lazy val sdf2: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd")

    import java.util
    import java.util.Calendar

    //    val bigtime = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(max_date + " 00:00:00")
    val bigtime = sdf2.parse(max_date)
    val endtime = sdf2.parse(curr_date)
    //    val endtime = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(curr_date + " 00:00:00")
    //定义一个接受时间的集合
    val lDate = new util.ArrayList[Date]
    lDate.add(bigtime)

    val calBegin = Calendar.getInstance
    // 使用给定的 Date 设置此 Calendar 的时间
    calBegin.setTime(bigtime)

    val calEnd = Calendar.getInstance
    calEnd.setTime(endtime)
    // 测试此日期是否在指定日期之后
    while ( {
      endtime.after(calBegin.getTime)
    }) { // 根据日历的规则，为给定的日历字段添加或减去指定的时间量
      calBegin.add(Calendar.DAY_OF_MONTH, 1)
      lDate.add(calBegin.getTime)
    }
    val dates = new util.LinkedList[String]
    import scala.collection.JavaConversions._
    for (date <- lDate) {
      //      dates.add(format2.format(date))
      dates.add(sdf1.format(date))
    }

    if (dates.length >= 1) {
      dates.remove(0)
    }
    dates
  }


  /**
    * 判断时间是否在一个范围集合里面
    *
    * @param date       时间
    * @param range_list 时间范围集合
    * @return true表示在范围里，false表示不在范围里
    */
  def judge_in_range(date: String, range_list: util.List[Row]): Boolean = {
    import scala.collection.JavaConversions._
    lazy val sdf1: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    lazy val sdf2: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd")

    val d11 = sdf2.parse(date)
    val d1 = sdf2.format(d11)


    var in_range_flag = false
    for (x <- range_list) {
      val d2 = sdf2.parse(x.toString.replaceAll("\\[", "").replaceAll("\\]", ""))
      val r = sdf2.format(d2)
      if (d1.equals(r)
      ) {
        in_range_flag = true
      }
    }
    in_range_flag

  }
}

