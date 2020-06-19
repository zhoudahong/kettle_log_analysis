package utils

import java.sql.{Connection, DriverManager}
import java.util

object Constants {
  private val max = 10 //设置连接最大数
  private val ConnectionNum = 3 //设置 每次可以获取几个Connection
  private var conNum = 0
  //连接数
  private val pool = new util.LinkedList[Connection]() //连接池

  def getDriver(): Unit = { //加载Driver
    //加载
    //这里判断的两个空不能去掉
    //可能有人以为在调用getDriver方法时已经判断过pool.isEmpty了，
    //在进行判断是没有意义的，而且当 连接数已经大于等于max时，会死循环
    //但是要考虑到运行过成中，在spark中是多线程运行的，在调用
    //getConnection方法时，可能当时pool中是空的，但是在调用后，
    //可能其他线程的数据运行完了，会还连接，
    //那么此时再进行判断时pool就不是空了，两个调教都不成立，
    //才能跳出循环，此时的情况是，获取的连接数已经大于等最大（max）的值
    //并且 已经有人把连接换了， 就可以直接取连接了，不用再创建
    //，也不能再创建
    if (conNum < max && pool.isEmpty) { //
      Class.forName("com.mysql.jdbc.Driver")

    } else if (conNum >= max && pool.isEmpty) {
      print("当前暂无可用Connection")
      Thread.sleep(2000)
      getDriver()
    }
  }

  def getConn(): Connection = {
    if (pool.isEmpty) {
      getDriver()
      for (i <- 1 to ConnectionNum) { //创建3个连接
        val conn = DriverManager.getConnection("jdbc:mysql://172.16.2.229:3306/sisyphe_kettle?useSSL=false", "zdh", "123456")
//        val conn = DriverManager.getConnection("jdbc:mysql://192.168.21.30:3306/xxf_cw?useSSL=false", "zdh", "3imr4AIB5Zm8Bg$$")
//        val conn = DriverManager.getConnection("jdbc:mysql://192.168.21.44:3306/flow_control?useSSL=false", "zhoudahong", "zhoudahong!@#")
        pool.push(conn) //  把连接放到连接池中，push是LinkedList中的方法
        conNum += 1
      }
    }
    val conn: Connection = pool.pop() //从线程池所在LinkedList中弹出一个Connection,pop 是LinkedList的方法
    conn //返回一个Connection
  }

  def returnConn(conn: Connection): Unit = { //还连接
    pool.push(conn)
  }


  /**
    * 清空表
    *
    * @param db    清空表所在的数据库
    * @param table 需要清空的表名
    * @return
    */
  def trunc_table(db: String, table: String) =  {
      val conn = getConn()
      val ps = conn.prepareStatement("truncate table " + db + "." + table)
      ps.execute()
  }
}

