package utils

/**
  * author: DahongZhou
  * Date:
  * Logger实体类
  */

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory


object Logger {
  def getLogger = new Logger

  def getLogger(c: Class[_]) = new Logger(c)

  def getLogger(className: String) = new Logger(className)

  try Log4jConfig.load() //装载log4j配置文件


}

class Logger private() {
  private var log: Log = null

  log = LogFactory.getLog(this.getClass)

  def this(c: Class[_]) {
    this()
    log = LogFactory.getLog(c)
  }

  def this(className: String) {
    this()
    log = LogFactory.getLog(className)
  }

  def trace(info: String): Unit = {
    if (log.isTraceEnabled) log.trace(info)
  }

  def debug(info: String): Unit = {
    if (log.isDebugEnabled) log.debug(info)
  }

  def info(info: String): Unit = {
    if (log.isInfoEnabled) log.info(info)
  }

  def warn(info: String): Unit = {
    if (log.isWarnEnabled) log.warn(info)
  }

  def error(info: String): Unit = {
    if (log.isErrorEnabled) log.error(info)
  }

  def fatal(info: String): Unit = {
    if (log.isFatalEnabled) log.fatal(info)
  }

  def isTraceEnabled: Boolean = log.isTraceEnabled

  def isDebugEnabled: Boolean = log.isDebugEnabled

  def isInfoEnabled: Boolean = log.isInfoEnabled

  def isWarnEnabled: Boolean = log.isWarnEnabled

  def isErrorEnabled: Boolean = log.isErrorEnabled

  def isFatalEnabled: Boolean = log.isFatalEnabled


}