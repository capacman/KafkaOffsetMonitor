package com.quantifind.plugins

import com.quantifind.kafka.OffsetGetter.OffsetInfo
import com.quantifind.kafka.offsetapp.OffsetInfoReporter
import org.apache.log4j.{DailyRollingFileAppender, Level, Logger, PatternLayout}

/**
  * Created by capacman on 15.12.2016.
  */
class Log4jReporter(args: String) extends OffsetInfoReporter {
  val loggerfile = args
    .split(",")
    .find(_.toLowerCase.contains("loggerfile"))
    .map(_.split("=")(1))
    .getOrElse("kafkainfo.log")
  val PATTERN = "[%d{dd MMM yyyy HH:mm:ss,SSS}] %m%n"
  val fa = new DailyRollingFileAppender()
  fa.setName("DailyFileLogger")
  fa.setFile(loggerfile)
  fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"))
  fa.setThreshold(Level.INFO)
  fa.setAppend(true)
  fa.activateOptions()
  fa.setDatePattern("'.'yyyy-MM-dd")

  //add appender to any Logger (here is root)
  private val logger = Logger.getLogger("kafka.log4j.logger")
  logger.addAppender(fa)

  override def report(info: IndexedSeq[OffsetInfo]): Unit = info.foreach { i =>
    logger.info(
      s"topic=${i.topic},partition=${i.partition},group=${i.group},lag=${i.lag}")
  }
}

