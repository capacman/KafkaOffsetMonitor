package com.quantifind.plugins

import com.quantifind.kafka.OffsetGetter.OffsetInfo
import com.quantifind.kafka.offsetapp.OffsetInfoReporter
import org.apache.log4j._
import org.kairosdb.client.HttpClient
import org.kairosdb.client.builder.MetricBuilder

/**
  * Created by capacman on 25.11.2016.
  */
class KairosReporter(args: String) extends OffsetInfoReporter {
  val kairosHTTP =
    args.split(",").find(_.toLowerCase.contains("kairos")).map(_.split("=")(1))
  val kairosPrefix = args
    .split(",")
    .find(_.toLowerCase.contains("kairosprefix"))
    .map(_.split("=")(1))

  override def report(info: IndexedSeq[OffsetInfo]): Unit = {

    kairosHTTP.foreach { httpAddr =>
      val ts = System.currentTimeMillis()
      val client = new HttpClient(httpAddr)
      client.pushMetrics(info.foldLeft(MetricBuilder.getInstance()) {
        (builder, offset) =>
          offset.owner
            .map(
              o =>
                createInitialsTags(s"${kairosPrefix.getOrElse("")}lag",
                                   builder,
                                   offset).addTag("owner", o))
            .getOrElse(createInitialsTags("lag", builder, offset))
            .addDataPoint(ts, offset.lag)

          offset.owner
            .map(
              o =>
                createInitialsTags(s"${kairosPrefix.getOrElse("")}logSize",
                                   builder,
                                   offset).addTag("owner", o))
            .getOrElse(createInitialsTags("logSize", builder, offset))
            .addDataPoint(ts, offset.logSize)

          offset.owner
            .map(
              o =>
                createInitialsTags(s"${kairosPrefix.getOrElse("")}offset",
                                   builder,
                                   offset).addTag("owner", o))
            .getOrElse(createInitialsTags("offset", builder, offset))
            .addDataPoint(ts, offset.offset)

          builder
      })

      client.pushMetrics(
        info
          .map(i => ((i.topic, i.partition), i.logSize))
          .toMap
          .toList
          .groupBy(_._1._1)
          .mapValues(_.map(_._2).sum)
          .foldLeft(MetricBuilder.getInstance()) {
            case (builder, (topic, total)) =>
              builder
                .addMetric(s"${kairosPrefix.getOrElse("")}totaloffset")
                .addTag("topic", topic)
                .addDataPoint(ts, total)
              builder
          })
      client.shutdown()
    }

  }

  private def createInitialsTags(metricName: String,
                                 builder: MetricBuilder,
                                 offset: OffsetInfo) = {
    builder
      .addMetric(metricName)
      .addTag("topic", offset.topic)
      .addTag("partition", offset.partition.toString)
      .addTag("group", offset.group)
  }
}