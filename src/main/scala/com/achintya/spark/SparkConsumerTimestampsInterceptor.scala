package com.achintya.spark

import io.confluent.connect.replicator.offsets.ConsumerTimestampsInterceptor
import org.apache.kafka.clients.consumer.{ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class SparkConsumerTimestampsInterceptor[K, V] extends ConsumerTimestampsInterceptor[K, V] {
    val logger: Logger = LoggerFactory.getLogger(getClass.getName)

    override def onConsume(records: ConsumerRecords[K, V]): ConsumerRecords[K, V] = {
        val cr = super.onConsume(records)

        val accumulator = OffsetTimestampsMappingSingleton.offsetTimestampsMapping.get

        offsetTimestamps()
            .asScala
            .foreach { offsetTimestamp =>
                val topicPartition = offsetTimestamp._1
                val offsetTranslations = offsetTimestamp._2
                if (offsetTranslations.nonEmpty) {
                    val map = offsetTranslations
                        .map(ot => (ot._1.asInstanceOf[Long], ot._2.asInstanceOf[Long]))
                    println(s"\t\t$topicPartition = ${map.size}")
                    accumulator.add((topicPartition, map))
//                    offsetTranslations.clear()
                } else {
                    logger.warn(s"Empty offset-translations for topic-partition $topicPartition")
                }
            }

        cr
    }

    override def onCommit(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
        println(s"Starting onCommit method - provided offsets = $offsets")
        val offsetTimestampsTrackingAccumulator = OffsetTimestampsMappingSingleton.offsetTimestampsMapping.get.value
        println(s"State of the special offset-tracking Accumulator = $offsetTimestampsTrackingAccumulator")

        offsetTimestampsTrackingAccumulator foreach { entry =>
            val topicPartition = entry._1
            val topicOffsetTranslationsMap = entry._2
                .map(ot => (ot._1.asInstanceOf[java.lang.Long], ot._2.asInstanceOf[java.lang.Long]))
            this.offsetTimestamps().put(topicPartition, topicOffsetTranslationsMap)
        }

        super.onCommit(offsets)

        this.offsetTimestamps().clear()
    }
}
