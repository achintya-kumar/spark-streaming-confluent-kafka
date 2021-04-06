package com.achintya.spark

import com.achintya.spark.TypeDefinitions._
import io.confluent.connect.replicator.offsets._
import org.apache.kafka.clients.consumer.{ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

class SparkConsumerTimestampsInterceptor[K, V] extends ConsumerTimestampsInterceptor[K, V] {
    private val logger: Logger = LoggerFactory.getLogger(getClass.getName)
    private var writer: ConsumerTimestampsWriter = _
    private var groupId: String = _

    override def configure(configs: util.Map[String, _]): Unit = {
        super.configure(configs)
        writer = new ConsumerTimestampsWriter()
        groupId = configs.getOrElse("group.id", "").asInstanceOf[String]
        if (groupId.isEmpty) {
            throw new KafkaException("Missing value for group.id in ConsumerTimestampsInterceptor")
        }
        else {
            val customClientId = s"custom-${configs.get("client.id")}"
            val modifiedConfigs = mutable.Map[String, Any]() ++ configs.asScala
            modifiedConfigs.put("client.id", customClientId.asInstanceOf[Any])
            writer.configure(modifiedConfigs.asJava)
        }
    }

    override def onConsume(records: ConsumerRecords[K, V]): ConsumerRecords[K, V] = {
        val cr = super.onConsume(records)

        offsetTimestamps()
            .asScala
            .foreach { offsetTimestamp =>
                val topicPartition = offsetTimestamp._1
                val offsetTranslations = offsetTimestamp._2
                if (offsetTranslations.nonEmpty) {
                    val map = new offsetTranslationsMap(offsetTranslations)
                    getAccumulator.add((topicPartition, map))
                    offsetTranslations.clear()
                } else {
                    logger.warn(s"Empty offset-translations for topic-partition $topicPartition")
                }
            }

        cr
    }

    override def onCommit(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
        println(s"Starting onCommit method - provided offsets = $offsets")
        println("BEFORE: State of the special offset-tracking Accumulator")
        println(getAccumulator.value)
//        getAccumulator.value foreach { entry =>
//            println(s"\tPartition = ${entry._1}, mapSize = ${entry._2.size()}")
//        }

        sendOffsetTimestamps(offsets)

        cleanUpAccumulator(offsets)

        println("AFTER: State of the special offset-tracking Accumulator")
        println(getAccumulator.value)
//        getAccumulator.value foreach { entry =>
//            println(s"\tPartition = ${entry._1}, mapSize = ${entry._2.size()}")
//        }
    }

    private def sendOffsetTimestamps(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
        offsets.asScala.entrySet() foreach { entry =>
            val topicPartition = entry.getKey
            val offset = entry.getValue.offset - 1L
            val offsetTimestamp = Option(getAccumulator.value.get(topicPartition).get(offset))
            if (offsetTimestamp.isDefined) {
                val groupTopicPartition = new GroupTopicPartition(this.groupId, topicPartition)
                try {
                    val timestampAndDelta = new TimestampAndDelta(offsetTimestamp.get, 0)
                    writer.send(groupTopicPartition, timestampAndDelta)
                    println(s"SUCCESS! $entry - $timestampAndDelta")
                } catch {
                    case e: Exception => {
                        println(e)
                    }
                }
            } else {
                println(s"FAILED! No matching offset found for $entry")
                logger.warn(s"FAILED! No matching offset found for $entry")
            }
        }
    }

    private def cleanUpAccumulator(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
        offsets.asScala.entrySet().par foreach { entry =>
            val topicPartition = entry.getKey
            val offset = entry.getValue.offset - 1L
            val offsetTimestampsMap = getAccumulator.value.get(topicPartition)
            val outdatedOffsets = offsetTimestampsMap.keySet().asScala.par.filter(_ < offset).toSeq
            outdatedOffsets.foreach(outdatedOffset => offsetTimestampsMap.remove(outdatedOffset))

            getAccumulator.value.put(topicPartition, offsetTimestampsMap)
        }
    }

    private def getAccumulator: OffsetTimestampsMappingAccumulatorV2.type = {
        OffsetTimestampsMappingSingleton.offsetTimestampsMapping.get
    }
}
