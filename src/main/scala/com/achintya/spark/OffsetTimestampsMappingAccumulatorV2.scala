package com.achintya.spark

import com.achintya.spark.TypeDefinitions._
import org.apache.kafka.common.TopicPartition
import org.apache.spark.util.AccumulatorV2

import java.util
import scala.collection.JavaConverters._

object OffsetTimestampsMappingAccumulatorV2 extends
    AccumulatorV2[offsetTranslationWithTopicPartition, offsetTranslationWithTopicPartitionsMap] {

    val offsetTimestampsMapping = new util.HashMap[TopicPartition, offsetTranslationsMap]()

    override def isZero: Boolean = offsetTimestampsMapping.isEmpty

    override def copy(): AccumulatorV2[
        offsetTranslationWithTopicPartition,
        offsetTranslationWithTopicPartitionsMap] = OffsetTimestampsMappingAccumulatorV2

    override def reset(): Unit = offsetTimestampsMapping.clear()

    override def add(v: (TopicPartition, offsetTranslationsMap)): Unit = this.synchronized {
        val oldOtMap = offsetTimestampsMapping.getOrDefault(v._1, new offsetTranslationsMap)
        val newOtMap = new offsetTranslationsMap(v._2)
        newOtMap.putAll(oldOtMap)
        offsetTimestampsMapping.put(v._1, newOtMap)
    }

    override def merge(
            other: AccumulatorV2[
                offsetTranslationWithTopicPartition,
                offsetTranslationWithTopicPartitionsMap]): Unit = {
        other.value.asScala.keySet.foreach(key => {
            val oldValue = offsetTimestampsMapping.getOrDefault(key, new offsetTranslationsMap())
            val newValue = other.value.getOrDefault(key, new offsetTranslationsMap())
            newValue.putAll(oldValue)
            offsetTimestampsMapping.put(key, newValue)
        })

    }

    override def value: offsetTranslationWithTopicPartitionsMap = offsetTimestampsMapping
}