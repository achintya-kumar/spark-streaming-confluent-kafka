package com.achintya.spark

import org.apache.kafka.common.TopicPartition
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
import scala.collection.JavaConverters._

object OffsetTimestampsMappingAccumulatorV2 extends AccumulatorV2[(TopicPartition, mutable.Map[Long, Long]),
        mutable.Map[TopicPartition, mutable.Map[Long, Long]]] {

    type offsetTranslationsMap = mutable.Map[Long, Long]
    type offsetTranslationWithTopicPartition = (TopicPartition, offsetTranslationsMap)
    type offsetTranslationWithTopicPartitionsMap = mutable.Map[TopicPartition, offsetTranslationsMap]

    val offsetTimestampsMapping = mutable.Map[TopicPartition, offsetTranslationsMap]()

    override def isZero: Boolean = offsetTimestampsMapping.isEmpty

    override def copy(): AccumulatorV2[offsetTranslationWithTopicPartition,
        offsetTranslationWithTopicPartitionsMap] = {
        OffsetTimestampsMappingAccumulatorV2
    }

    override def reset(): Unit = offsetTimestampsMapping.clear()

    override def add(v: (TopicPartition, offsetTranslationsMap)): Unit = {
        val otMap = v._2.clone()
        offsetTimestampsMapping.put(v._1, otMap)
    }

    override def merge(
            other: AccumulatorV2[
                offsetTranslationWithTopicPartition,
                offsetTranslationWithTopicPartitionsMap]): Unit = {
        other.value.keySet.foreach(key => offsetTimestampsMapping.put(key, other.value(key)))
    }

    override def value: offsetTranslationWithTopicPartitionsMap = offsetTimestampsMapping
}