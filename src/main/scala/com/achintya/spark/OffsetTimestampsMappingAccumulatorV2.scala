package com.achintya.spark

import org.apache.kafka.common.TopicPartition
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

object OffsetTimestampsMappingAccumulatorV2
    extends AccumulatorV2[(TopicPartition, (Long, Long)), mutable.Map[TopicPartition, (Long, Long)]] {

    val offsetTimestampsMapping = mutable.Map[TopicPartition, (Long, Long)]()

    override def isZero: Boolean = offsetTimestampsMapping.isEmpty

    override def copy(): AccumulatorV2[(TopicPartition, (Long, Long)), mutable.Map[TopicPartition, (Long, Long)]] = {
        OffsetTimestampsMappingAccumulatorV2
    }

    override def reset(): Unit = offsetTimestampsMapping.clear()

    override def add(v: (TopicPartition, (Long, Long))): Unit = offsetTimestampsMapping.put(v._1, v._2)

    override def merge(other: AccumulatorV2[(TopicPartition, (Long, Long)),
        mutable.Map[TopicPartition, (Long, Long)]]): Unit = {
        other.value.keySet.foreach(key => offsetTimestampsMapping.put(key, other.value.get(key).get))
    }

    override def value: mutable.Map[TopicPartition, (Long, Long)] = offsetTimestampsMapping
}