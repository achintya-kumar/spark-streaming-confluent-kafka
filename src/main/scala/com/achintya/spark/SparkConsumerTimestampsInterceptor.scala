package com.achintya.spark

import io.confluent.connect.replicator.offsets.ConsumerTimestampsInterceptor
import org.apache.kafka.clients.consumer.ConsumerRecords

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.util.Try

class SparkConsumerTimestampsInterceptor[K, V] extends ConsumerTimestampsInterceptor[K, V] {

    override def onConsume(records: ConsumerRecords[K, V]): ConsumerRecords[K, V] = {
        val cr = super.onConsume(records)

        val accumulator = OffsetTimestampsMappingSingleton.offsetTimestampsMapping.get
        offsetTimestamps().asScala.foreach(ot => {
            val topicPartition = ot._1
//            val offsetTimestampMap = ListMap(
//                ot._2.asScala
//                .map(a => (a._1.asInstanceOf[Long], a._2.asInstanceOf[Long]))
//                .toSeq
//                .sortBy(_._1): _*)
            Try {
                val a = ot._2.asScala
                    .map(a => (a._1.asInstanceOf[Long], a._2.asInstanceOf[Long]))
                    .toSeq
                    .sortBy(_._1)
                val b = a(a.size - 2)
                accumulator.add((topicPartition, b))
            }
        })

        cr
    }
}
