package com.achintya.spark

import io.confluent.connect.replicator.offsets.{GroupTopicPartition, TimestampAndDelta}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer
import scala.collection.JavaConverters._

object SparkConsumerTimestampsWriter {
    val kafkaProducerParams = Map[String, Object](
        "key.deserializer" -> "class org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer" -> "class org.apache.kafka.common.serialization.StringDeserializer",
        "compression.type" -> "lz4",
        "group.id" -> "foo1",
        "acks" -> "all",
        "bootstrap.servers" -> "localhost:9091",
        "delivery.timeout.ms" -> "2147483647",
        "retry.backoff.ms" -> "500",
        "key.serializer" -> "io.confluent.connect.replicator.offsets.GroupTopicPartitionSerializer",
        "max.request.size" -> "10485760",
        "value.serializer" -> "io.confluent.connect.replicator.offsets.TimestampAndDeltaSerializer",
        "max.in.flight.requests.per.connection" -> "1",
        "client.id" -> "kumar-thinkpad",
        "linger.ms" -> "500"
    )

//    val producer = new KafkaProducer[String, String](kafkaProducerParams)

}
