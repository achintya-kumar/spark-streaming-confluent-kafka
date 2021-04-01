package com.achintya.spark

import io.confluent.connect.replicator.offsets.{GroupTopicPartition, TimestampAndDelta}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.net.InetAddress
import scala.util.Try
import java.util.Properties
import scala.collection.JavaConverters._

case class OffsetProps(topic: String, partition: Int, fromOffset: Long, untilOffset: Long)

object Driver {
    def main(args: Array[String]): Unit = {
        println("starting spark streaming")
        val sparkConf = new SparkConf()
        val spark = SparkSession
            .builder
            .config(sparkConf)
            .appName("test")
            .master("local")
            .getOrCreate()

        val groupId = "kumar-topic-consumer-group-abcdefgh"
        val bootstrapServers = "localhost:9092"

        val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
        ssc.sparkContext.register(OffsetTimestampsMappingAccumulatorV2, "OffsetTimestampsMappingAccumulatorV2")
        val otma = OffsetTimestampsMappingAccumulatorV2
        val broadcast = ssc.sparkContext.broadcast(new OffsetTimestampsMappingSingletonInitializer(otma))

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> bootstrapServers,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> groupId,
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean),
//            ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "1000",
            ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG -> "com.achintya.spark.SparkConsumerTimestampsInterceptor"
        )

        val streamRead = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](
                Array("kumar-topic"),
                kafkaParams)
        )
        val props = new Properties
        props.put("bootstrap.servers", bootstrapServers)
        props.put("client.id", InetAddress.getLocalHost.getHostName)
        props.put("group.id", groupId)
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
        val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)


        val kafkaProducerParams = Map[String, Object](
            "key.deserializer" -> "class org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "class org.apache.kafka.common.serialization.StringDeserializer",
            "compression.type" -> "lz4",
            "group.id" -> groupId,
            "acks" -> "all",
            "bootstrap.servers" -> bootstrapServers,
            "delivery.timeout.ms" -> "2147483647",
            "retry.backoff.ms" -> "500",
            "key.serializer" -> "io.confluent.connect.replicator.offsets.GroupTopicPartitionSerializer",
            "max.request.size" -> "10485760",
            "value.serializer" -> "io.confluent.connect.replicator.offsets.TimestampAndDeltaSerializer",
            "max.in.flight.requests.per.connection" -> "1",
            "client.id" -> "kumar-thinkpad",
            "linger.ms" -> "500"
        )

        val kafkaProducer = new KafkaProducer[GroupTopicPartition, TimestampAndDelta](kafkaProducerParams.asJava)

        streamRead.foreachRDD(rdd => {
//            val a = consumer.committed(Collections.singleton(new TopicPartition("kumar-topic", 0)))
//            println(s"consumer-committed = $a")
            rdd.map(record => s"${record.offset()}, ${record.key()}, ${record.value()}").collect().foreach(println)
            println
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            streamRead.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
            println(s"offsetRanges = ${offsetRanges(0)}")
//            val offsetPropsList = offsetRanges.map(or => OffsetProps(or.topic, or.partition, or.fromOffset, or.untilOffset)).toList
//            Thread.sleep(5000)
//            val a = consumer.committed(Collections.singleton(new TopicPartition("kumar-topic", 0)))
//            println(s"consumer-committed = $a")
            Try {
                val a = otma.value
                val b = a.last._2._2
                kafkaProducer.send(new ProducerRecord("__consumer_timestamps",
                    new GroupTopicPartition(groupId, "kumar-topic", 0),
                    new TimestampAndDelta(b.asInstanceOf[java.lang.Long])))
            }
            println(/*a.stream().map()*/)
        })

        ssc.start()
        println("AwaitTermination")
        sys.addShutdownHook(println(System.currentTimeMillis()))
        ssc.awaitTermination()


        ssc.stop()
    }

//    def ensureAsyncCommitSucceeded(offsetPropsList: List[OffsetProps],
//            kafkaConsumer: KafkaConsumer[String, String]): Boolean = {
//        offsetPropsList.map()
//    }
}
