package com.achintya.spark

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

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

        val groupId = "kumar-topic-consumer-group"
        val bootstrapServers = "localhost:9091"

        val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
        initializeTimestampsInterceptor(ssc)

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> bootstrapServers,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> groupId,
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean),
//            ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "1000",
            ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG -> "com.achintya.spark.SparkConsumerTimestampsInterceptor"
//            ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG -> "io.confluent.connect.replicator.offsets.ConsumerTimestampsInterceptor"
        )

        val streamRead = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](
                Array("kumar-topic"),
                kafkaParams)
        )

        streamRead.foreachRDD(rdd => {
            rdd.map(record => s"${record.offset()}, ${record.key()}, ${record.value()}").collect().foreach(println)
            println
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            streamRead.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        })

        ssc.start()
        println("AwaitTermination")
        sys.addShutdownHook(println(System.currentTimeMillis()))
        ssc.awaitTermination()


        ssc.stop()
    }

    def initializeTimestampsInterceptor(ssc: StreamingContext): Unit = {
        ssc.sparkContext.register(OffsetTimestampsMappingAccumulatorV2, "OffsetTimestampsMappingAccumulatorV2")
        val otma = OffsetTimestampsMappingAccumulatorV2
        ssc.sparkContext.broadcast(new OffsetTimestampsMappingSingletonExecutorInitializer(otma))
    }
}
