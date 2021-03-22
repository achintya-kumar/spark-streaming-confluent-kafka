package com.achintya.spark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

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

        val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "localhost:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "use_a_separate_group_id_for_each_stream",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
            //            ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG -> "io.confluent.connect.replicator.offsets.ConsumerTimestampsInterceptor"
        )

        val streamRead = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](
                Array("kumar-topic"),
                kafkaParams)
        )

        streamRead.map(record => (record.key(), record.value())).foreachRDD(rdd => {
            rdd.collect().foreach(println)
            println()
        })

        ssc.start()
        println("AwaitTermination")
        ssc.awaitTermination()


        ssc.stop()
    }
}
