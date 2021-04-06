package com.achintya.spark

import org.apache.kafka.common.TopicPartition

import java.{lang, util}

object TypeDefinitions {
    type offsetTranslationsMap = util.HashMap[lang.Long, lang.Long]
    type offsetTranslationWithTopicPartition = (TopicPartition, offsetTranslationsMap)
    type offsetTranslationWithTopicPartitionsMap = util.HashMap[TopicPartition, offsetTranslationsMap]
}
