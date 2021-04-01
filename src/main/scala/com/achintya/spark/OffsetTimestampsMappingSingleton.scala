package com.achintya.spark

object OffsetTimestampsMappingSingleton extends Serializable {
    var offsetTimestampsMapping: Option[OffsetTimestampsMappingAccumulatorV2.type] = None
}
