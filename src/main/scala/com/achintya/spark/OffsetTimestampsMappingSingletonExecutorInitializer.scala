package com.achintya.spark

class OffsetTimestampsMappingSingletonExecutorInitializer(accumulator:
    OffsetTimestampsMappingAccumulatorV2.type) extends Serializable {

    if (OffsetTimestampsMappingSingleton.offsetTimestampsMapping.isEmpty) {
            OffsetTimestampsMappingSingleton.offsetTimestampsMapping = Some(accumulator)
    }
}
