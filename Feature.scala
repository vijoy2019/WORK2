package com.intuit.aiml.metrics.processing.api.io

import com.intuit.aiml.metrics.processing.api.io.FeatureDataType.FeatureDataType


case class Feature private(
  name: String,
  dataType: FeatureDataType,
  value: AnyRef,
  timestamp: Long,
  batchKey: String
)

object Feature {
  private val separator = "_"

  def buildFeature(batchDurationMs: Long)(
    name: String,
    dataType: FeatureDataType,
    value: AnyRef,
    timestamp: Long
  ): Feature = new Feature(
    name,
    dataType,
    value,
    timestamp,
    name + separator + math.ceil(timestamp / batchDurationMs.toFloat)
  )

  def getBatchTimeStamp(batchKey: String): Long = {
    batchKey.split("_").last.toLong
  }
}
