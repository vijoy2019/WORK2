package com.intuit.aiml.metrics.processing.api.io

import org.apache.spark.sql.Dataset

import com.typesafe.config.Config

import com.intuit.aiml.infostore.messaging.Metric

abstract class MetricsSink extends Serializable {
  def publish(stream: Dataset[Metric]): Unit
}

object MetricsSink {
  /**
    * Method to return a new instance of sink class implementation
    *
    * @param clazz Sink class implementation string
    * @param conf  config for sink
    * @return a [[MetricsSink]] instance
    */
  implicit def instantiateSink(clazz: String, conf: Config): MetricsSink = {
    Instantiable[MetricsSink](clazz)(conf)
  }
}
