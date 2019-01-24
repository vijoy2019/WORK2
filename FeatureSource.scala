package com.intuit.aiml.metrics.processing.api.io

import org.apache.spark.sql.{Dataset, SparkSession}

import com.typesafe.config.{Config, ConfigFactory}

abstract class FeatureSource(config: Config, ss: SparkSession) extends Serializable {
  def load(): Dataset[Feature]
}

object FeatureSource {
  implicit def function2Wrapper(f: () => Dataset[Feature]): FeatureSource =
    new Wrapper(f)(ConfigFactory.empty(), SparkSession.getActiveSession.get)

  private[this] class Wrapper(f: () => Dataset[Feature])(config: Config, ss: SparkSession)
    extends FeatureSource(config, ss) {
    override def load(): Dataset[Feature] = f()
  }

}
