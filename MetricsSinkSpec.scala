package com.intuit.aiml.metrics.processing.api.io

import org.scalatest.{FeatureSpec, GivenWhenThen}
import org.apache.spark.sql.Dataset

import com.typesafe.config.ConfigFactory

import com.intuit.aiml.metrics.processing.api.io.CustomMetricsSink
import com.intuit.aiml.infostore.messaging.Metric


class MetricsSinkSpec extends FeatureSpec with GivenWhenThen {
  feature("A MetricsSink should be instantiable") {
    scenario("A CustomMetricsSink is to be instantiated") {
       Given("No Input")
        val config = ConfigFactory.load()
        // val config = ConfigFactory.load("test-application.conf").getConfig(SPARK_CONFIG)
        // val config = ConfigFactory.load("test-application.conf").getConfig("com.intuit.value")
       // val config = ConfigFactory.load().getString("com.intuit.value")


       When("Instantiate CustomMetricsSink")
      /*
      val sinkValue = MetricsSink.instantiateSink("/src/test/scala/com" +
        "/intuit/aiml/metrics/processing/api/io/MetricsSinkSpec.scala",config);
      print(sinkValue)
      */
       val className = new CustomMetricsSink

      // val className = new CustomMetricsSink() // "com.intuit.aiml.metrics.processing.api.io.CustomMetricsSink"
//      Instantiable.apply[MetricsSink]("com.intuit.aiml.metrics.processing.api.io.CustomMetricSink")
       Instantiable.apply[MetricsSink](classOf[CustomSink].getCanonicalName)
      // val sinkValue = MetricsSink.
      // instantiateSink("com.intuit.aiml.metrics.processing.api.io.CustomMetricsSink",config)

     //  print(sinkValue)


      // assert(sinkValue.equals("Intuit",config))
    }
  }
}

/*
class CustomMetricsSink extends MetricsSink {
  override def publish(stream: Dataset[Metric]): Unit = Unit
}

*/

class CustomSink extends MetricsSink {
  override def publish(stream: Dataset[Metric]): Unit = "Hello"
}
