package com.intuit.aiml.metrics.processing

import org.scalatest.FunSuite

class MetricsPipelineSpec extends FunSuite {

  val pipeline_name = "Metrics"
  val pipelinenameSet = "metrics_pipeline"
  val PIPELINE_DELIMITER = "_"
  val sourceConfigKey = "123"
  val sourceClassName = "KafkaSource"
  val metricsConfigKey = "456"
  val metricsCalculatorClassName = "Metrics-Calculations"
  val sinkConfigKey = "789"
  val sinkClassName = "KafkaSink"

  var metricsPipeline = MetricsPipeline.builder()
                        .withPipelineName("Metrics")
                        .withSource("123", "KafkaSource")
                        .withMetricsCalculator("456", "Metrics-Calculations")
                        // .withSink("789", "KafkaSink")
    .withSink("KafkaSink","789")

                        .build();

  test("Pipeline name is set correctly in constructor") {
    assert(metricsPipeline.pipelineId.equals(Some({pipelinenameSet + PIPELINE_DELIMITER + pipeline_name})))
  }

  test("Added Source name to metricsPipeline") {
    assert(metricsPipeline.source.equals(Some(PipelineEntity(sourceConfigKey,sourceClassName))))
  }

  test("Added metricsCalculator to metricsPipeline") {
    assert(metricsPipeline.metricsCalculator.equals(Some(PipelineEntity(metricsConfigKey,metricsCalculatorClassName))))
  }

  test("Added sink to metricsPipeline") {
      assert(metricsPipeline.sink.equals(Some(PipelineEntity(sinkClassName,sinkConfigKey))))
    }
}
