package com.intuit.aiml.metrics.processing

import org.slf4j.{Logger, LoggerFactory}


case class MetricsPipeline private(
  pipelineId: Option[String],
  source: Option[PipelineEntity],
  metricsCalculator: Option[PipelineEntity],
  sink: Option[PipelineEntity]
)


case class PipelineEntity(entity: String, entityValue: String)

object MetricsPipeline {

  def builder(): Builder = new Builder

  final class Builder {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(classOf[MetricsPipeline.Builder])

    private var _pipelineId: Option[String] = None
    private var _source: Option[PipelineEntity] = None
    private var _metricsCalculator: Option[PipelineEntity] = None
    private var _sink: Option[PipelineEntity] = None

    def withPipelineName(name: String): Builder = {
      _pipelineId = Some(METRICS_PIPELINE + PIPELINE_DELIMITER + name)
      this
    }

    def withSource(sourceClassName: String, configKey: String): Builder = {
      if (_source.isEmpty) {
        _source = Some(PipelineEntity(sourceClassName, configKey))
        logger.info(s"Added source: ${_source} to metricsPipeline")
      } else {
        logger.error(s"Tried adding source: $sourceClassName to metricsPipeline, while ${_source} is already added.")
        throw new Exception("Only one source can be added to metricsPipeline")
      }
      this
    }

    def withMetricsCalculator(metricsCalculatorClassName: String, configKey: String): Builder = {
      if (_metricsCalculator.isEmpty) {
        _metricsCalculator = Some(PipelineEntity(metricsCalculatorClassName, configKey))
        logger.info(s"Added metricsCalculator: ${_metricsCalculator} to metricsPipeline")
      } else {
        logger.error(s"Tried adding metricsCalculator: $metricsCalculatorClassName to metricsPipeline," +
          s" while ${_metricsCalculator} is already added.")
        throw new Exception("Only one Metrics Calculator can be added to metricsPipeline")
      }
      this
    }

    def withSink(sinkClassName: String, configKey: String): Builder = {
      if (_sink.isEmpty) {
        _sink = Some(PipelineEntity(sinkClassName, configKey))
        logger.info(s"Added sink: ${_sink} to metricsPipeline")
      } else {
        logger.error(s"Tried adding sink: $sinkClassName to metricsPipeline," +
          s" while ${_sink} is already added.")
        throw new Exception("Only one Sink can be added to metricsPipeline")
      }
      this
    }

    def build(): MetricsPipeline = MetricsPipeline.synchronized {
      require(_pipelineId.isDefined, "A pipeline name is to be defined")

      val metricsPipeline = MetricsPipeline(
        pipelineId = _pipelineId,
        source = _source,
        metricsCalculator = _metricsCalculator,
        sink = _sink
      )

      logger.info(s"Create metricsPipeline: $metricsPipeline")
      metricsPipeline
    }
  }

}
