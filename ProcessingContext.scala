package com.intuit.aiml.metrics.processing

import java.util.concurrent.atomic.AtomicReference

import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.sql.{Dataset, SparkSession}

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}

import com.intuit.aiml.infostore.messaging.Metric
import com.intuit.aiml.infostore.processing.utils.config.ConfigHelper
import com.intuit.aiml.infostore.processing.utils.log._
import com.intuit.aiml.metrics.processing.api.io._

case class ProcessingContext private(
  @transient config: Config,
  @transient sparkSession: SparkSession,
  appLog: App) {

  import ProcessingContext._

  var processingLog: DataProcessing = _
  var sourceLog: Source = Source()
  var destLog: Destination = Destination()

  // TODO Fix this
  private def createProcessingLog(): Unit = {
    if (processingLog == null) {
      sourceLog = LogUtils.createSourceLog(config, "source-key")
      destLog = LogUtils.createDestLog(config, "dest-key")
      processingLog = DataProcessing(appLog, sourceLog, destLog)
    }
  }


  def execute[Input: TypeTag](pipeline: MetricsPipeline): Unit = {
    require(pipeline.source.isDefined, s"A source must be defined for metricsPipeline: $pipeline")
    require(pipeline.sink.isDefined, s"A sink must be defined for metricsPipeline: $pipeline")

    createProcessingLog()
    processingLog.addMessage("pipelineId", s"${pipeline.pipelineId.getOrElse("").replaceAll(" ", "-")}")
    logger.info(processingLog.quotedMessage("Running Pipeline"))

    val source: FeatureSource = ProcessorUtils.getSourceClass(
      pipeline.source.get.entity,
      config.getConfig(pipeline.source.get.entityValue),
      sparkSession
    )

    val sourceDS: Dataset[Feature] = source.load()

    val metricsSink: MetricsSink = MetricsSink.instantiateSink(
      pipeline.sink.get.entity,
      config.getConfig(pipeline.sink.get.entityValue)
    )

    val metricsCalculator: MetricsCalculator = ProcessorUtils.getMetricsCalculator(
      config.getConfig(pipeline.metricsCalculator.get.entityValue)
    )

    try {
      // calculate metrics
      val metricsDS: Dataset[Metric] = metricsCalculator(sourceDS, sparkSession)

      // send metrics to sink
      metricsSink.publish(metricsDS)

    } catch {
      case e: Throwable =>
        logger.error(processingLog.quotedError(
          s"Error running pipeline: $pipeline"
        ), e)
        System.exit(-1)
    }

  }
}

object ProcessingContext {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(classOf[ProcessingContext])
  @transient private val context = new AtomicReference[ProcessingContext]

  def builder(): Builder = new Builder

  final class Builder {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(classOf[ProcessingContext.Builder])
    private[this] var config: Config = ConfigFactory.empty()


    def getOrCreate(): ProcessingContext = synchronized {
      Option(context.get()).getOrElse(create())
    }

    // TODO Add more patterns
    def withConfig(conf: Config): Builder = {
      config = config.withFallback(conf)
      this
    }

    private[this] def create(): ProcessingContext = ProcessingContext.synchronized {
      val conf = config
        .withFallback(ConfigFactory.load())
        .resolve()

      val sparkConf: SparkConf = ConfigHelper.getSparkConf(conf)

      logger.info(s"Creating SparkSession with config: $conf")
      logger.info(s"Spark Conf used: ${sparkConf.toDebugString}")

      val sparkSession: SparkSession = SparkSession.builder()
        .config(sparkConf)
        .getOrCreate()

      val appLog: App = LogUtils.createAppLog(conf, sparkSession.sparkContext.getConf)

      val ctx: ProcessingContext = ProcessingContext(config, sparkSession, appLog)
      sparkSession.sparkContext.addSparkListener(new SparkListener {
        override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
          logger.info(appLog.quotedMessage(s"Processing Context Job Started jobId:${jobStart.jobId}"))
        }

        override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
          logger.info(appLog.quotedMessage(s"Processing Context Job Ended jobId:${jobEnd.jobId}"))
        }

        override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
          logger.info(appLog.quotedMessage(s"Processing Context Executor Added executorId:${executorAdded.executorId}"))
        }

        override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
          logger.info(
            appLog.quotedMessage(s"Processing Context Executor Removed executorId:${executorRemoved.executorId}"))
        }

        override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
          logger.info(appLog.quotedMessage(
            s"""Processing Context Task Started on
               |TaskID:${taskStart.taskInfo.taskId}
               |ExecutorID:${taskStart.taskInfo.executorId}
               |Host:${taskStart.taskInfo.host}
             """.stripMargin))
        }

        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
          logger.info(appLog.quotedMessage(
            s"""Processing Context Task Ended on
               |TaskID:${taskEnd.taskInfo.taskId}
               |ExecutorID:${taskEnd.taskInfo.executorId}
               |Host:${taskEnd.taskInfo.host}
             """.stripMargin))
        }

        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
          logger.info(appLog.quotedMessage(s"Shutting down the context"))
          context.set(null)
        }
      })

      context.set(ctx)
      ctx
    }
  }

}
