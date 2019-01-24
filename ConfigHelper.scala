package com.intuit.aiml.metrics.processing.utils.config

import java.util.Map.Entry

import scala.collection.JavaConverters._
import org.apache.spark.SparkConf

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import org.slf4j.{Logger, LoggerFactory}


class ConfigHelper {}

object ConfigHelper {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(classOf[ConfigHelper])

  implicit class TypesafeSparkConfPatch(sparkConfig: SparkConf) {
    def setAll(conf: Config): SparkConf = {
      val entries: Iterable[Entry[String, ConfigValue]] = conf.entrySet().asScala
      val configMap: Map[String, String] = {
        (for {
          entry: Entry[String, ConfigValue] <- entries
          key = entry.getKey
          value = entry.getValue.unwrapped().toString
        } yield (key, value)).toMap
      }
      sparkConfig.setAll(configMap)
    }
  }

  def getSparkConf(conf: Config): SparkConf = {

    val sparkConf: Config = try {
      conf.getConfig(SPARK_CONFIG)
    } catch {
      case _: Throwable =>
        logger.info("No additional spark configs provided.  Using default config")
        ConfigFactory.empty()
    }
    new SparkConf().setAll(sparkConf)
  }
}
