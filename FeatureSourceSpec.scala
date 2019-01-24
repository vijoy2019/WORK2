package com.intuit.aiml.metrics.processing.api.io

import org.scalatest.FunSuite
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import com.typesafe.config.{Config, ConfigFactory}


class ImplementedFeatureSource(config: Config = ConfigFactory.empty, sparkSession: SparkSession)
  extends FeatureSource(config, sparkSession) {

  override def load(): Dataset[Feature] = {
    val batchDuration: Long = 1000
    val name = "f1"
    val value = "Hello"
    val timestamp = 10000L
    val batchKey = "123abc"
    // implicit def setEncoder[Feature]: Encoder[Seq[Feature]] = Encoders.kryo[Seq[Feature]]
    import sparkSession.implicits._

    // val data = Seq.fill[Feature](150)("Intuit")
    // val data = Seq.fill[Feature](150)(element => Feature)
    implicit val feaEncoder = Encoders.kryo[FeatureDataType.FeatureDataType]
    val data: Seq[Feature] = Seq.fill[Feature](5)(Feature.buildFeature(batchDuration)
    (name, FeatureDataType.StringType, value, timestamp))

    sparkSession.createDataset(data)
  }
}

class FeatureSourceSpec extends FunSuite {

  test("Generate Dataset by implementing FeatureSource") {

    val ss = SparkSession.builder().appName("feature_source_test")
      .master("local")
      .getOrCreate()

    val featureSource = new ImplementedFeatureSource(sparkSession = ss)
    val ds: Dataset[Feature] = featureSource.load()

    assert(ds.count === 5)

    // assert(ds.collect() === Seq.fill[Feature](150)(element => Feature))
  }
}
