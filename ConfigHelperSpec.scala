package com.intuit.aiml.metrics.processing.utils.config

import org.scalatest.{FeatureSpec, GivenWhenThen}
import com.typesafe.config.ConfigFactory

class ConfigHelperSpec extends FeatureSpec with GivenWhenThen {

  feature("Get SparkConf") {

    scenario("Get an already created SparkConf") {

      Given("First test case")
      val config = ConfigFactory.load("test-application.conf").getConfig(SPARK_CONFIG)
      val sparkConfig = ConfigHelper.getSparkConf(config)
      Then("Assert the context is not null")
      assert(sparkConfig.get("spark.app.name").equals("Feature Processor Driver App"))
      assert(sparkConfig.get("spark.master").equals("local"))
      assert(sparkConfig.get("spark.conf.1").equals("1234"))
    }
  }

  feature("Testing for setAll values to SparkConf") {

    scenario("Test case is working fine") {

      Given("Second test case")
      val config = ConfigFactory.empty()
      val sparkConfig = ConfigHelper.getSparkConf(config)
      Then("set all values to setAll function")
      assert(sparkConfig.getAll.length == 0)
    }
  }
}
