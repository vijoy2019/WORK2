package com.intuit.aiml.metrics.processing.api.io

import org.scalatest.{FeatureSpec, GivenWhenThen}

class FeatureTestSpec extends FeatureSpec with GivenWhenThen {

  feature("Building Feature objects") {

    scenario("Build a feature object") {
      Given("Parameters for feature with batch duration")
      val batchDuration: Long = 1000
      val name = "f1"
      val value = "Hello"
      val timestamp = 10000L
      val separator = "_"

      val batchKeyCalc = math.ceil(timestamp/batchDuration)

      val batchKeyVal = name + separator + batchKeyCalc

      When("I make a request to buildFeature")
      val response = Feature.buildFeature(batchDuration)(name,FeatureDataType.BooleanType, value,timestamp)
      Then("The message is returned successfully")
      assert(response.name.equals(name))
      assert(response.dataType.equals(FeatureDataType.BooleanType))
      assert(response.value.equals(value))
      assert(response.timestamp.equals(timestamp))
      assert(response.batchKey.equals(batchKeyVal))
    }

    scenario("Test case for getBatchTimeStamp method") {
      When("I make a request to getBatchTimeStamp method")
      val response = Feature.getBatchTimeStamp("12:45_59")
      Then("The message is returned successfully")
      assert(response.equals(59L))
    }
  }
}
