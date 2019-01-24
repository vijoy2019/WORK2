package com.intuit.aiml.metrics.processing

import org.scalatest.{FeatureSpec, GivenWhenThen}

import com.typesafe.config.ConfigFactory

class ProcessingContextSpec extends FeatureSpec with GivenWhenThen {

  feature("Get Processing Context") {

    scenario("Get an already created processing Context") {

      Given("An empty config")
      val config = ConfigFactory.load("test-application.conf").getConfig("spark-configs-1")
      val processingContext = ProcessingContext.builder.withConfig(config).getOrCreate

      When("Create a Processing Context which was already created")
      val processingContext2 = ProcessingContext.builder.withConfig(config).getOrCreate

      Then("Assert the context is not null")
      assert(processingContext != null)
      assert(processingContext.sparkSession != null)
      assert(processingContext.hashCode == processingContext2.hashCode)
    }
  }
}

