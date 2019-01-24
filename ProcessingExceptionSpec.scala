package com.intuit.aiml.metrics.processing

import org.scalatest.FunSuite

class ProcessingExceptionSpec extends FunSuite {
  test("ProcessingException should work") {

    val thrown: ProcessingException = intercept[ProcessingException] {
      throw new ProcessingException("Intuit", new Throwable("Intuit"))
    }
    assert(thrown.getMessage === "Intuit")
  }
}
