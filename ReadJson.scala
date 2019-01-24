package com.intuit.jsonoutput

import org.apache.spark.sql.SparkSession

object ReadJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[10]").appName("Read CSV File")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._
    val csvDF = spark.read.json("/Users/pswain1/workspace/JsonFormatOutput/TESTRESULT6")

    println("number of rows"+csvDF.filter($"BUSINESS_NAME".contains("Shopify")).count())

  }
  }
