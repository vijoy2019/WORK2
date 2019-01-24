package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.{SparkConf, SparkContext}
//import com.typesafe.scalalogging.slf4j.LazyLogging


object SparkExample extends LazyLogging {

  private val master = "local[2]"
  private val appName = "example-spark"
  private val stopWords = Set("a", "an", "the")

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    val sc = new SparkContext(conf)

    val lines = sc.textFile("src/main/resources/data/words.txt")
    val wordsCount = WordCount.count(sc, lines, stopWords)

    val counts = wordsCount.collect().mkString("[", ", ", "]")
    logger.info(counts)
  }
}
